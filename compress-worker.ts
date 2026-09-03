/**
 * Compression worker — pulls a job, downloads the raw upload from R2,
 * runs two-pass ffmpeg targeting ~6MB output, uploads the result back
 * to R2, and reports the final size/key.
 *
 * Run this as a separate process from your web server (e.g. a
 * long-running worker on Railway/Fly/a VPS, or a queue consumer).
 */

import { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { spawn } from "child_process";
import { createWriteStream, statSync, unlinkSync } from "fs";
import { pipeline } from "stream/promises";
import { getVideoDurationInSeconds } from "get-video-duration"; // or parse via ffprobe

const r2 = new S3Client({
  region: "auto",
  endpoint: process.env.R2_ENDPOINT, // https://<account_id>.r2.cloudflarestorage.com
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID!,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY!,
  },
});

const BUCKET = process.env.R2_BUCKET!;
const TARGET_BYTES = 6 * 1024 * 1024; // 6MB
const TOLERANCE_MAX_BYTES = 7.13 * 1024 * 1024; // matches existing acceptance band
const AUDIO_KBPS = 128;

type Job = { videoId: string; rawKey: string };

export async function processJob(job: Job): Promise<{ compressedKey: string; sizeBytes: number }> {
  const localRaw = `/tmp/${job.videoId}-raw.mp4`;
  const localOut = `/tmp/${job.videoId}-out.mp4`;

  await downloadFromR2(job.rawKey, localRaw);

  const durationSec = await getVideoDurationInSeconds(localRaw);
  let videoKbps = calculateTargetBitrate(durationSec);

  await runTwoPassEncode(localRaw, localOut, videoKbps);

  let sizeBytes = statSync(localOut).size;

  // One corrective re-encode if we overshot the tolerance band
  if (sizeBytes > TOLERANCE_MAX_BYTES) {
    const overshoot = sizeBytes / TARGET_BYTES;
    videoKbps = Math.floor(videoKbps / overshoot);
    await runTwoPassEncode(localRaw, localOut, videoKbps);
    sizeBytes = statSync(localOut).size;
  }

  const compressedKey = `compressed/${job.videoId}.mp4`;
  await uploadToR2(localOut, compressedKey);

  // Delete the large raw original from R2 now that we have the compressed version
  await r2.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: job.rawKey }));

  unlinkSync(localRaw);
  unlinkSync(localOut);

  return { compressedKey, sizeBytes };
}

function calculateTargetBitrate(durationSec: number): number {
  // target_kbps = (target_MB * 8192) / duration - audio_kbps
  const targetMB = TARGET_BYTES / (1024 * 1024);
  const kbps = (targetMB * 8192) / durationSec - AUDIO_KBPS;
  return Math.max(Math.floor(kbps), 150); // floor to avoid absurdly low bitrates on long clips
}

function runTwoPassEncode(input: string, output: string, videoKbps: number): Promise<void> {
  const scaleFilter =
    "scale=720:1280:force_original_aspect_ratio=decrease,pad=720:1280:(ow-iw)/2:(oh-ih)/2";

  const pass1Args = [
    "-y", "-i", input,
    "-vf", scaleFilter,
    "-c:v", "libx264", "-b:v", `${videoKbps}k`,
    "-pass", "1", "-an", "-f", "mp4", "/dev/null",
  ];

  const pass2Args = [
    "-y", "-i", input,
    "-vf", scaleFilter,
    "-c:v", "libx264", "-b:v", `${videoKbps}k`,
    "-pass", "2",
    "-preset", "slow", "-profile:v", "high", "-level", "4.1",
    "-c:a", "aac", "-b:a", `${AUDIO_KBPS}k`,
    output,
  ];

  return runFfmpeg(pass1Args).then(() => runFfmpeg(pass2Args)).then(() => undefined);
}

function runFfmpeg(args: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const proc = spawn("ffmpeg", args);
    let stderr = "";
    proc.stderr.on("data", (d) => (stderr += d.toString()));
    proc.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`ffmpeg exited ${code}: ${stderr.slice(-500)}`));
    });
  });
}

async function downloadFromR2(key: string, localPath: string): Promise<void> {
  const res = await r2.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
  await pipeline(res.Body as NodeJS.ReadableStream, createWriteStream(localPath));
}

async function uploadToR2(localPath: string, key: string): Promise<void> {
  const fs = await import("fs");
  await r2.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: fs.createReadStream(localPath),
      ContentType: "video/mp4",
    })
  );
}

/**
 * Presigned upload URL generator — call this from your API when the
 * user starts an upload, so the raw 200MB file goes straight to R2
 * without touching your server.
 */
export async function getPresignedUploadUrl(videoId: string): Promise<string> {
  const { getSignedUrl } = await import("@aws-sdk/s3-request-presigner");
  const command = new PutObjectCommand({ Bucket: BUCKET, Key: `raw/${videoId}.mp4` });
  return getSignedUrl(r2, command, { expiresIn: 3600 });
}

/**
 * MANUAL TEST MODE — no Redis/queue needed yet.
 *
 * Two ways to trigger a test:
 *   A) TEST_RAW_KEY — an object already sitting in your R2 bucket
 *      (e.g. you uploaded a 200MB file via the Cloudflare dashboard
 *      to raw/my-test.mp4). This is the way to go for large files —
 *      skips baking anything into the Docker image entirely.
 *   B) TEST_LOCAL_FILE — a small file baked into the image via COPY
 *      in the Dockerfile (fine for quick small-file smoke tests only).
 *
 * Either way it will:
 *   1. Get the file into R2 under raw/ (skipped if using TEST_RAW_KEY,
 *      since it's already there)
 *   2. Run processJob() on it (download, two-pass encode, upload)
 *   3. Delete the raw original from R2
 *   4. Log the final compressed key + size
 *
 * Once this works end-to-end, swap this block out for a real BullMQ
 * consumer that calls processJob() per incoming job instead.
 */
async function runManualTest() {
  const testRawKey = process.env.TEST_RAW_KEY;
  const testFilePath = process.env.TEST_LOCAL_FILE;

  if (!testRawKey && !testFilePath) {
    console.log("Worker deployed. Set TEST_RAW_KEY (R2 object key) or TEST_LOCAL_FILE to run a manual test.");
    setInterval(() => console.log("Worker alive, idle (no queue connected yet)."), 60_000);
    return;
  }

  const videoId = `test-${Date.now()}`;
  let rawKey: string;

  if (testRawKey) {
    console.log(`Using existing R2 object: ${testRawKey}`);
    rawKey = testRawKey;
  } else {
    rawKey = `raw/${videoId}.mp4`;
    console.log(`Uploading ${testFilePath} to R2 as ${rawKey}...`);
    await uploadToR2(testFilePath!, rawKey);
  }

  console.log("Running compression job...");
  const result = await processJob({ videoId, rawKey });

  console.log(`Done. Compressed key: ${result.compressedKey}, size: ${(result.sizeBytes / 1024 / 1024).toFixed(2)}MB`);
  console.log("Raw original deleted from R2. Check your bucket to confirm.");
}

runManualTest().catch((err) => {
  console.error("Manual test failed:", err);
  process.exit(1);
});
