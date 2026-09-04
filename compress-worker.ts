/**
 * Compression worker — pulls a job, downloads the raw upload from R2,
 * runs CRF-based ffmpeg encoding (quality-driven, not a fixed
 * bitrate), uploads the result back to R2, and reports the final
 * size/key.
 *
 * Why CRF instead of a fixed bitrate: a fixed bitrate spends the same
 * bits/second on every video regardless of content — wasteful on a
 * simple talking-head clip, insufficient on a busy/detailed one. CRF
 * lets the encoder decide bits-per-frame based on actual visual
 * complexity, so simple content compresses efficiently and complex
 * content gets more bits automatically. File size becomes a natural
 * byproduct of content and duration, not a forced target.
 *
 * Run this as a separate process from your web server (e.g. a
 * long-running worker on Railway/Fly/a VPS, or a queue consumer).
 */

import { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { spawn } from "child_process";
import { createWriteStream, statSync, unlinkSync } from "fs";
import { pipeline } from "stream/promises";

const r2 = new S3Client({
  region: "auto",
  endpoint: process.env.R2_ENDPOINT, // https://<account_id>.r2.cloudflarestorage.com
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID!,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY!,
  },
});

const BUCKET = process.env.R2_BUCKET!;

// CRF 23 is a well-established "very good visual quality" target for
// x264 — lower = better quality/bigger file, higher = smaller/worse.
// 100MB is a safety cap only — CRF rarely needs it for normal clips,
// but protects against pathological cases (very long or very complex
// footage) producing an unreasonably large file.
const DEFAULT_CRF = 23;
const FALLBACK_CRF = 26; // used only if DEFAULT_CRF somehow overshoots the cap
const MAX_OUTPUT_BYTES = 100 * 1024 * 1024;
const AUDIO_KBPS = 96;

type Job = { videoId: string; userId: string; rawKey: string };

export async function processJob(job: Job): Promise<{ compressedKey: string; sizeBytes: number }> {
  const localRaw = `/tmp/${job.videoId}-raw.mp4`;
  const localOut = `/tmp/${job.videoId}-out.mp4`;

  await downloadFromR2(job.rawKey, localRaw);

  const rawSizeBytes = statSync(localRaw).size;

  if (rawSizeBytes <= MAX_OUTPUT_BYTES) {
    // Already small enough — don't re-encode and lose quality for no
    // reason. Just remux (stream copy, no decode/re-encode) to ensure
    // it's a clean, faststart-enabled mp4 for fast web playback. This
    // is near-instant and 100% lossless since nothing is re-encoded.
    console.log(`Input is ${(rawSizeBytes / 1024 / 1024).toFixed(2)}MB, already under cap — remuxing only, no re-encode.`);
    try {
      await remuxOnly(localRaw, localOut);
    } catch (err) {
      // Rare: some source codec/container combos can't be stream-copied
      // cleanly into mp4. Fall back to a real encode so the job still
      // succeeds rather than failing outright.
      console.log("Remux failed, falling back to full encode:", err);
      await runSinglePassEncode(localRaw, localOut, DEFAULT_CRF);
    }
  } else {
    // Genuinely needs compressing — CRF lets the encoder decide bits
    // based on actual content complexity rather than a flat target.
    console.log(`Input is ${(rawSizeBytes / 1024 / 1024).toFixed(2)}MB, over cap — compressing.`);
    await runSinglePassEncode(localRaw, localOut, DEFAULT_CRF);

    let sizeBytes = statSync(localOut).size;

    // If a genuinely long/complex video still overshoots the cap, one
    // corrective pass at a slightly higher CRF (a bit more compression,
    // not a crushing amount) rather than trying to hit an exact size.
    if (sizeBytes > MAX_OUTPUT_BYTES) {
      console.log("Output exceeded cap — increasing compression slightly.");
      await runSinglePassEncode(localRaw, localOut, FALLBACK_CRF);
    }
  }

  const sizeBytes = statSync(localOut).size;
  const compressedKey = `compressed/${job.userId}/${job.videoId}.mp4`;
  await uploadToR2(localOut, compressedKey);

  // Delete the large raw original from R2 now that we have the compressed version
  await r2.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: job.rawKey }));

  unlinkSync(localRaw);
  unlinkSync(localOut);

  return { compressedKey, sizeBytes };
}

/**
 * Stream-copies the file into a clean mp4 with faststart enabled, with
 * NO re-encoding — used when the original is already small enough
 * that compressing it further would only cost quality for no benefit.
 * Runs in a fraction of a second regardless of file size, since ffmpeg
 * isn't decoding or encoding any frames, just repackaging the container.
 */
function remuxOnly(input: string, output: string): Promise<void> {
  const args = [
    "-y", "-fflags", "+genpts", "-avoid_negative_ts", "make_zero", "-i", input,
    "-c", "copy",
    "-movflags", "+faststart",
    output,
  ];
  return runFfmpeg(args);
}

function runSinglePassEncode(input: string, output: string, crf: number): Promise<void> {
  // Scale the longer dimension to 1080, preserving aspect ratio and
  // orientation instead of forcing everything into a fixed portrait
  // canvas — landscape videos get 1080 height, portrait get 1080 width.
  const scaleFilter = "scale='if(gt(iw,ih),-2,1080)':'if(gt(iw,ih),1080,-2)'";

  const args = [
    "-y", "-fflags", "+genpts", "-avoid_negative_ts", "make_zero", "-i", input,
    "-threads", "2",
    "-vf", scaleFilter,
    "-c:v", "libx264", "-crf", `${crf}`,
    "-preset", "fast", "-profile:v", "high", "-level", "4.1",
    "-c:a", "aac", "-b:a", `${AUDIO_KBPS}k`,
    "-movflags", "+faststart",
    output,
  ];

  return runFfmpeg(args);
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
export async function getPresignedUploadUrl(userId: string, videoId: string): Promise<string> {
  const { getSignedUrl } = await import("@aws-sdk/s3-request-presigner");
  const command = new PutObjectCommand({ Bucket: BUCKET, Key: `raw/${userId}/${videoId}.mp4` });
  return getSignedUrl(r2, command, { expiresIn: 3600 });
}

/**
 * REAL QUEUE CONSUMER — listens on the "video-compression" BullMQ
 * queue and runs processJob() for every incoming job. Your API adds
 * jobs to this same queue (name must match exactly) after confirming
 * an upload to R2 is complete, e.g.:
 *
 *   import { Queue } from "bullmq";
 *   const queue = new Queue("video-compression", { connection: { url: process.env.REDIS_URL } });
 *   await queue.add("compress", { videoId, userId, rawKey });
 *
 * This worker process just needs to stay running — Railway keeps it
 * alive as a long-running service.
 *
 * If REDIS_URL isn't set yet (Redis not wired up), falls back to the
 * manual TEST_RAW_KEY test mode instead — so you can still test the
 * compression pipeline itself before adding the queue.
 */

/**
 * Notifies your app's API once a video is done compressing, so it can
 * update the DB row and flip the UI status to "encoded". Builds a
 * permanent public URL (requires R2 public access enabled — see
 * bucket Settings → Public Access in Cloudflare) rather than a
 * presigned URL, since embeds need a link that never expires.
 */
async function notifyAppOfCompletion(videoId: string, compressedKey: string) {
  const publicBase = process.env.R2_PUBLIC_URL;
  if (!publicBase || !process.env.APP_URL) {
    console.log("R2_PUBLIC_URL or APP_URL not set — skipping app notification (fine for manual testing).");
    return;
  }
  const compressedUrl = `${publicBase}/${compressedKey}`;

  await fetch(`${process.env.APP_URL}/api/video-status`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ videoId, status: "encoded", compressedUrl }),
  });
}

async function startBullMqWorker() {
  const { Worker } = await import("bullmq");
  const connection = { url: process.env.REDIS_URL! };

  const worker = new Worker(
    "video-compression",
    async (job) => {
      console.log(`Picked up job ${job.id}:`, job.data);
      const result = await processJob(job.data as Job);
      console.log(`Job ${job.id} done. Compressed key: ${result.compressedKey}, size: ${(result.sizeBytes / 1024 / 1024).toFixed(2)}MB`);
      await notifyAppOfCompletion((job.data as Job).videoId, result.compressedKey);
      return result;
    },
    { connection, concurrency: 1 } // keep at 1 until memory headroom is confirmed on larger files
  );

  worker.on("failed", (job, err) => {
    console.error(`Job ${job?.id} failed:`, err.message);
  });

  worker.on("ready", () => {
    console.log("Worker connected to Redis, listening for jobs on 'video-compression' queue...");
  });
}

/**
 * MANUAL TEST MODE — used when REDIS_URL isn't set yet. Runs one job
 * against TEST_RAW_KEY (an object already in R2) or TEST_LOCAL_FILE,
 * logs the result, and then goes idle. Does NOT call process.exit() on
 * failure, so Railway won't crash-loop it — the container just stays
 * up so you can read the error in the logs at your own pace.
 */
async function runManualTest() {
  const testRawKey = process.env.TEST_RAW_KEY;
  const testFilePath = process.env.TEST_LOCAL_FILE;

  if (!testRawKey && !testFilePath) {
    console.log("No REDIS_URL, TEST_RAW_KEY, or TEST_LOCAL_FILE set. Idling.");
    setInterval(() => console.log("Worker alive, idle."), 60_000);
    return;
  }

  const videoId = `test-${Date.now()}`;
  const userId = "test-user";
  let rawKey: string;

  try {
    if (testRawKey) {
      console.log(`Using existing R2 object: ${testRawKey}`);
      rawKey = testRawKey;
    } else {
      rawKey = `raw/${userId}/${videoId}.mp4`;
      console.log(`Uploading ${testFilePath} to R2 as ${rawKey}...`);
      await uploadToR2(testFilePath!, rawKey);
    }

    console.log("Running compression job...");
    const result = await processJob({ videoId, userId, rawKey });
    console.log(`Done. Compressed key: ${result.compressedKey}, size: ${(result.sizeBytes / 1024 / 1024).toFixed(2)}MB`);
  } catch (err) {
    console.error("Manual test failed:", err);
  }

  console.log("Manual test finished (success or failure above). Idling — no auto-restart loop.");
  setInterval(() => {}, 1 << 30); // keep process alive without spamming logs
}

if (process.env.REDIS_URL) {
  startBullMqWorker();
} else {
  runManualTest();
}
