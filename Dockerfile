FROM node:20-slim
RUN apt-get update && apt-get install -y ffmpeg
WORKDIR /app
COPY . .
RUN npm install
RUN npx tsc compress-worker.ts
CMD ["node", "compress-worker.js"]
