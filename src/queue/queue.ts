import { Queue } from "bullmq";
import IORedis from "ioredis";
import dotenv from "dotenv";

dotenv.config();

// Ensure TLS is enabled if an Upstash Redis URL is provided
const rawRedisUrl = process.env.REDIS_URL?.trim();
const redisUrl =
  rawRedisUrl && rawRedisUrl.includes("upstash.io") && rawRedisUrl.startsWith("redis://")
    ? rawRedisUrl.replace("redis://", "rediss://")
    : rawRedisUrl;

export const redisConnection = redisUrl
  ? new IORedis(redisUrl, { maxRetriesPerRequest: null })
  : new IORedis({
      host: process.env.REDIS_HOST || "localhost",
      port: Number(process.env.REDIS_PORT || 6379),
      password: process.env.REDIS_PASSWORD || undefined,
      maxRetriesPerRequest: null,
    });

// BullMQ delayed queue backed by Redis
export const emailQueue = new Queue("email-queue", {
  connection: redisConnection,
});
