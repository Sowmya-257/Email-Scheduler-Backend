import { redisConnection } from "../queue/queue";

// Interface for rate limit check result
export interface RateLimitResult {
  allowed: boolean;
  retryAt: number;
}

/**
 * Checks and reserves a sending slot for a sender.
 * Enforces:
 * 1. Minimum delay between consecutive emails from the same sender (send throttling)
 * 2. Maximum emails allowed per hour for that sender (hourly quota)
 * If limits are reached, computes the next available retry timestamp without dropping jobs.
 */
export async function checkRateLimit(
  sender: string,
  delaySeconds: number,
  hourlyLimit: number
): Promise<RateLimitResult> {
  const now = Date.now();
  const delayMs = Math.max(0, delaySeconds) * 1000;

  // 1. Check minimum delay between consecutive emails for this sender
  const lastSentKey = `last-sent:${sender}`;
  const lastSent = await redisConnection.get(lastSentKey);

  if (lastSent) {
    const elapsed = now - Number(lastSent);
    if (elapsed < delayMs) {
      const waitTime = delayMs - elapsed;
      return {
        allowed: false,
        retryAt: now + waitTime,
      };
    }
  }

  // 2. Check hourly limit using Redis hourly window counter
  const hourWindow = Math.floor(now / 3_600_000);
  const hourKey = `rate:${sender}:${hourWindow}`;

  const currentCount = await redisConnection.incr(hourKey);
  if (currentCount === 1) {
    // Keep counter for 2 hours to cleanly cover window transitions
    await redisConnection.expire(hourKey, 7200);
  }

  // If hourly quota exceeded, rollback counter and reschedule for the next hour window
  if (currentCount > hourlyLimit) {
    await redisConnection.decr(hourKey);
    const nextHourMs = (hourWindow + 1) * 3_600_000;
    return {
      allowed: false,
      retryAt: nextHourMs,
    };
  }

  // Mark the current send timestamp for consecutive delay throttling
  await redisConnection.set(lastSentKey, now, "PX", Math.max(delayMs * 2, 10000));

  return {
    allowed: true,
    retryAt: now,
  };
}
