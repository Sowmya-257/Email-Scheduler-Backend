import { redisConnection } from "../queue/queue";

export async function reserveHourlySlot(
  sender: string,
  emailId: number,
  delaySeconds: number,
  hourlyLimit: number
) {
  const now = Date.now();
  const delayMs = Math.max(0, delaySeconds) * 1000;
  const hourWindow = Math.floor(now / 3_600_000);

  const hourKey = `rate:${sender}:${hourWindow}`;
  const delayKey = `send-delay:${sender}`;
  const reservationKey = `reservation:${emailId}`;
  const nextHour = (hourWindow + 1) * 3_600_000;

  const result = await redisConnection.eval(
    `
    local existing = redis.call('GET', KEYS[3])
    if existing then
      return tonumber(existing)
    end

    local count = tonumber(redis.call('GET', KEYS[1]) or '0')
    local now = tonumber(ARGV[1])
    local max = tonumber(ARGV[2])
    local delay = tonumber(ARGV[3])
    local nextHour = tonumber(ARGV[4])
    local nextAllowed = tonumber(redis.call('GET', KEYS[2]) or '0')

    if count >= max then
      local slot = math.max(nextHour, nextAllowed)
      redis.call('SET', KEYS[2], slot + delay, 'PX', math.max((slot + delay - now) + 60000, 60000))
      redis.call('SET', KEYS[3], slot, 'EX', 86400)
      return slot
    end

    local slot = math.max(now, nextAllowed)

    redis.call('INCR', KEYS[1])
    redis.call('EXPIRE', KEYS[1], 7200)
    redis.call('SET', KEYS[2], slot + delay, 'PX', math.max((slot + delay - now) + 60000, 60000))
    redis.call('SET', KEYS[3], slot, 'EX', 86400)

    return slot
    `,
    3,
    hourKey,
    delayKey,
    reservationKey,
    now,
    Math.max(1, hourlyLimit),
    delayMs,
    nextHour
  );

  const retryAt = Number(result);

  return {
    allowed: retryAt <= Date.now(),
    retryAt,
  };
}
