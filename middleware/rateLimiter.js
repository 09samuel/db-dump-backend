const redisClient = require('../utils/redisClient');
const logger = require('../utils/logger');

const LUA_SLIDING_WINDOW_SCRIPT = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local window = tonumber(ARGV[3])

-- Remove old elements
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

-- Count remaining elements
local current_requests = redis.call('ZCARD', key)

if current_requests < limit then
    -- Add the current request timestamp
    redis.call('ZADD', key, now, now)
    redis.call('EXPIRE', key, math.ceil(window / 1000))
    return {1, limit - current_requests - 1} -- Allowed, remaining count
else
    return {0, 0} -- Denied
end
`;

function createRateLimiter({ windowMs, max, keyPrefix }) {
  return async (req, res, next) => {
    // 1. Identify the key: user ID if authenticated, fallback to IP
    const isAuth = req.user && req.user.userId;
    const identifier = isAuth ? `user:${req.user.userId}` : `ip:${req.ip}`;
    const key = `rate_limit:${keyPrefix}:${identifier}`;

    const limit = max;
    const now = Date.now();
    const window = windowMs;

    try {
      // Evaluate the sliding window Lua script
      const result = await redisClient.eval(
        LUA_SLIDING_WINDOW_SCRIPT,
        1,
        key,
        limit,
        now,
        window
      );

      const [allowed, remaining] = result;

      // Set standard headers
      res.setHeader('X-RateLimit-Limit', limit);
      res.setHeader('X-RateLimit-Remaining', remaining);
      res.setHeader('X-RateLimit-Reset', Math.ceil((now + windowMs) / 1000));

      if (allowed === 1) {
        next();
      } else {
        // Return 429 and Retry-After header
        res.setHeader('Retry-After', Math.ceil(windowMs / 1000));
        return res.status(429).json({
          message: 'Too many requests, please try again later.'
        });
      }
    } catch (err) {
      // Fail-open strategy for rate limiting resilience
      logger.error(`Rate limit evaluation error for key ${key}:`, err);
      next();
    }
  };
}

module.exports = { createRateLimiter };
