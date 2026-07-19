const express = require('express');
const request = require('supertest');
const cookieParser = require('cookie-parser');
const { createRateLimiter } = require('../middleware/rateLimiter');
const redisClient = require('../utils/redisClient');

describe('Rate Limiter Middleware (Isolated Tests)', () => {
  let app;

  beforeEach(async () => {
    // Clear all rate limit keys in mock store
    const keys = await redisClient.keys('rate_limit:*');
    for (const key of keys) {
      await redisClient.del(key);
    }
  });

  describe('IP-Based Rate Limiting', () => {
    beforeAll(() => {
      app = express();
      app.use(cookieParser());
      app.use(express.json());

      const testLimiter = createRateLimiter({
        windowMs: 60 * 1000,
        max: 3,
        keyPrefix: 'test-ip'
      });

      app.get('/test-ip-route', testLimiter, (req, res) => {
        res.status(200).json({ success: true });
      });
    });

    it('should limit request and set headers when limit is exceeded', async () => {
      // First 3 requests -> success (200)
      for (let i = 0; i < 3; i++) {
        const res = await request(app)
          .get('/test-ip-route')
          .expect(200);

        expect(res.headers['x-ratelimit-limit']).toBe('3');
        expect(Number(res.headers['x-ratelimit-remaining'])).toBe(2 - i);
      }

      // 4th request -> blocked (429)
      const resBlocked = await request(app)
        .get('/test-ip-route')
        .expect(429);

      expect(resBlocked.body.message).toContain('Too many requests');
      expect(resBlocked.headers['x-ratelimit-remaining']).toBe('0');
      expect(resBlocked.headers['retry-after']).toBe('60');
    });
  });

  describe('User-Based Rate Limiting', () => {
    beforeAll(() => {
      app = express();
      app.use(cookieParser());
      app.use(express.json());

      // Mock auth middleware to assign req.user
      const mockAuth = (req, res, next) => {
        req.user = { userId: 'user-rate-test-123' };
        next();
      };

      const testLimiter = createRateLimiter({
        windowMs: 10 * 1000,
        max: 2,
        keyPrefix: 'test-user'
      });

      app.get('/test-user-route', mockAuth, testLimiter, (req, res) => {
        res.status(200).json({ success: true });
      });
    });

    it('should limit request based on user ID and set headers', async () => {
      // First 2 requests -> success (200)
      for (let i = 0; i < 2; i++) {
        const res = await request(app)
          .get('/test-user-route')
          .expect(200);

        expect(res.headers['x-ratelimit-limit']).toBe('2');
        expect(Number(res.headers['x-ratelimit-remaining'])).toBe(1 - i);
      }

      // 3rd request -> blocked (429)
      const resBlocked = await request(app)
        .get('/test-user-route')
        .expect(429);

      expect(resBlocked.body.message).toContain('Too many requests');
      expect(resBlocked.headers['x-ratelimit-remaining']).toBe('0');
      expect(resBlocked.headers['retry-after']).toBe('10');
    });
  });

  describe('Fail-Open Resiliency', () => {
    beforeAll(() => {
      app = express();
      app.use(cookieParser());
      app.use(express.json());

      const testLimiter = createRateLimiter({
        windowMs: 60 * 1000,
        max: 1,
        keyPrefix: 'test-fail'
      });

      app.get('/test-fail-route', testLimiter, (req, res) => {
        res.status(200).json({ success: true });
      });
    });

    it('should bypass rate limiting and allow request if Redis fails', async () => {
      // Mock redisClient.eval to throw an error
      const originalEval = redisClient.eval;
      redisClient.eval = jest.fn().mockRejectedValue(new Error('Redis connection lost'));

      // 1st request -> succeeds (200)
      await request(app)
        .get('/test-fail-route')
        .expect(200);

      // 2nd request -> would normally fail, but succeeds because rate limiter fails open
      await request(app)
        .get('/test-fail-route')
        .expect(200);

      // Restore mock
      redisClient.eval = originalEval;
    });
  });
});
