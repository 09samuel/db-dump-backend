const request = require('supertest');
const { pool } = require('../db/index');
const app = require('../app');
const redisClient = require('../utils/redisClient');
const jwt = require('jsonwebtoken');

process.env.ACCESS_TOKEN_SECRET = process.env.ACCESS_TOKEN_SECRET || 'test-access-secret';

describe('Connections Caching and ETags', () => {
  let token;
  let userId;
  let connectionId;

  beforeEach(async () => {
    // 1. Create a test user
    const userRes = await pool.query(
      "INSERT INTO users (name, email, password_hash, is_verified) VALUES ($1, $2, $3, $4) RETURNING id",
      ['Cache Test User', 'cachetest@example.com', 'hash', true]
    );
    userId = userRes.rows[0].id;
    token = jwt.sign({ userId, email: 'cachetest@example.com', role: 'OWNER' }, process.env.ACCESS_TOKEN_SECRET, { expiresIn: '1h' });

    // 2. Create a test connection
    const connRes = await pool.query(
      `INSERT INTO connections (db_type, db_host, db_port, db_name, env_tag, status) 
       VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
      ['postgresql', 'localhost', 5432, 'test_db_cache', 'DEV', 'CREATED']
    );
    connectionId = connRes.rows[0].id;

    // 3. Link user role
    await pool.query(
      "INSERT INTO user_connection_roles (user_id, connection_id, role) VALUES ($1, $2, $3)",
      [userId, connectionId, 'OWNER']
    );

    // Clear the Redis mock map
    const summaryKey = `user:${userId}:connections:summary`;
    const detailsKey = `connection:${connectionId}:basic-details`;
    await redisClient.del(summaryKey);
    await redisClient.del(detailsKey);
  });

  describe('GET /connections/summary caching', () => {
    it('should query DB on cache miss, populate cache, and read from cache on hit', async () => {
      // First Request -> Cache Miss
      const res1 = await request(app)
        .get('/connections/summary')
        .set('Cookie', [`accessToken=${token}`])
        .expect(200);

      expect(res1.body).toHaveProperty('data');

      // Verify it was set in Redis
      const cached = await redisClient.get(`user:${userId}:connections:summary`);
      expect(cached).not.toBeNull();
      const parsed = JSON.parse(cached);
      expect(parsed[0].id).toBe(connectionId);

      // Mutate cache manually to verify cache hit is used
      await redisClient.set(`user:${userId}:connections:summary`, JSON.stringify([{ id: connectionId, db_name: 'cached_mock' }]));

      // Second Request -> Cache Hit
      const res2 = await request(app)
        .get('/connections/summary')
        .set('Cookie', [`accessToken=${token}`])
        .expect(200);

      expect(res2.body.data[0].db_name).toBe('cached_mock');
    });
  });

  describe('GET /connections/:connectionId/basic-details caching and HTTP ETags', () => {
    it('should support ETag (304 Not Modified) and cache details in Redis', async () => {
      // First Request -> Cache Miss, sets ETag
      const res1 = await request(app)
        .get(`/connections/${connectionId}/basic-details`)
        .set('Cookie', [`accessToken=${token}`])
        .expect(200);

      const etag = res1.headers['etag'];
      expect(etag).toBeDefined();

      // Verify key is in Redis
      const cached = await redisClient.get(`connection:${connectionId}:basic-details`);
      expect(cached).not.toBeNull();

      // Second Request with If-None-Match header -> 304 Not Modified
      await request(app)
        .get(`/connections/${connectionId}/basic-details`)
        .set('Cookie', [`accessToken=${token}`])
        .set('If-None-Match', etag)
        .expect(304);
    });

    it('should evict cache when connection is updated', async () => {
      // Populate cache
      await request(app)
        .get(`/connections/${connectionId}/basic-details`)
        .set('Cookie', [`accessToken=${token}`])
        .expect(200);

      expect(await redisClient.get(`connection:${connectionId}:basic-details`)).not.toBeNull();

      // Update connection
      await request(app)
        .patch(`/connections/${connectionId}`)
        .set('Cookie', [`accessToken=${token}`])
        .send({ dbName: 'updated_name_cache' })
        .expect(204);

      // Verify cache was evicted
      expect(await redisClient.get(`connection:${connectionId}:basic-details`)).toBeNull();
    });
  });
});
