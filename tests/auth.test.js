const request = require('supertest');
const crypto = require('crypto');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');

// Ensure test JWT secrets are defined
process.env.ACCESS_TOKEN_SECRET = process.env.ACCESS_TOKEN_SECRET || 'test-access-secret';
process.env.REFRESH_TOKEN_SECRET = process.env.REFRESH_TOKEN_SECRET || 'test-refresh-secret';

const { pool } = require('../db/index');
const { enqueueEmailJob } = require('../queue/email.queue');
const app = require('../app');

describe('Auth Endpoints (Integration)', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('POST /auth/register', () => {
    it('should return 400 validation error if input fields are missing or invalid', async () => {
      const res = await request(app)
        .post('/auth/register')
        .send({
          name: '',
          email: 'invalid-email',
          password: '123',
          confirmPassword: '456',
        })
        .expect(400);

      expect(res.body.success).toBe(false);
      expect(res.body.errors).toHaveProperty('name');
      expect(res.body.errors).toHaveProperty('email');
      expect(res.body.errors).toHaveProperty('password');
      expect(res.body.errors).toHaveProperty('confirmPassword');
    });

    it('should return 400 if passwords do not match', async () => {
      const res = await request(app)
        .post('/auth/register')
        .send({
          name: 'John Doe',
          email: 'john@example.com',
          password: 'Password123!',
          confirmPassword: 'DifferentPassword123!',
        })
        .expect(400);

      expect(res.body.success).toBe(false);
      expect(res.body.errors).toHaveProperty('confirmPassword', 'Passwords do not match');
    });

    it('should return 400 if user already exists', async () => {
      // Pre-insert user into database
      await pool.query(
        'INSERT INTO users (name, email, password_hash, is_verified) VALUES ($1, $2, $3, $4)',
        ['John Doe', 'john@example.com', 'some_dummy_hash', false]
      );

      const res = await request(app)
        .post('/auth/register')
        .send({
          name: 'John Doe',
          email: 'john@example.com',
          password: 'Password123!',
          confirmPassword: 'Password123!',
        })
        .expect(400);

      expect(res.body.success).toBe(false);
      expect(res.body.message).toBe('User already exists');
    });

    it('should register user and queue verification email successfully', async () => {
      const res = await request(app)
        .post('/auth/register')
        .send({
          name: 'John Doe',
          email: 'john@example.com',
          password: 'Password123!',
          confirmPassword: 'Password123!',
        })
        .expect(201);

      expect(res.body.success).toBe(true);
      expect(res.body.message).toBe('User registered. Please verify your email');

      // Verify user exists in the database
      const userResult = await pool.query('SELECT * FROM users WHERE email = $1', ['john@example.com']);
      expect(userResult.rows.length).toBe(1);
      const user = userResult.rows[0];
      expect(user.name).toBe('John Doe');
      expect(user.is_verified).toBe(false);

      // Verify verification token exists
      const tokenResult = await pool.query('SELECT * FROM email_verification_tokens WHERE user_id = $1', [user.id]);
      expect(tokenResult.rows.length).toBe(1);

      // Verify email queue job was triggered
      expect(enqueueEmailJob).toHaveBeenCalledWith(
        expect.objectContaining({
          type: 'VERIFY_EMAIL',
          email: 'john@example.com',
          token: expect.any(String),
        })
      );
    });
  });

  describe('POST /auth/login', () => {
    it('should return 400 if email or password is missing', async () => {
      const res = await request(app)
        .post('/auth/login')
        .send({ email: '' })
        .expect(400);

      expect(res.body.success).toBe(false);
      expect(res.body.message).toBe('Email and password are required');
    });

    it('should return 400 if user does not exist', async () => {
      const res = await request(app)
        .post('/auth/login')
        .send({ email: 'nonexistent@example.com', password: 'Password123!' })
        .expect(400);

      expect(res.body.success).toBe(false);
      expect(res.body.message).toBe('Invalid email or password');
    });

    it('should return 400 if user is not verified', async () => {
      const correctHash = await bcrypt.hash('Password123!', 10);
      await pool.query(
        'INSERT INTO users (name, email, password_hash, is_verified) VALUES ($1, $2, $3, $4)',
        ['Unverified User', 'unverified@example.com', correctHash, false]
      );

      const res = await request(app)
        .post('/auth/login')
        .send({ email: 'unverified@example.com', password: 'Password123!' })
        .expect(400);

      expect(res.body.success).toBe(false);
      expect(res.body.message).toBe('Please verify your email before logging in');
    });

    it('should return 400 if password does not match', async () => {
      const correctHash = await bcrypt.hash('Password123!', 10);
      await pool.query(
        'INSERT INTO users (name, email, password_hash, is_verified) VALUES ($1, $2, $3, $4)',
        ['Verified User', 'verified@example.com', correctHash, true]
      );

      const res = await request(app)
        .post('/auth/login')
        .send({ email: 'verified@example.com', password: 'WrongPassword!' })
        .expect(400);

      expect(res.body.success).toBe(false);
      expect(res.body.message).toBe('Invalid email or password');
    });

    it('should log in successfully and set tokens in cookies', async () => {
      const correctHash = await bcrypt.hash('Password123!', 10);
      await pool.query(
        'INSERT INTO users (name, email, password_hash, is_verified) VALUES ($1, $2, $3, $4)',
        ['Verified User', 'verified@example.com', correctHash, true]
      );

      const res = await request(app)
        .post('/auth/login')
        .send({ email: 'verified@example.com', password: 'Password123!' })
        .expect(200);

      expect(res.body.success).toBe(true);
      expect(res.body.message).toBe('Login successful');

      // Verify cookies are set
      const cookies = res.headers['set-cookie'].join(';');
      expect(cookies).toContain('accessToken');
      expect(cookies).toContain('refreshToken');
    });
  });

  describe('POST /auth/refresh-token', () => {
    it('should return 403 if refresh token is missing', async () => {
      const res = await request(app)
        .post('/auth/refresh-token')
        .expect(403);

      expect(res.body.message).toBe('Invalid token');
    });

    it('should detect token reuse, revoke all refresh tokens and clear cookies', async () => {
      // Insert user
      const userRes = await pool.query(
        'INSERT INTO users (name, email, password_hash, is_verified) VALUES ($1, $2, $3, $4) RETURNING id',
        ['Token User', 'token@example.com', 'hash', true]
      );
      const userId = userRes.rows[0].id;

      // Insert revoked token
      const refreshToken = 'revoked-token-123';
      const hashedRefreshToken = crypto.createHash("sha256").update(refreshToken).digest("hex");
      await pool.query(
        'INSERT INTO refresh_tokens (user_id, token_hash, expires_at, revoked) VALUES ($1, $2, $3, $4)',
        [userId, hashedRefreshToken, new Date(Date.now() + 100000), true]
      );

      // Insert a secondary active token to verify reuse deletes ALL user tokens
      await pool.query(
        'INSERT INTO refresh_tokens (user_id, token_hash, expires_at, revoked) VALUES ($1, $2, $3, $4)',
        [userId, 'some-other-active-token-hash', new Date(Date.now() + 100000), false]
      );

      const res = await request(app)
        .post('/auth/refresh-token')
        .set('Cookie', [`refreshToken=${refreshToken}`])
        .expect(403);

      expect(res.body.message).toBe('Token reuse detected. Logged out everywhere.');

      // Check DB: all refresh tokens for this user should be deleted
      const dbTokens = await pool.query('SELECT * FROM refresh_tokens WHERE user_id = $1', [userId]);
      expect(dbTokens.rows.length).toBe(0);

      const cookies = res.headers['set-cookie'].join(';');
      expect(cookies).toContain('accessToken=;');
      expect(cookies).toContain('refreshToken=;');
    });

    it('should return 403 if refresh token is expired', async () => {
      const userRes = await pool.query(
        'INSERT INTO users (name, email, password_hash, is_verified) VALUES ($1, $2, $3, $4) RETURNING id',
        ['Token User', 'token@example.com', 'hash', true]
      );
      const userId = userRes.rows[0].id;

      const refreshToken = 'expired-token-123';
      const hashedRefreshToken = crypto.createHash("sha256").update(refreshToken).digest("hex");
      await pool.query(
        'INSERT INTO refresh_tokens (user_id, token_hash, expires_at, revoked) VALUES ($1, $2, $3, $4)',
        [userId, hashedRefreshToken, new Date(Date.now() - 100000), false] // expired
      );

      const res = await request(app)
        .post('/auth/refresh-token')
        .set('Cookie', [`refreshToken=${refreshToken}`])
        .expect(403);

      expect(res.body.message).toBe('Token expired');
    });

    it('should refresh tokens successfully', async () => {
      const userRes = await pool.query(
        'INSERT INTO users (name, email, password_hash, is_verified) VALUES ($1, $2, $3, $4) RETURNING id',
        ['Token User', 'token@example.com', 'hash', true]
      );
      const userId = userRes.rows[0].id;

      const refreshToken = 'valid-token-123';
      const hashedRefreshToken = crypto.createHash("sha256").update(refreshToken).digest("hex");
      await pool.query(
        'INSERT INTO refresh_tokens (user_id, token_hash, expires_at, revoked) VALUES ($1, $2, $3, $4)',
        [userId, hashedRefreshToken, new Date(Date.now() + 100000), false]
      );

      const res = await request(app)
        .post('/auth/refresh-token')
        .set('Cookie', [`refreshToken=${refreshToken}`])
        .expect(200);

      expect(res.body.success).toBe(true);

      const cookies = res.headers['set-cookie'].join(';');
      expect(cookies).toContain('accessToken');
      expect(cookies).toContain('refreshToken');

      // Verify the old token is marked revoked
      const oldToken = await pool.query('SELECT * FROM refresh_tokens WHERE token_hash = $1', [hashedRefreshToken]);
      expect(oldToken.rows[0].revoked).toBe(true);
    });
  });

  describe('POST /auth/logout', () => {
    it('should return 400 if no refresh token cookie is present', async () => {
      const userRes = await pool.query(
        "INSERT INTO users (name, email, password_hash, is_verified) VALUES ($1, $2, $3, $4) RETURNING id",
        ['Logout User', 'logout-no-token@example.com', 'hash', true]
      );
      const userId = userRes.rows[0].id;
      const accessToken = jwt.sign({ userId }, process.env.ACCESS_TOKEN_SECRET);

      const res = await request(app)
        .post('/auth/logout')
        .set('Cookie', [`accessToken=${accessToken}`])
        .expect(400);

      expect(res.body.message).toBe('No token');
    });

    it('should successfully log out, revoke token in DB, and clear cookies', async () => {
      const userRes = await pool.query(
        'INSERT INTO users (name, email, password_hash, is_verified) VALUES ($1, $2, $3, $4) RETURNING id',
        ['Logout User', 'logout@example.com', 'hash', true]
      );
      const userId = userRes.rows[0].id;

      const refreshToken = 'logout-token-123';
      const hashedRefreshToken = crypto.createHash("sha256").update(refreshToken).digest("hex");
      await pool.query(
        'INSERT INTO refresh_tokens (user_id, token_hash, expires_at, revoked) VALUES ($1, $2, $3, $4)',
        [userId, hashedRefreshToken, new Date(Date.now() + 100000), false]
      );

      const accessToken = jwt.sign({ userId }, process.env.ACCESS_TOKEN_SECRET);

      const res = await request(app)
        .post('/auth/logout')
        .set('Cookie', [
          `accessToken=${accessToken}`,
          `refreshToken=${refreshToken}`,
        ])
        .expect(200);

      expect(res.body.message).toBe('Logged out successfully');

      const cookies = res.headers['set-cookie'].join(';');
      expect(cookies).toContain('accessToken=;');
      expect(cookies).toContain('refreshToken=;');

      // Verify token in DB is revoked
      const revokedToken = await pool.query('SELECT * FROM refresh_tokens WHERE token_hash = $1', [hashedRefreshToken]);
      expect(revokedToken.rows[0].revoked).toBe(true);
    });
  });
});
