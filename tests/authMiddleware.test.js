const jwt = require('jsonwebtoken');
const { pool } = require('../db/index');
const { insertAuditLog } = require('../utils/auditLogger');

// Ensure test secrets are defined
process.env.ACCESS_TOKEN_SECRET = process.env.ACCESS_TOKEN_SECRET || 'test-access-secret';

// Mock auditLogger to prevent writing logs during tests
jest.mock('../utils/auditLogger', () => ({
  insertAuditLog: jest.fn().mockResolvedValue(),
  getRequestMeta: jest.fn(() => ({ ipAddress: '127.0.0.1', userAgent: 'test-agent' })),
}));

const { authenticate, checkPermission } = require('../middleware/authMiddleware');

describe('authMiddleware (Integration)', () => {
  let req, res, next;

  beforeEach(() => {
    jest.clearAllMocks();
    req = {
      cookies: {},
      headers: {},
      params: {},
      body: {},
      originalUrl: '/test-route',
      method: 'GET',
      ip: '127.0.0.1',
    };
    res = {
      status: jest.fn().mockReturnThis(),
      json: jest.fn().mockReturnThis(),
    };
    next = jest.fn();
  });

  describe('authenticate middleware', () => {
    it('should return 401 Unauthorized if no accessToken cookie is present', () => {
      authenticate(req, res, next);

      expect(res.status).toHaveBeenCalledWith(401);
      expect(res.json).toHaveBeenCalledWith({ message: 'Unauthorized' });
      expect(insertAuditLog).toHaveBeenCalledWith(expect.objectContaining({
        status: 'DENIED',
        errorMessage: 'Unauthorized',
      }));
      expect(next).not.toHaveBeenCalled();
    });

    it('should return 401 Invalid token if jwt verification fails', () => {
      req.cookies.accessToken = 'invalid-token-value';

      authenticate(req, res, next);

      expect(res.status).toHaveBeenCalledWith(401);
      expect(res.json).toHaveBeenCalledWith({ message: 'Invalid token' });
      expect(insertAuditLog).toHaveBeenCalledWith(expect.objectContaining({
        status: 'DENIED',
        errorMessage: 'Invalid token',
      }));
      expect(next).not.toHaveBeenCalled();
    });

    it('should call next and set req.user if jwt verification succeeds', () => {
      const payload = { userId: 'user-uuid-123' };
      const token = jwt.sign(payload, process.env.ACCESS_TOKEN_SECRET);
      req.cookies.accessToken = token;

      authenticate(req, res, next);

      expect(req.user).toEqual(expect.objectContaining(payload));
      expect(next).toHaveBeenCalled();
      expect(res.status).not.toHaveBeenCalled();
      expect(res.json).not.toHaveBeenCalled();
    });
  });

  describe('checkPermission middleware', () => {
    const requiredPermission = 'backup:execute';
    let userId, connectionId;

    beforeEach(async () => {
      // Create user
      const userRes = await pool.query(
        "INSERT INTO users (name, email, password_hash, is_verified) VALUES ($1, $2, $3, $4) RETURNING id",
        ['Test User', 'test-user@example.com', 'hash', true]
      );
      userId = userRes.rows[0].id;

      // Create connection
      const connRes = await pool.query(
        "INSERT INTO connections (db_type, db_host, db_name, env_tag) VALUES ($1, $2, $3, $4) RETURNING id",
        ['postgresql', 'localhost', 'test_db', 'dev']
      );
      connectionId = connRes.rows[0].id;

      req.user = { userId };
      req.params.connectionId = connectionId;
    });

    it('should return 403 No access to this connection if role query returns no rows', async () => {
      // Use connectionId that doesn't exist in roles mapping (e.g. random connection ID)
      req.params.connectionId = '00000000-0000-0000-0000-000000000000';

      const middleware = checkPermission(requiredPermission);
      await middleware(req, res, next);

      expect(res.status).toHaveBeenCalledWith(403);
      expect(res.json).toHaveBeenCalledWith({ message: 'No access to this connection' });
      expect(insertAuditLog).toHaveBeenCalledWith(expect.objectContaining({
        status: 'DENIED',
        errorMessage: 'No access to this connection',
      }));
      expect(next).not.toHaveBeenCalled();
    });

    it('should return 403 Forbidden if user role has insufficient permissions', async () => {
      // Add VIEWER role (does not have backup:execute permission)
      await pool.query(
        "INSERT INTO user_connection_roles (user_id, connection_id, role) VALUES ($1, $2, $3)",
        [userId, connectionId, 'VIEWER']
      );

      const middleware = checkPermission(requiredPermission);
      await middleware(req, res, next);

      expect(res.status).toHaveBeenCalledWith(403);
      expect(res.json).toHaveBeenCalledWith({ message: 'Forbidden' });
      expect(insertAuditLog).toHaveBeenCalledWith(expect.objectContaining({
        status: 'DENIED',
        errorMessage: 'Forbidden',
      }));
      expect(next).not.toHaveBeenCalled();
    });

    it('should call next and set req.userRole if user role has sufficient permissions', async () => {
      // Add OPERATOR role (has backup:execute permission)
      await pool.query(
        "INSERT INTO user_connection_roles (user_id, connection_id, role) VALUES ($1, $2, $3)",
        [userId, connectionId, 'OPERATOR']
      );

      const middleware = checkPermission(requiredPermission);
      await middleware(req, res, next);

      expect(req.userRole).toBe('OPERATOR');
      expect(next).toHaveBeenCalled();
      expect(res.status).not.toHaveBeenCalled();
      expect(res.json).not.toHaveBeenCalled();
    });

    it('should call next and set req.userRole if user role is OWNER (wildcard "*")', async () => {
      // Add OWNER role
      await pool.query(
        "INSERT INTO user_connection_roles (user_id, connection_id, role) VALUES ($1, $2, $3)",
        [userId, connectionId, 'OWNER']
      );

      const middleware = checkPermission('some:arbitrary:action');
      await middleware(req, res, next);

      expect(req.userRole).toBe('OWNER');
      expect(next).toHaveBeenCalled();
      expect(res.status).not.toHaveBeenCalled();
      expect(res.json).not.toHaveBeenCalled();
    });

    it('should return 500 Internal server error if database query fails', async () => {
      const dbError = new Error('Database connection failed');
      const spy = jest.spyOn(pool, 'query').mockRejectedValueOnce(dbError);

      const middleware = checkPermission(requiredPermission);
      await middleware(req, res, next);

      expect(res.status).toHaveBeenCalledWith(500);
      expect(res.json).toHaveBeenCalledWith({ message: 'Internal server error' });
      expect(insertAuditLog).toHaveBeenCalledWith(expect.objectContaining({
        status: 'FAILED',
        errorMessage: dbError.message,
      }));
      expect(next).not.toHaveBeenCalled();

      spy.mockRestore();
    });
  });
});
