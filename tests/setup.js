// Global Mock and DB Lifecycle Setup for Jest Tests
// This avoids opening real connections to Redis (via BullMQ) but connects to the real test database.

const { pool } = require('../db/index');

// Mock BullMQ Queues
jest.mock('../queue/email.queue', () => ({
  enqueueEmailJob: jest.fn().mockResolvedValue(),
}));

jest.mock('../queue/verification.queue', () => ({
  enqueueVerificationJob: jest.fn().mockResolvedValue(),
}));

jest.mock('../queue/backup_db.queue', () => ({
  enqueueBackupDBJob: jest.fn().mockResolvedValue(),
}));

jest.mock('../queue/restore_db.queue', () => ({
  enqueueRestoreDBJob: jest.fn().mockResolvedValue(),
}));

jest.mock('../queue/retention.queue', () => ({
  enqueueRetentionJob: jest.fn().mockResolvedValue(),
}));

// List of tables to clean up between test runs to ensure test isolation
const tables = [
  'users',
  'refresh_tokens',
  'email_verification_tokens',
  'password_reset_tokens',
  'connections',
  'backups',
  'restores',
  'audit_logs',
  'user_connection_roles',
  'backup_settings',
  'backup_jobs'
];

beforeEach(async () => {
  // Truncate all tables in a single query to prevent multi-transaction deadlocks
  const existingTables = [];
  for (const table of tables) {
    try {
      const res = await pool.query(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)",
        [table]
      );
      if (res.rows[0].exists) {
        existingTables.push(`"${table}"`);
      }
    } catch (err) {
      // Ignore
    }
  }

  if (existingTables.length > 0) {
    try {
      await pool.query(`TRUNCATE TABLE ${existingTables.join(', ')} CASCADE`);
    } catch (err) {
      console.error("Single-query TRUNCATE failed:", err);
      throw err;
    }
  }
});

// Close pg pool connection after all tests complete so Jest can exit cleanly
afterAll(async () => {
  await pool.end();
});
