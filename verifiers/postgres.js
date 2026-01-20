const { Client } = require("pg");

/**
 * Verifies a PostgreSQL connection by:
 * 1. Connecting with provided credentials
 * 2. Listing schemas (permission + connectivity check)
 *
 * Throws on failure (handled by worker)
 */
async function verifyPostgres(connection, options = {}) {
  const { signal } = options;
  
  const client = new Client({
    host: connection.db_host,
    port: connection.db_port,
    user: connection.db_user_name,
    password: connection.db_user_secret,
    database: connection.db_name,
    connectionTimeoutMillis: 5000,
    statement_timeout: 5000,
  });

  if (signal) {
    signal.addEventListener("abort", () => {
      client.end().catch(() => {});
    });
  }

  try {

    if (signal?.aborted) {
      throw new Error("Verification aborted");
    }

    // Connect
    await client.connect();

    if (signal?.aborted) {
      throw new Error("Verification aborted");
    }

    // Permission + reachability check
    await client.query(`
      SELECT schema_name
      FROM information_schema.schemata
      LIMIT 1
    `);

  } catch (err) {
    // Normalize error
    throw new Error(normalizePostgresError(err));
  } finally {
    // Always close connection
    try {
      await client.end();
    } catch (_) {}
  }
}


// Convert low-level PG errors into user-friendly messages
function normalizePostgresError(err) {
  if (err.code === "28P01") {
    return "Invalid PostgreSQL credentials";
  }

  if (err.code === "3D000") {
    return "Database does not exist";
  }

  if (err.code === "ECONNREFUSED") {
    return "PostgreSQL host unreachable";
  }

  if (err.code === "ETIMEDOUT") {
    return "PostgreSQL connection timed out";
  }

  if (err.code === "42501") {
    return "Insufficient privileges for schema access";
  }

  return `PostgreSQL verification failed: ${err.message}`;
}

module.exports = { verifyPostgres };
