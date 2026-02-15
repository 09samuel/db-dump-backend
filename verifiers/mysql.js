const mysql = require("mysql2/promise");

/**
 * Verifies a MySQL connection by:
 * 1. Connecting with provided credentials
 * 2. Listing databases (permission + connectivity check)
 *
 * Throws on failure
 */
async function verifyMySQL(connection, options = {}) {
  const { signal } = options;

  const sslConfig =
    connection.ssl_mode && connection.ssl_mode !== "disable"
      ? {}
      : false;

  const config = {
    host: connection.db_host,
    port: connection.db_port,
    user: connection.db_user_name,
    password: connection.db_user_secret,
    database: connection.db_name,
    connectTimeout: 5000,
    ssl: sslConfig,
  };

  let conn;

  try {
    if (signal?.aborted) {
      throw new Error("Verification aborted");
    }

    conn = await mysql.createConnection(config);

    if (signal?.aborted) {
      throw new Error("Verification aborted");
    }

    // Lightweight permission + connectivity check
    await conn.query("SELECT 1");

  } catch (err) {
    throw new Error(normalizeMySQLError(err));
  } finally {
    if (conn) {
      try {
        await conn.end();
      } catch (_) {}
    }
  }
}

function normalizeMySQLError(err) {
  if (!err) return "Unknown MySQL verification error";

  if (err.code === "ER_ACCESS_DENIED_ERROR") {
    return "Invalid MySQL credentials";
  }

  if (err.code === "ER_BAD_DB_ERROR") {
    return "Database does not exist";
  }

  if (err.code === "ECONNREFUSED") {
    return "MySQL host unreachable";
  }

  if (err.code === "ETIMEDOUT") {
    return "MySQL connection timed out";
  }

  if (err.code === "HANDSHAKE_NO_SSL_SUPPORT") {
    return "MySQL server does not support SSL";
  }

  if (err.message?.toLowerCase().includes("ssl")) {
    return "MySQL SSL connection failed";
  }

  return `MySQL verification failed: ${err.message}`;
}

module.exports = { verifyMySQL };