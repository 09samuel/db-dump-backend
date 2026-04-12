const { Client } = require("pg");
const fs = require("fs");
const path = require("path");

/**
 * Verifies a PostgreSQL connection by:
 * 1. Connecting with provided credentials
 * 2. Listing schemas (permission + connectivity check)
 *
 * Throws on failure (handled by worker)
 */
async function verifyPostgres(connection, options = {}) {
  const { signal } = options;

  const sslConfig = getSSLConfig(connection);

  const client = new Client({
    host: connection.db_host,
    port: connection.db_port,
    user: connection.db_user_name,
    password: connection.db_user_secret,
    database: connection.db_name,
    connectionTimeoutMillis: 5000,
    statement_timeout: 5000,
    ssl: sslConfig,
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

function getSSLConfig(connection) {
  if (!connection.ssl_mode || connection.ssl_mode === "disable") {
    return false;
  }

  if (connection.ssl_mode === "require") {
    return {
      rejectUnauthorized: false,
    };
  }

  if (
    connection.ssl_mode === "verify-ca" ||
    connection.ssl_mode === "verify-full"
  ) {
    try {
      return {
        ca: getCAForHost(connection.db_host),
        rejectUnauthorized: true,
      };
    } catch {
      return {
        rejectUnauthorized: false,
      };
    }
  }

  return false;
}


const certCache = {};

function getCAForHost(host) {
  let caPath;

  if (host.includes("rds.amazonaws.com")) {
    caPath = path.join(__dirname, "../certs/global-bundle.pem");
  } else if (host.includes("supabase.co")) {
    caPath = path.join(__dirname, "../certs/supabase-ca.pem");
  } else if (host.includes("azure.com")) {
    caPath = path.join(__dirname, "../certs/azure-ca.pem");
  } else {
    throw new Error(
      "SSL verification requires a known provider (RDS, Supabase, Azure)"
    );
  }

  if (!certCache[caPath]) {
    certCache[caPath] = fs.readFileSync(caPath);
  }

  return certCache[caPath];
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
