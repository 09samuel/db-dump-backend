const path = require("path");
const fs = require("fs");
const bcrypt = require("bcrypt");

// Load the test environment variables if running in test environment
const envFile = process.env.NODE_ENV === "test" ? ".env.test" : ".env";
const envPath = path.resolve(__dirname, "..", envFile);

if (fs.existsSync(envPath)) {
  require("dotenv").config({ path: envPath });
} else {
  require("dotenv").config();
}

const { pool } = require("../db/index");
const { encrypt } = require("../utils/crypto");

async function seed() {
  console.log(`Starting database seed using environment: ${envFile}`);
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // Clean up existing tables
    const tables = [
      "refresh_tokens",
      "email_verification_tokens",
      "password_reset_tokens",
      "user_connection_roles",
      "backup_settings",
      "backup_jobs",
      "restores",
      "backups",
      "audit_logs",
      "connections",
      "users",
    ];

    console.log("Truncating tables...");
    for (const table of tables) {
      await client.query(`TRUNCATE TABLE "${table}" CASCADE`);
    }

    // 1. Seed Test User
    console.log("Seeding test user...");
    const email = "testuser@example.com";
    const name = "Test User";
    const plainPassword = "Password123!";
    const passwordHash = await bcrypt.hash(plainPassword, 10);
    const isVerified = true;

    const userResult = await client.query(
      `
      INSERT INTO users (email, name, password_hash, is_verified)
      VALUES ($1, $2, $3, $4)
      RETURNING id
      `,
      [email, name, passwordHash, isVerified]
    );
    const userId = userResult.rows[0].id;
    console.log(`Seeded user: ${email} (ID: ${userId})`);

    // 2. Seed Test Connection (PostgreSQL - Verified)
    console.log("Seeding test database connection...");
    const dbType = "postgresql";
    const dbHost = "localhost";
    const dbPort = 5432;
    const dbName = "test_db";
    const envTag = "Staging";
    const dbUserName = "postgres";
    const dbUserSecret = encrypt("secretpassword");
    const status = "VERIFIED";

    const connectionResult = await client.query(
      `
      INSERT INTO connections (db_type, db_host, db_port, db_name, env_tag, db_user_name, db_user_secret, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      RETURNING id
      `,
      [dbType, dbHost, dbPort, dbName, envTag, dbUserName, dbUserSecret, status]
    );
    const connectionId = connectionResult.rows[0].id;
    console.log(`Seeded connection: ${dbName} (ID: ${connectionId})`);

    // 3. Seed User Connection Role (OWNER)
    console.log("Seeding user connection role...");
    await client.query(
      `
      INSERT INTO user_connection_roles (user_id, connection_id, role)
      VALUES ($1, $2, $3)
      `,
      [userId, connectionId, "OWNER"]
    );

    // 4. Seed Default Backup Settings
    console.log("Seeding default backup settings...");
    await client.query(
      `
      INSERT INTO backup_settings (
        connection_id,
        storage_target,
        local_storage_path,
        retention_enabled,
        retention_mode,
        retention_value,
        default_backup_type,
        scheduling_enabled,
        cron_expression,
        timeout_minutes,
        updated_by
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
      `,
      [
        connectionId,
        "LOCAL",
        "/var/backups",
        true,
        "COUNT",
        5,
        "FULL",
        false,
        null,
        30,
        userId,
      ]
    );

    await client.query("COMMIT");
    console.log("Database seeded successfully!");
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error seeding database:", error);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

seed();
