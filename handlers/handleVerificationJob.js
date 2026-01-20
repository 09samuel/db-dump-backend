const { pool } = require("../db/index");
const { decrypt } = require("../utils/crypto");

const { verifyConnectionCredentials } = require("../verifiers/verifyConnectionCredentials");
const { verifyPostgres } = require("../verifiers/postgres");
// const { verifyMySQL } = require("./verifiers/mysql");
// const { verifyMongo } = require("./verifiers/mongo");

async function handleVerificationJob(job) {
  try {

    const { connectionId } = job.data;
    const jobId = String(job.id);

    // Re-fetch from DB
    const { rows } = await pool.query(
      `SELECT * FROM connections WHERE id = $1`,
      [connectionId]
    );

    if (!rows.length) {
      console.warn("Verification job skipped: connection not found", connectionId);
      return;
    }

    const connection = rows[0];

    if (
      connection.verification_job_id &&
      connection.verification_job_id !== String(jobId)
    ) {
      console.warn("Skipping outdated verification job", jobId, "expected", connection.verification_job_id);
      return;
    }

    // // Decrypt DB password only inside worker
    // connection.db_user_secret = decrypt(connection.db_user_secret);

    // // Run DB-specific verification
    // switch (connection.db_type) {
    //   case "postgres":
    //     await verifyPostgres(connection);
    //     break;

    //   // case "mysql":
    //   //   await verifyMySQL(connection);
    //   //   break;

    //   // case "mongo":
    //   //   await verifyMongo(connection);
    //   //   break;

    //   default:
    //     throw new Error(`Unsupported db_type: ${connection.db_type}`);
    // }

    // Decrypt DB password only inside worker
    connection.db_user_secret = decrypt(connection.db_user_secret);

    await verifyConnectionCredentials(connection);


    // Mark VERIFIED
    await pool.query(
      `
      UPDATE connections
      SET status = 'VERIFIED',
        verified_at = now(),
        verification_started_at = NULL,
        error_message = NULL
      WHERE id = $1 
        AND status = 'VERIFYING';
      `,
      [connectionId]
    );

  } catch (error) {
    console.error("Verification failed:", error.message);

    // Mark ERROR 
    await pool.query(
      `
      UPDATE connections
      SET status = 'ERROR',
        verification_started_at = NULL,
        error_message = $1
      WHERE id = $2
        AND status = 'VERIFYING';
      `,
      [error.message, connectionId]
    );
  }
}

module.exports = { handleVerificationJob };