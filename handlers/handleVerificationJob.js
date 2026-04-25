const { pool } = require("../db/index");
const { decrypt } = require("../utils/crypto");
const { insertAuditLog } = require("../utils/auditLogger");

const { verifyConnectionCredentials } = require("../verifiers/verifyConnectionCredentials");

async function handleVerificationJob(job) {
  const { connectionId } = job.data;
  const jobId = String(job.id);

  try {
    // Re-fetch from DB
    const { rows } = await pool.query(
      `SELECT * FROM connections WHERE id = $1`,
      [connectionId]
    );

    if (!rows.length) {
      console.warn("Verification job skipped: connection not found", connectionId);
      await insertAuditLog({
        roleAtTime: "SYSTEM",
        actionType: "CONNECTION_VERIFICATION_COMPLETED",
        actionCategory: "DATABASE",
        resourceType: "CONNECTION",
        resourceId: connectionId,
        message: "Connection verification skipped because connection was not found",
        status: "FAILED",
        errorMessage: "Connection not found",
      });
      return;
    }

    const connection = rows[0];

    if (
      connection.verification_job_id &&
      connection.verification_job_id !== String(jobId)
    ) {
      console.warn("Skipping outdated verification job", jobId, "expected", connection.verification_job_id);
      await insertAuditLog({
        roleAtTime: "SYSTEM",
        actionType: "CONNECTION_VERIFICATION_COMPLETED",
        actionCategory: "DATABASE",
        resourceType: "CONNECTION",
        resourceId: connectionId,
        message: "Connection verification skipped because job is outdated",
        status: "DENIED",
        errorMessage: "Outdated verification job",
        metadata: { jobId, expectedJobId: connection.verification_job_id },
      });
      return;
    }

    // Decrypt DB password only if it is not null
    if (connection.db_user_secret) {
      connection.db_user_secret = decrypt(connection.db_user_secret);
    }

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

    await insertAuditLog({
      roleAtTime: "SYSTEM",
      actionType: "CONNECTION_VERIFICATION_COMPLETED",
      actionCategory: "DATABASE",
      resourceType: "CONNECTION",
      resourceId: connectionId,
      message: "Connection verification completed successfully",
      status: "SUCCESS",
    });

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

    await insertAuditLog({
      roleAtTime: "SYSTEM",
      actionType: "CONNECTION_VERIFICATION_COMPLETED",
      actionCategory: "DATABASE",
      resourceType: "CONNECTION",
      resourceId: connectionId,
      message: "Connection verification failed",
      status: "FAILED",
      errorMessage: error.message,
    });
  }
}

module.exports = { handleVerificationJob };
