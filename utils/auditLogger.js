const { pool } = require("../db/index");
const logger = require("./logger");

async function resolveActorContext({ userId = null, connectionId = null, roleAtTime = null, client = null} = {}) {
  if (!userId) {
    return {
      userId: null,
      userEmail: null,
      roleAtTime: roleAtTime || "SYSTEM",
    };
  }

  const db = client || pool;

  try {
    const { rows } = await db.query(
      `
      SELECT
        u.id AS user_id,
        u.email AS user_email,
        ucr.role AS connection_role
      FROM users u
      LEFT JOIN user_connection_roles ucr
        ON ucr.user_id = u.id
       AND ($2::uuid IS NULL OR ucr.connection_id = $2)
      WHERE u.id = $1
      LIMIT 1
      `,
      [userId, connectionId]
    );

    if (!rows.length) {
      return {
        userId,
        userEmail: null,
        roleAtTime: roleAtTime || "SYSTEM",
      };
    }

    return {
      userId: rows[0].user_id,
      userEmail: rows[0].user_email || null,
      roleAtTime: roleAtTime || rows[0].connection_role || "SYSTEM",
    };
  } catch (error) {
    logger.error("resolveActorContext error:", error);

    return {
      userId,
      userEmail: null,
      roleAtTime: roleAtTime || "SYSTEM",
    };
  }
}

function getRequestMeta(req) {
  return {
    ipAddress: req?.ip || null,
    userAgent: req?.headers?.["user-agent"] || null,
  };
}

async function insertAuditLog(
  {
    userId = null,
    userEmail = null,
    roleAtTime = "SYSTEM",
    actionType,
    actionCategory,
    resourceType = null,
    resourceId = null,
    resourceName = null,
    message,
    ipAddress = null,
    userAgent = null,
    status,
    errorMessage = null,
    metadata = {},
  },
  { client = null, throwOnError = false } = {}
) {
  const db = client || pool;

  try {
    await db.query(
      `
      INSERT INTO audit_logs (
        user_id,
        user_email,
        role_at_time,
        action_type,
        action_category,
        resource_type,
        resource_id,
        resource_name,
        message,
        ip_address,
        user_agent,
        status,
        error_message,
        metadata
      )
      VALUES (
        $1, $2, $3, $4, $5,
        $6, $7, $8, $9, $10,
        $11, $12, $13, $14::jsonb
      )
      `,
      [
        userId,
        userEmail,
        roleAtTime || "SYSTEM",
        actionType,
        actionCategory,
        resourceType,
        resourceId,
        resourceName,
        message,
        ipAddress,
        userAgent,
        status,
        errorMessage,
        JSON.stringify(metadata || {}),
      ]
    );
  } catch (error) {
    logger.error("insertAuditLog error:", error);
    if (throwOnError) {
      throw error;
    }
  }
}

module.exports = { getRequestMeta, insertAuditLog, resolveActorContext};
