const { pool } = require("../db/index");

function parseCursor(cursor) {
  if (!cursor) return { createdAt: null, id: null };

  try {
    const decoded = JSON.parse(Buffer.from(cursor, "base64").toString());
    return {
      createdAt: decoded.createdAt || null,
      id: decoded.id || null,
    };
  } catch {
    return null;
  }
}

function buildNextCursor(rows, limit) {
  if (rows.length !== limit) return null;
  const last = rows[rows.length - 1];
  return Buffer.from(
    JSON.stringify({
      createdAt: last.created_at,
      id: last.id,
    })
  ).toString("base64");
}

function parseSortDirection(sort) {
  const normalized = String(sort || "DESC").trim().toUpperCase();
  return normalized === "ASC" ? "ASC" : "DESC";
}

async function getUserAuditLogs(req, res) {
  try {
    const userId = req.user.userId;
    const {
      status = null,
      actionCategory = null,
      actionType = null,
      from = null,
      to = null,
      cursor = null,
      search = null,
      sort = "DESC",
      limit: rawLimit,
    } = req.query;

    const limit = Math.min(parseInt(rawLimit, 10) || 25, 100);
    const parsedCursor = parseCursor(cursor);

    if (parsedCursor === null) {
      return res.status(400).json({ error: "Invalid cursor" });
    }

    const { createdAt: cursorCreatedAt, id: cursorId } = parsedCursor;
    const sortDirection = parseSortDirection(sort);
    const cursorComparison = sortDirection === "ASC" ? ">" : "<";
    const searchTerm = search && String(search).trim() ? `%${String(search).trim()}%` : null;

    const { rows } = await pool.query(
      `
      SELECT
        id,
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
        metadata,
        created_at
      FROM audit_logs
      WHERE user_id = $1
        AND ($2::text IS NULL OR status::text = $2)
        AND ($3::text IS NULL OR action_category::text = $3)
        AND ($4::text IS NULL OR action_type = $4)
        AND ($5::timestamptz IS NULL OR created_at >= $5)
        AND ($6::timestamptz IS NULL OR created_at <= $6)
        AND (
          $7::timestamptz IS NULL
          OR (created_at, id) ${cursorComparison} ($7::timestamptz, $8::uuid)
        )
        AND (
          $9::text IS NULL
          OR user_email ILIKE $9
          OR message ILIKE $9
          OR resource_name ILIKE $9
          OR action_type ILIKE $9
          OR action_category::text ILIKE $9
          OR resource_type::text ILIKE $9
        )
      ORDER BY created_at ${sortDirection}, id ${sortDirection}
      LIMIT $10;
      `,
      [
        userId,
        status ? String(status).trim().toUpperCase() : null,
        actionCategory ? String(actionCategory).trim().toUpperCase() : null,
        actionType ? String(actionType).trim() : null,
        from || null,
        to || null,
        cursorCreatedAt,
        cursorId,
        searchTerm,
        limit,
      ]
    );

    return res.json({
      data: rows,
      nextCursor: buildNextCursor(rows, limit),
      hasMore: rows.length === limit,
    });
  } catch (error) {
    console.error("Get user audit logs error:", error);
    return res.status(500).json({ error: "Failed to fetch audit logs" });
  }
}

async function getConnectionAuditLogs(req, res) {
  try {
    const { connectionId } = req.params;
    const {
      status = null,
      actionCategory = null,
      actionType = null,
      from = null,
      to = null,
      cursor = null,
      search = null,
      sort = "DESC",
      limit: rawLimit,
    } = req.query;

    const limit = Math.min(parseInt(rawLimit, 10) || 25, 100);
    const parsedCursor = parseCursor(cursor);

    if (parsedCursor === null) {
      return res.status(400).json({ error: "Invalid cursor" });
    }

    const { createdAt: cursorCreatedAt, id: cursorId } = parsedCursor;
    const sortDirection = parseSortDirection(sort);
    const cursorComparison = sortDirection === "ASC" ? ">" : "<";
    const searchTerm = search && String(search).trim() ? `%${String(search).trim()}%` : null;

    const { rows } = await pool.query(
      `
      SELECT
        al.id,
        al.user_id,
        al.user_email,
        al.role_at_time,
        al.action_type,
        al.action_category,
        al.resource_type,
        al.resource_id,
        al.resource_name,
        al.message,
        al.ip_address,
        al.user_agent,
        al.status,
        al.error_message,
        al.metadata,
        al.created_at
      FROM audit_logs al
      LEFT JOIN backup_jobs bj
        ON al.resource_type = 'BACKUP_JOB'
       AND bj.id = al.resource_id
      LEFT JOIN restores r
        ON al.resource_type = 'RESTORE'
       AND r.id = al.resource_id
      LEFT JOIN backups b
        ON al.resource_type = 'BACKUP'
       AND b.id = al.resource_id AND b.deleted_at IS NULL
      WHERE (
        (al.resource_type = 'CONNECTION' AND al.resource_id = $1::uuid)
        OR (al.metadata->>'connectionId' = $1::text)
        OR (bj.connection_id = $1::uuid)
        OR (r.connection_id = $1::uuid)
        OR (b.connection_id = $1::uuid)
      )
        AND ($2::text IS NULL OR al.status::text = $2)
        AND ($3::text IS NULL OR al.action_category::text = $3)
        AND ($4::text IS NULL OR al.action_type = $4)
        AND ($5::timestamptz IS NULL OR al.created_at >= $5)
        AND ($6::timestamptz IS NULL OR al.created_at <= $6)
        AND (
          $7::timestamptz IS NULL
          OR (al.created_at, al.id) ${cursorComparison} ($7::timestamptz, $8::uuid)
        )
        AND (
          $9::text IS NULL
          OR al.user_email ILIKE $9
          OR al.message ILIKE $9
          OR al.resource_name ILIKE $9
          OR al.action_type ILIKE $9
          OR al.action_category::text ILIKE $9
          OR al.resource_type::text ILIKE $9
        )
      ORDER BY al.created_at ${sortDirection}, al.id ${sortDirection}
      LIMIT $10;
      `,
      [
        connectionId,
        status ? String(status).trim().toUpperCase() : null,
        actionCategory ? String(actionCategory).trim().toUpperCase() : null,
        actionType ? String(actionType).trim() : null,
        from || null,
        to || null,
        cursorCreatedAt,
        cursorId,
        searchTerm,
        limit,
      ]
    );

    return res.json({
      data: rows,
      nextCursor: buildNextCursor(rows, limit),
      hasMore: rows.length === limit,
    });
  } catch (error) {
    console.error("Get connection audit logs error:", error);
    return res.status(500).json({ error: "Failed to fetch audit logs" });
  }
}

module.exports = { getUserAuditLogs, getConnectionAuditLogs };
