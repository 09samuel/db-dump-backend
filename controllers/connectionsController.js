const { pool } = require("../db/index");
const { encrypt } = require("../utils/crypto");
const { enqueueVerificationJob } = require("../queue/verification.queue");
const { mapConnectionSummary } = require("../mappers/connectionsMapper")
const { verifyConnectionCredentials } = require("../verifiers/verifyConnectionCredentials");

const VERIFY_TIMEOUT_MINUTES = 5;

const DEFAULT_BACKUP_SETTINGS = {
  storageTarget: "LOCAL",
  localStoragePath: "/var/backups",
  retentionEnabled: false,
  defaultBackupType: "FULL",
  schedulingEnabled: false,
  cronExpression: null,
  timeoutMinutes: 30
};


async function addConnection(req, res) {
  const client = await pool.connect();

  try {
    const {
      dbType,
      dbHost,
      dbPort,
      dbName,
      envTag,
      dbUserName,
      dbUserSecret
    } = req.body;

    if (!dbType || !dbHost || !dbPort || !dbName || !envTag || !dbUserName || !dbUserSecret) {
      return res.status(400).json({ error: "All fields are required" });
    }

    await client.query("BEGIN");

    const encryptedSecret = encrypt(dbUserSecret);

    const insertConnectionQuery = `
      INSERT INTO connections (
        db_type,
        db_host,
        db_port,
        db_name,
        env_tag,
        db_user_name,
        db_user_secret,
        status
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      RETURNING id, db_type, db_name, env_tag, status, created_at
    `;

    const connectionResult = await client.query(insertConnectionQuery, [
      dbType,
      dbHost,
      dbPort,
      dbName,
      envTag,
      dbUserName,
      encryptedSecret,
      "CREATED"
    ]);

    const connectionId = connectionResult.rows[0].id;

    const insertBackupSettingsQuery = `
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
        timeout_minutes
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    `;

    const d = DEFAULT_BACKUP_SETTINGS;

    await client.query(insertBackupSettingsQuery, [
      connectionId,
      d.storageTarget,
      d.localStoragePath,
      d.retentionEnabled,
      d.retentionEnabled ? d.retentionMode : null,
      d.retentionEnabled ? d.retentionValue : null,
      d.defaultBackupType,
      d.schedulingEnabled,
      d.cronExpression,
      d.timeoutMinutes
    ]);

    await client.query("COMMIT");

    return res.status(201).json({
      message: "Database connection added successfully",
      connection: connectionResult.rows[0]
    });

  } catch (err) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackErr) {
        console.error("Rollback failed:", rollbackErr);
      }
      console.error("Add connection error:", err);
      return res.status(500).json({ error: "Internal server error" });
  } finally {
      client.release();
  }
}


async function verifyConnection (req, res) {
  const client = await pool.connect();

  try {
    const { id } = req.params;   

    // if (!isUUID(id)) { // return res.status(400).json({ error: "Invalid connection id" }); // }

    const jobId = `verify:${id}:${Date.now()}`;

    await client.query("BEGIN");

    // Acquire verification lease + store jobId
    const { rows } = await client.query (
      `
      UPDATE connections
      SET status = 'VERIFYING',
          verification_started_at = now(),
          verification_job_id = $2,
          error_message = NULL
      WHERE id = $1
        AND (
          status IN ('CREATED', 'ERROR')
          OR (
            status = 'VERIFYING'
            AND verification_started_at < now() - interval '${VERIFY_TIMEOUT_MINUTES} minutes'
          )
        )
      RETURNING id
      `,
      [id, jobId]
    );

    if (!rows.length) {
      await client.query("ROLLBACK");
      return res.status(409).json({
        error: "Verification already in progress or invalid state",
      });
    }

    await client.query("COMMIT");

    // Enqueue job
    try {
      await enqueueVerificationJob({ connectionId: id }, jobId);
    } catch (enqueueError) {
      // Rollback state on enqueue failure
      await pool.query(
        `
        UPDATE connections
        SET status = 'ERROR',
            verification_job_id = NULL,
            verification_started_at = NULL,
            error_message = 'Failed to enqueue verification job'
        WHERE id = $1
        `,
        [id]
      );

      return res.status(503).json({
        error: "Verification could not be started. Please retry.",
      });
    }

    return res.json({
      connectionId: id,
      status: "VERIFYING",
    });


  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Verify connection error:", error);

    return res.status(500).json({
      error: "Internal server error",
    });
  } finally {
    client.release();
  }
}



async function verifyConnectionDryRun(req, res) {
  try {
    console.log("verify-dry-run called");

    const { dbType, dbHost, dbPort, dbName, dbUserName, dbUserSecret} = req.body;

    //Input validation
    if (!dbType || !dbHost || !dbPort || !dbName || !dbUserName || !dbUserSecret) {
      return res.status(400).json({
        verified: false,
        error: "All fields are required",
      });
    }

    //hard timeout
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 5000);

    try {
      await verifyConnectionCredentials(
        {
          db_type: dbType.toLowerCase(),
          db_host: dbHost,
          db_port: dbPort,
          db_name: dbName,
          db_user_name: dbUserName,
          db_user_secret: dbUserSecret,
        },
        { signal: controller.signal }
      );

      console.log("Dry-run verification succeeded");

      return res.json({ verified: true });

    } catch (err) {

      console.error("Dry-run verification failed:", err.message);
      return res.status(422).json({
        verified: false,
        error: err.message || "Verification failed",
      });
    } finally {
      clearTimeout(timeout);
    }

  } catch (error) {
    console.error("verify-dry-run error:", error);

    return res.status(500).json({
      verified: false,
      error: "Internal server error",
    });
  }
}


async function getConnectionStatus (req, res) {
  try {
    const { id } = req.params;  

    const { rows } = await pool.query(
      `
      SELECT id, status, error_message, verified_at, verification_started_at
      FROM connections
      WHERE id = $1
      `,
      [id]
    );

    if (!rows.length) {
      return res.status(404).json({ error: "Connection not found" });
    }

    return res.json({
      connectionId: rows[0].id,
      status: rows[0].status,
      errorMessage: rows[0].error_message,
      verificationStartedAt: rows[0].verification_started_at,
      verifiedAt: rows[0].verified_at,
    });

  } catch (error) {
    console.error("Get connection status error:", error); 
    return res.status(500).json({
      error: "Internal server error",
    });
  }
};


async function getConnnectionsSummary (req, res) {
  try{
    const { rows } = await pool.query(
      `
      WITH latest_backup AS (
        SELECT DISTINCT ON (connection_id)
          connection_id,
          created_at,
          backup_size_bytes
        FROM backups
        ORDER BY connection_id, created_at DESC
      )

      SELECT
        c.id,
        c.db_name,
        c.db_type,
        c.env_tag,
        c.status,

        lb.created_at AS last_backup_at,
        CASE
          WHEN lb.connection_id IS NOT NULL THEN 'COMPLETED'
          ELSE NULL
        END AS backup_status,

        COALESCE(SUM(b.backup_size_bytes), 0) AS storage_used_bytes

      FROM connections c
      LEFT JOIN backups b
        ON b.connection_id = c.id
      LEFT JOIN latest_backup lb
        ON lb.connection_id = c.id

      GROUP BY c.id, lb.created_at, lb.connection_id
      ORDER BY c.created_at DESC;
      `
    )

    return res.json({
        data: rows.map(mapConnectionSummary),
    });

  } catch (error) {
      console.error("Get connections summary error:", error);
      return res.status(500).json({
        error: "Failed to fetch connections summary",
      });
    }
  
}


async function getConnectionDetails (req, res) {
  try{
    const { id } = req.params; 

    const { rows } = await pool.query(
      `
      SELECT
        c.db_name     AS "dbName",
        c.db_host     AS "dbHost",
        c.db_port     AS "dbPort",
        c.db_type     AS "dbEngine",
        c.env_tag     AS "environment",
        c.db_user_name AS "dbUsername"
      FROM connections c
      WHERE c.id = $1;
      `,[id]
    )

    if (rows.length === 0) {
      return res.status(404).json({
        error: "Connection not found",
      });
    }

    return res.json({
      data: rows[0], 
    });

  } catch (error) {
      console.error("Get connection details error:", error);
      return res.status(500).json({
        error: "Failed to fetch connection details",
      });
    }
  
}


async function getConnectionOverview(req, res) {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      `
      WITH latest_backup AS (
        SELECT DISTINCT ON (connection_id)
          connection_id,
          created_at     AS last_backup_at,
          storage_target AS last_storage_target
        FROM backups
        WHERE connection_id = $1
        ORDER BY connection_id, created_at DESC
      ),
      storage_usage AS (
        SELECT
          connection_id,
          COALESCE(SUM(backup_size_bytes), 0) AS storage_used_bytes
        FROM backups
        WHERE connection_id = $1
        GROUP BY connection_id
      )
      SELECT
        c.db_name,
        c.db_type,
        c.env_tag,
        c.db_host,
        c.db_port,
        c.status,

        lb.last_backup_at,
        lb.last_storage_target,

        COALESCE(su.storage_used_bytes, 0) AS storage_used_bytes
      FROM connections c
      LEFT JOIN latest_backup lb
        ON lb.connection_id = c.id
      LEFT JOIN storage_usage su
        ON su.connection_id = c.id
      WHERE c.id = $1;
      `,
      [id]
    );

    if (!rows.length) {
      return res.status(404).json({ error: "Connection not found" });
    }

    return res.json({ data: rows[0] });
  } catch (error) {
    console.error("Get connection overview error:", error);
    return res.status(500).json({
      error: "Failed to fetch connection overview",
    });
  }
}

async function getConnectionBasicDetails(req, res) {
  console.log("get connection basic details hit")
  try {
    const { id } = req.params;

    const { rows } =  await pool.query(
      `
      SELECT 
        db_name,
        db_type,
        env_tag,
        status
      FROM connections
      WHERE id = $1
      `,[id]
    )

    if (!rows.length) {
      return res.status(404).json({ error: "Connection not found" });
    }
    
    return res.json({
      data: rows[0]
    })
  } catch (error) {
     console.error("Get connection basic details error:", error);
    return res.status(500).json({
      error: "Failed to connection basic details overview",
    });
  }
}

async function updateDatabaseDetails(req, res) {
  try {
    const { id } = req.params;
    const { dbName, dbHost, dbPort, dbEngine, environment, dbUsername, dbUserSecret,} = req.body;

    const fields = [];
    const values = [];
    let index = 1;

    const credentialFieldsChanged = dbHost !== undefined || dbPort !== undefined || dbEngine !== undefined || dbUsername !== undefined || dbUserSecret !== undefined;

    if (dbName !== undefined) {
      fields.push(`db_name = $${index++}`);
      values.push(dbName);
    }

    if (dbHost !== undefined) {
      fields.push(`db_host = $${index++}`);
      values.push(dbHost);
    }

    if (dbPort !== undefined) {
      fields.push(`db_port = $${index++}`);
      values.push(dbPort);
    }

    if (dbEngine !== undefined) {
      fields.push(`db_type = $${index++}`);
      values.push(dbEngine);
    }

    if (environment !== undefined) {
      fields.push(`env_tag = $${index++}`);
      values.push(environment);
    }

    if (dbUsername !== undefined) {
      fields.push(`db_user_name = $${index++}`);
      values.push(dbUsername);
    }

    if (dbUserSecret !== undefined) {
      fields.push(`db_user_secret = $${index++}`);
      const encryptedSecret = encrypt(dbUserSecret);
      values.push(encryptedSecret);
    }

    //reset validation state
    if (credentialFieldsChanged) {
      fields.push(`status = 'CREATED'`);
      fields.push(`verified_at = NULL`);
      fields.push(`verification_started_at = NULL`);
      fields.push(`verification_job_id = NULL`);
      fields.push(`error_message = NULL`);
    }

    if (fields.length === 0) {
      return res.status(400).json({ error: "No fields provided for update" });
    }

    values.push(id);

    // Update connection
    const updateQuery = `
      UPDATE connections
      SET ${fields.join(", ")}
      WHERE id = $${index}
      RETURNING id;
    `;

    const updateResult = await pool.query(updateQuery, values);

    if (updateResult.rows.length === 0) {
      return res.status(404).json({ error: "Connection not found" });
    }

    return res.status(204).send();

  } catch (error) {
    console.error("Update database error:", error);
    return res.status(500).json({ error: "Failed to update database" });
  }
}


async function deleteConnection(req, res) {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      `
      DELETE FROM connections
      WHERE id = $1
      RETURNING id;
      `,
      [id]
    );

    if (rows.length === 0) {
      return res.status(404).json({ error: "Connection not found" });
    }

    return res.json({ message: "Connection deleted successfully" });
  } catch (error) {
    console.error("Delete connection error:", error);
    return res.status(500).json({ error: "Failed to delete connection" });
  }
}

module.exports = { addConnection, verifyConnection, verifyConnectionDryRun, getConnectionStatus, getConnnectionsSummary, getConnectionDetails, getConnectionOverview, getConnectionBasicDetails, updateDatabaseDetails, deleteConnection };