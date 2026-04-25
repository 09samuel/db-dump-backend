require("dotenv").config();
const { pool } = require("../db/index");
const { decrypt } = require("../utils/crypto");
const storage = require("../storage/downloader")
const engineRestore = require("../restore/engineRestore")
const fs = require("fs/promises");
const os = require("os");
const { insertAuditLog } = require("../utils/auditLogger");


async function handleRestoreDBJob(job) {
    const { restoreId } = job.data;
    if (!restoreId) return;

    const runtime = {
        connectionId: null,
        backupPath: null,
        isTempFile: false,
        actor: {
            userId: null,
            userEmail: null,
            roleAtTime: "SYSTEM",
            ipAddress: null,
            userAgent: null,
        },
    };


    //claim restore atomically
    const { rows } = await pool.query(
        `
        UPDATE restores r
        SET status = 'IN_PROGRESS',
            started_at = now(),
            worker_id = $2,
            attempt = attempt + 1,
            error = NULL
        WHERE r.id = $1
        AND r.status = 'QUEUED'
        RETURNING
          r.id,
          r.connection_id,
          r.backup_id,
          r.actor_user_id,
          r.actor_user_email,
          r.actor_role_at_time,
          r.actor_ip_address,
          r.actor_user_agent;
        `,
        [restoreId, process.env.WORKER_ID || os.hostname()]
    );

    if (!rows.length) {
        //already processed or claimed
        return;
    }

    const restore = rows[0];
    runtime.connectionId = restore.connection_id;
    runtime.actor = {
        userId: restore.actor_user_id || null,
        userEmail: restore.actor_user_email || null,
        roleAtTime: restore.actor_role_at_time || "SYSTEM",
        ipAddress: restore.actor_ip_address || null,
        userAgent: restore.actor_user_agent || null,
    };

    try {
        //load restore context
        const { rows: ctxRows } = await pool.query(
            `
            SELECT
                c.db_type,
                c.db_host,
                c.db_port,
                c.db_name,
                c.db_user_name,
                c.db_user_secret,
                c.ssl_mode,

                b.storage_target,
                b.storage_path,
                b.checksum,

                bs.backup_restore_role_arn,
                bs.s3_bucket,
                bs.s3_region
            FROM restores r
            JOIN connections c
                ON c.id = r.connection_id
            JOIN backups b
                ON b.id = r.backup_id
                WHERE b.deleted_at IS NULL
            JOIN backup_settings bs
                ON bs.connection_id = c.id
            WHERE r.id = $1;
            `,
            [restoreId]   
        );


        if (!ctxRows.length) {
            throw new Error("Restore context not found");
        }

        const ctx = ctxRows[0];

        function decryptPassword(encrypted) {
            try {

                if(encrypted){
                    return decrypt(encrypted);
                }
            } catch {
                throw new Error("Failed to decrypt database credentials");
            }
        }


        //acquire restore lock on DB
        const res = await pool.query(
            `
            UPDATE connections
            SET restore_status = 'IN_PROGRESS'
            WHERE id = $1 AND restore_status = 'IDLE';
            `,
            [runtime.connectionId]
        );

        if (res.rowCount === 0) {
            throw new Error("Failed to acquire DB restore lock");
        }



        //resolve backup file
        runtime.backupPath = await resolveBackupPath(ctx);
        runtime.isTempFile = ctx.storage_target === "S3";


        if (!runtime.backupPath) {
            throw new Error("Backup file path could not be resolved");
        }

        //execute restore
        await engineRestore.restore({
            engine: ctx.db_type,
            host: ctx.db_host,
            port: ctx.db_port,
            database: ctx.db_name,
            username: ctx.db_user_name,
            password: decryptPassword(ctx.db_user_secret),
            backupPath: runtime.backupPath,
            checksumSha256: ctx.checksum,
            sslMode: ctx.ssl_mode,
            restoreMode: getRestoreMode(ctx)
        });

        //mark restore complete
        await pool.query(
            `
            UPDATE restores
            SET status = 'COMPLETED',
                finished_at = now()
            WHERE id = $1;
            `,
            [restoreId]
        );

        await insertAuditLog({
            ...runtime.actor,
            actionType: "RESTORE_COMPLETED",
            actionCategory: "RESTORE",
            resourceType: "RESTORE",
            resourceId: restoreId,
            message: "Restore completed successfully",
            status: "SUCCESS",
            metadata: {
                connectionId: runtime.connectionId,
            },
        });
    } catch (err) {
        console.error("RESTORE FAILED:", err);
        await pool.query(
            `
            UPDATE restores
            SET status = 'FAILED',
                finished_at = now(),
                error = $2
            WHERE id = $1;
            `,
            [restoreId, err.message]
        );

        await insertAuditLog({
            ...runtime.actor,
            actionType: "RESTORE_COMPLETED",
            actionCategory: "RESTORE",
            resourceType: "RESTORE",
            resourceId: restoreId,
            message: "Restore failed",
            status: "FAILED",
            errorMessage: err.message,
            metadata: {
                connectionId: runtime.connectionId,
            },
        });
    } finally {
        // cleanup temp S3 file AFTER restore completes
        try {
            if (runtime.isTempFile && runtime.backupPath) {
                await fs.unlink(runtime.backupPath);
            }
        } catch (e) {
            console.warn("Temp cleanup failed:", e.message);
        }

        try {
            //release DB restore lock
            if (runtime.connectionId) {
                await pool.query(
                    `
                    UPDATE connections
                    SET restore_status = 'IDLE'
                    WHERE id = $1
                        AND restore_status = 'IN_PROGRESS';
                    `,
                [runtime.connectionId]
                );
            }
        } catch (err){
            console.error("Failed to reset restore_status", err);
        }
        
    }
}


async function resolveBackupPath(dbCtx) {
  if (dbCtx.storage_target === "LOCAL") {
    if (!dbCtx.storage_path) {
      throw new Error("Local backup path is missing");
    }

    try {
      await fs.access(dbCtx.storage_path);
    } catch {
      throw new Error(`Local backup file not found: ${dbCtx.storage_path}`);
    }

    return dbCtx.storage_path;
  }

  if (dbCtx.storage_target === "S3") {
    if (!dbCtx.backup_restore_role_arn) {
      throw new Error(
        "Restore Role ARN is not configured. Please configure it to restore from S3."
      );
    }

    return storage.downloadFromS3({
      bucket: dbCtx.s3_bucket,
      region: dbCtx.s3_region,
      s3Path: dbCtx.storage_path,
      roleArn: dbCtx.backup_restore_role_arn,
    });
  }

  throw new Error(`Unsupported storage target: ${dbCtx.storage_target}`);
}

function getRestoreMode(ctx) {
  // Supabase detection (best signal = host)
  if (ctx.db_type === "postgresql" && ctx.db_host.includes("supabase.co")) {
    return "schema";
  }

  return "database";
}

module.exports = { handleRestoreDBJob }
