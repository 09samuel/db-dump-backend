require("dotenv").config();
const { pool } = require("../db");
const { decrypt } = require("../utils/crypto");
const storage = require("../storage/downloader")
const engineRestore = require("../restore/engineRestore")
const fs = require("fs/promises");
const os = require("os");


async function handleRestoreDBJob(job) {
    const { restoreId } = job.data;
    if (!restoreId) return;

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
        RETURNING r.id, r.connection_id, r.backup_id;
        `,
        [restoreId, process.env.WORKER_ID || os.hostname()]
    );

    if (!rows.length) {
        //already processed or claimed
        return;
    }

    const restore = rows[0];

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

                b.storage_target,
                b.storage_path,

                bs.backup_restore_role_arn,
                bs.s3_bucket,
                bs.s3_region

            FROM connections c
            JOIN backups b
            ON b.id = $2
            JOIN backup_settings bs
            ON bs.connection_id = c.id
            WHERE c.id = $1;
            `,
            [restore.connection_id, restore.backup_id]
        );

        if (!ctxRows.length) {
            throw new Error("Restore context not found");
        }

        const ctx = ctxRows[0];

        function decryptPassword(encrypted) {
            try {
                return decrypt(encrypted);
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
            [restore.connection_id]
        );

        if (res.rowCount === 0) {
            throw new Error("Failed to acquire DB restore lock");
        }



        //resolve backup file
        const resolveBackupPath = async (ctx) => {
            if (ctx.storage_target === "LOCAL") {
                if (!ctx.storage_path) {
                throw new Error("Local backup path is missing");
                }

                try {
                    await fs.access(ctx.storage_path);
                } catch {
                        throw new Error(`Local backup file not found: ${ctx.storage_path}`);
                }

                return ctx.storage_path;
            }

            if (ctx.storage_target === "S3") {
                if (!ctx.backup_restore_role_arn) {
                    throw new Error("Restore Role ARN is not configured. Please configure it to restore from S3.");
                }

                return storage.downloadFromS3({
                    bucket: ctx.s3_bucket,
                    region: ctx.s3_region,
                    key: ctx.storage_path,
                    roleArn: ctx.backup_restore_role_arn
                });

            }

            throw new Error(`Unsupported storage target: ${ctx.storage_target}`);
        };

        const backupPath = await resolveBackupPath(ctx);

        if (!backupPath) {
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
            backupPath
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
    } catch (err) {
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
    } finally {
        //release DB restore lock
        await pool.query(
            `
            UPDATE connections
            SET restore_status = 'IDLE'
            WHERE id = $1
                AND restore_status = 'IN_PROGRESS';
            `,
        [restore.connection_id]
        );
    }
}


module.exports = { handleRestoreDBJob }