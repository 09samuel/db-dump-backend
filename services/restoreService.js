const { pool } = require("../db/index");
const { enqueueRestoreDBJob }= require("../queue/restore_db.queue")

async function requestRestore(dbId, backupId) {
  const client = await pool.connect();

  try {
    const restore = await (async () => {

        console.log(dbId)
        console.log(backupId)
        await client.query("BEGIN");

        //lock database row
        const { rows: dbRows } = await client.query(
            `SELECT id, status, restore_status
            FROM connections
            WHERE id = $1
            FOR UPDATE`,
            [dbId]
        );

        if (!dbRows.length) {
            const err = new Error("Database not found");
            err.status = 404;
            throw err;
        }

        const database = dbRows[0];

        if (database.restore_status === "IN_PROGRESS") {
            const err = new Error("Restore already in progress");
            err.status = 409;
            throw err;
        }

        // ensure no backup job running
        const { rows: backupRunning } = await client.query(
        `SELECT 1
        FROM backup_jobs
        WHERE connection_id = $1
            AND status IN ('QUEUED', 'RUNNING')
        LIMIT 1`,
        [dbId]
        );

        if (backupRunning.length) {
            const err = new Error("Backup running, try later");
            err.status = 409;
            throw err;
        }

        //validate backup
        const { rows: backupRows } = await client.query(
            `SELECT 
                b.id,
                b.connection_id,
                b.backup_type,
                b.storage_target,
                bs.backup_restore_role_arn
            FROM backups b
            JOIN backup_settings bs
                ON bs.connection_id = b.connection_id
            WHERE b.id = $1`,
            [backupId]
        );

        if (!backupRows.length) {
            const err = new Error("Backup not found");
            err.status = 404;
            throw err;
        }

        const backup = backupRows[0];

        if (backup.connection_id !== dbId) {
            const err = new Error("Backup does not belong to this DB");
            err.status = 400;
            throw err;
        }

        if (backup.backup_type !== "FULL") {
            const err = new Error("Only FULL backups can be restored");
            err.status = 400;
            throw err;
        }

        if (backup.storage_target !== "S3") {
            const err = new Error("Backup is not stored in S3");
            err.status = 400;
            throw err;
        }

        const roleArn = backup.backup_restore_role_arn;
        const iamRoleArnRegex = /^arn:(aws|aws-us-gov|aws-cn):iam::\d{12}:role\/[\w+=,.@\-_/]+$/;

        if (!roleArn || !iamRoleArnRegex.test(roleArn)) {
            const err = new Error("Invalid or missing backup restore/download role ARN");
            err.status = 400;
            throw err;
        }

        //create restore record
        const { rows: restoreRows } = await client.query(
            `INSERT INTO restores (connection_id, backup_id, status)
            VALUES ($1, $2, 'QUEUED')
            RETURNING *`,
            [dbId, backupId]
        );

        //lock DB restore state
        // await client.query(
        //     `UPDATE connections
        //     SET restore_status = 'IN_PROGRESS'
        //     WHERE id = $1`,
        //     [dbId]
        // );

        await client.query("COMMIT");

        return restoreRows[0];
    })();

        //enqueue after commit
        try {
            await enqueueRestoreDBJob({ restoreId: restore.id });
        } catch (err) {
            await pool.query(
                `UPDATE restores
                SET status = 'FAILED'
                WHERE id = $1`,
                [restore.id]
            );
            throw err;
        }

        return restore;

    } catch (err) {
        try {
            await client.query("ROLLBACK");
        } finally {
            client.release();
        }
        throw err;
    } finally {
        client.release()
    }
}

module.exports = { requestRestore };
