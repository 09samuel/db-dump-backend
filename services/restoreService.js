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
            throw new Error("Database not found");
        }

        const database = dbRows[0];

        if (database.restore_status === "IN_PROGRESS") {
            throw new Error("Restore already in progress");
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
            throw new Error("Backup running, try later");
        }

        //validate backup
        const { rows: backupRows } = await client.query(
            `SELECT id, connection_id, backup_type
            FROM backups
            WHERE id = $1`,
            [backupId]
        );

        if (!backupRows.length) {
            throw new Error("Backup not found");
        }

        const backup = backupRows[0];

        if (backup.connection_id !== dbId) {
            throw new Error("Backup does not belong to this DB");
        }

        if (backup.backup_type !== "FULL") {
            throw new Error("Only FULL backups can be restored");
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
    }
}

module.exports = { requestRestore };
