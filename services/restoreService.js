const { pool } = require("../db/index");
const logger = require("../utils/logger");
const { enqueueRestoreDBJob }= require("../queue/restore_db.queue")
const { insertAuditLog, resolveActorContext } = require("../utils/auditLogger");

async function requestRestore(dbId, backupId, actorInput = {}) {
    const client = await pool.connect();
    let actor = { userId: null, userEmail: null, roleAtTime: "SYSTEM" };
    let auditLogged = false;

    try {
        actor = await resolveActorContext({
            userId: actorInput.userId || null,
            connectionId: dbId,
            roleAtTime: actorInput.roleAtTime || null,
            client,
        });

    const restore = await (async () => {

        logger.debug(`Request restore - dbId: ${dbId}, backupId: ${backupId}`);
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
            `INSERT INTO restores (
              connection_id,
              backup_id,
              status,
              triggered_by,
              actor_user_id,
              actor_user_email,
              actor_role_at_time,
              actor_ip_address,
              actor_user_agent
            )
            VALUES ($1, $2, 'QUEUED', $3, $3, $4, $5, $6, $7)
            RETURNING *`,
            [
              dbId,
              backupId,
              actor.userId,
              actor.userEmail,
              actor.roleAtTime,
              actorInput.ipAddress || null,
              actorInput.userAgent || null,
            ]
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
            await insertAuditLog({
                ...actor,
                ipAddress: actorInput.ipAddress || null,
                userAgent: actorInput.userAgent || null,
                actionType: "RESTORE_REQUESTED",
                actionCategory: "RESTORE",
                resourceType: "RESTORE",
                resourceId: restore.id,
                message: "Restore request failed: queue enqueue failed",
                status: "FAILED",
                errorMessage: err.message,
                metadata: { connectionId: dbId, backupId },
            });
            auditLogged = true;
            throw err;
        }

        await insertAuditLog({
            ...actor,
            ipAddress: actorInput.ipAddress || null,
            userAgent: actorInput.userAgent || null,
            actionType: "RESTORE_REQUESTED",
            actionCategory: "RESTORE",
            resourceType: "RESTORE",
            resourceId: restore.id,
            message: "Restore job queued successfully",
            status: "SUCCESS",
            metadata: { connectionId: dbId, backupId },
        });

        return restore;

    } catch (err) {
        try {
            await client.query("ROLLBACK");
        } catch (e) {
        
        }
        if (!auditLogged) {
          await insertAuditLog({
            ...actor,
            ipAddress: actorInput.ipAddress || null,
            userAgent: actorInput.userAgent || null,
            actionType: "RESTORE_REQUESTED",
            actionCategory: "RESTORE",
            resourceType: "CONNECTION",
            resourceId: dbId,
            message: err.status && err.status >= 400 && err.status < 500
              ? "Restore request denied"
              : "Restore request failed",
            status: err.status && err.status >= 400 && err.status < 500 ? "DENIED" : "FAILED",
            errorMessage: err.message,
            metadata: { connectionId: dbId, backupId },
          });
        }
        throw err;
    } finally {
        client.release()
    }
}

module.exports = { requestRestore };
