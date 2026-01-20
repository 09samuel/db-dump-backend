require('dotenv').config();
const { pool } = require("../db/index");
const { decrypt } = require("../utils/crypto");
const { getBackupCommand } = require("../backup/strategy");
const { createStorageStream } = require("../storage/writer");
const { runBackup } = require("../backup/executor");

async function handleBackupDBJob(job) {
    const jobId = job?.data?.jobId;
    if (!jobId) return;

    console.log("Starting backup DB job:", jobId);


    let decryptedPassword;

    try{
        const MAX_BACKUP_RUNTIME_MINUTES = 60;

        const { rows } = await pool.query(
            `
            UPDATE backup_jobs bj
            SET status = 'RUNNING',
                started_at = now(),
                error = NULL
            FROM connections c
            WHERE bj.id = $1
                AND (
                    bj.status = 'QUEUED'
                    OR (
                        bj.status = 'RUNNING'
                        AND bj.started_at < now() - ($2 || ' minutes')::interval
                    )
                )
                AND bj.connection_id = c.id
            RETURNING
                bj.id,
                bj.storage_target,
                bj.backup_type,
                bj.backup_name,
                c.db_type,
                c.db_host,
                c.db_port,
                c.db_name,
                c.db_user_name,
                c.db_user_secret;
            `,
            [jobId, MAX_BACKUP_RUNTIME_MINUTES]
        )

        if (!rows.length) {
            console.warn("Backup job skipped: Job already processed or not found", jobId);
            return;
        }

        const jobData = rows[0];

        try {
            decryptedPassword = decrypt(jobData.db_user_secret);
        } catch {
            await pool.query(
                `
                UPDATE backup_jobs
                SET status = 'ERROR',
                    finished_at = now(),
                    error = 'Failed to decrypt database credentials'
                WHERE id = $1
                    AND status = 'RUNNING'
                `,
                [jobId]
            );
            
            return;
        }


        // Validate required fields
        if (
        !jobData.db_host || !jobData.db_name || !jobData.db_user_name || !jobData.db_port) {
            await pool.query(
                `
                UPDATE backup_jobs
                SET status = 'ERROR',
                    finished_at = now(),
                    error = 'Invalid connection configuration'
                WHERE id = $1
                    AND status = 'RUNNING'
                `,
                [jobId]
            );

            return;
        }

        if (!jobData.storage_target) {
            await pool.query(
                `
                UPDATE backup_jobs
                SET status = 'ERROR',
                    finished_at = now(),
                    error = 'Missing storage target'
                WHERE id = $1
                    AND status = 'RUNNING'
                `,
                [jobId]
            );
            return;
        }



        // Choose storage strategy and backup command
        let command;
        try {
            command = getBackupCommand(jobData.db_type, jobData.backup_type, {
                host: jobData.db_host,
                port: jobData.db_port,
                user: jobData.db_user_name,
                password: decryptedPassword,
                database: jobData.db_name,
            });
        } catch (err) {
            console.error("Error getting backup command:", err);

            await pool.query(`
                UPDATE backup_jobs
                SET status = 'ERROR', finished_at = now(), 
                    error = 'Unsupported database type'
                WHERE id = $1
                    AND status = 'RUNNING';
            `, [jobId]);
            return;
        }

        // Storage stream
        const storage = createStorageStream(jobData.storage_target);

        // Execute
        try {
            const bytesWritten = await runBackup(command, storage);

            await pool.query(`
                UPDATE backup_jobs
                SET status = 'SUCCESS', 
                    finished_at = now(),
                    backup_size_bytes = $2,
                    error = NULL
                WHERE id = $1
                    AND status = 'RUNNING';
            `, [jobId, bytesWritten]);

        } catch (err) {
            console.error("Backup execution error:", err);

            const errorMessage = getBackupExecutionErrorMessage(err);

            await pool.query(`
                UPDATE backup_jobs
                SET status = 'ERROR',
                    finished_at = now(),
                    error = $2
                WHERE id = $1
                    AND status = 'RUNNING';
            `, [jobId, errorMessage]);
        }

    } catch (error) {
        console.error("Error handling backup DB job:", error);

        await pool.query(
            `
            UPDATE backup_jobs
            SET status = 'ERROR',
                finished_at = now(),
                error = 'Backup execution failed'
            WHERE id = $1
                AND status = 'RUNNING';
            `,
            [jobId]
        );
    } finally {
        decryptedPassword = null; // Clear sensitive data
    }
}


function getBackupExecutionErrorMessage(err) {
    if (!err) return 'Unknown backup execution error';

    if (err.code === 'ENOENT') {
        return 'Backup tool not available on server';
    }

    if (err.code === 'ETIMEDOUT') {
        return 'Backup exceeded maximum runtime';
    }

    if (err.message?.includes('authentication') || err.message?.includes('password')) {
        return 'Database authentication failed';
    }

    if (err.message?.includes('permission denied')) {
        return 'Insufficient permissions to perform backup';
    }

    if (err.message?.includes('write') || err.message?.includes('stream')) {
        return 'Failed to write backup to storage';
    }

    if (typeof err.exitCode === 'number') {
        return `Backup process exited with code ${err.exitCode}`;
    }

    return 'Unexpected error during backup execution';
}


module.exports = { handleBackupDBJob };