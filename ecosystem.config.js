module.exports = {
  apps: [
    {
        name: "server",
        script: "server.js",
        // instances: 2,           // run 2 instances (for the server only)
        // autorestart: true,      // restart if it crashes
        // max_memory_restart: "300M", // restart if memory exceeds 300MB
        env: {
            NODE_ENV: "production"
        }
    },
    {
        name: "verificationWorker",
        script: "workers/verification.worker.js",
    },
    {
        name: "backupWorker",
        script: "workers/backup_db.worker.js",
    },
    {
        name: "restoreWorker",
        script: "workers/restore_db.worker.js",
    },
    {
        name: "emailWorker",
        script: "workers/email.worker.js",
    },
    {
        name: "scheduler",
        script: "scheduler/index.js",
    },
  ],
}