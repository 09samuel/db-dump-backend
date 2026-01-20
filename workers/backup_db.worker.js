const { Worker } = require("bullmq");
const {handleBackupDBJob} = require("../handlers/handleBackupDBJob");

console.log("Backup worker started");

new Worker(
  "backup-db",
  async (job) => {
    await handleBackupDBJob(job);
  },
  {
    connection: { host: "localhost", port: 6379 },
  }
);
