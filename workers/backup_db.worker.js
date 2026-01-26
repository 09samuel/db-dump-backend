const { Worker } = require("bullmq");
const {handleBackupDBJob} = require("../handlers/handleBackupDBJob");

console.log("Backup worker started");

const worker = new Worker(
  "backup-db",
  async (job) => {
    console.log("JOB RECEIVED BY WORKER", job.id, job.name);
    await handleBackupDBJob(job);
  },
  {
    connection: { host: "localhost", port: 6379 },
  }
);

worker.on("failed", (job, err) => {
  console.error("JOB FAILED", job?.id, err);
});

worker.on("error", (err) => {
  console.error("WORKER ERROR", err);
});


