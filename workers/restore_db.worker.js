const { Worker } = require("bullmq");
const {handleRestoreDBJob} = require("../handlers/handleRestoreDBJob");

console.log("Restore worker started");

const worker = new Worker(
  "restore-db",
  async (job) => {
    console.log("JOB RECEIVED BY WORKER", job.id, job.name);
    await handleRestoreDBJob(job);
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


