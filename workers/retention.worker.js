const { Worker } = require("bullmq");
const { applyRetainForDays } = require("../retention/keepForNDays");

new Worker(
  "retention",
  async (job) => {
    const { connectionId } = job.data;
    await applyRetainForDays(connectionId);
  },
  {
    connection: { host: "localhost", port: 6379 },
  }
);

worker.on("failed", (job, err) => {
  console.error("RETENTION JOB FAILED", job?.id, err);
});

worker.on("error", (err) => {
  console.error("RETENTION WORKER ERROR", err);
});

