const { Worker } = require("bullmq");
const {handleEmailJob} = require("../handlers/handleEmailJob");

console.log("Email worker started");

const worker = new Worker(
  "emailQueue",

  async (job) => {
    console.log("JOB RECEIVED BY WORKER", job.id, job.name);
    await handleEmailJob(job);
  },
  {
    connection: { host: "localhost", port: 6379 },
  }
);

worker.on("completed", (job) => {
  console.log("JOB COMPLETED", job.id);
});

worker.on("failed", (job, err) => {
  console.error("JOB FAILED", job?.id, err);
});

worker.on("error", (err) => {
  console.error("WORKER ERROR", err);
});


