require("dotenv").config();
const { Worker } = require("bullmq");
const { handleVerificationJob } = require("../handlers/handleVerificationJob");

console.log("Verification worker started");

new Worker(
  "verify-connection",
  async (job) => {
    await handleVerificationJob(job);
  },
  {
    connection: { host: "localhost", port: 6379 },
  }
);
