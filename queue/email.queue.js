const { Queue } = require("bullmq");

const emailQueue = new Queue("emailQueue", {
  connection: { host: "localhost", port: 6379 },
});

async function enqueueEmailJob({ type, email, token }) {
  await emailQueue.add(
    "sendEmail",
    { type, email, token },
    {
        attempts: 3,
        backoff: {
            type: "exponential",
            delay: 5000,
        },
        removeOnComplete: true,
        removeOnFail: false,
    }
    );
}


module.exports = { enqueueEmailJob };