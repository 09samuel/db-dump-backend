require("dotenv").config();
const { Queue } = require("bullmq");
const redisConnection = require("../config/redis");

const emailQueue = new Queue("emailQueue", {
  connection: redisConnection,
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