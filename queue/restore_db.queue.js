require("dotenv").config();
const { Queue } = require("bullmq");

const redisConnection = {
  host: process.env.REDIS_HOST,
  port: Number(process.env.REDIS_PORT || 6379),
};

const restoreDBQueue = new Queue("restore-db", {
  connection: redisConnection,
});

async function enqueueRestoreDBJob({ restoreId }) {
  await restoreDBQueue.add("restore", { restoreId });
}


module.exports = { enqueueRestoreDBJob };