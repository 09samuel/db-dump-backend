require("dotenv").config();
const { Queue } = require("bullmq");
const redisConnection = require("../config/redis");

const restoreDBQueue = new Queue("restore-db", {
  connection: redisConnection,
});

async function enqueueRestoreDBJob({ restoreId }) {
  await restoreDBQueue.add("restore", { restoreId });
}


module.exports = { enqueueRestoreDBJob };