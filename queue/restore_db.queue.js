const { Queue } = require("bullmq");

const restoreDBQueue = new Queue("restore-db", {
  connection: { host: "localhost", port: 6379 },
});

async function enqueueRestoreDBJob({ restoreId }) {
  await restoreDBQueue.add("restore", { restoreId });
}


module.exports = { enqueueRestoreDBJob };