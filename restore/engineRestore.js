const { resetPostgresSchema, resetMySQLTables,  dropMongoDatabase } = require("./fullRestore");
const { runRestoreCommand } = require("./executor");

const SUPPORTED_ENGINES = ["postgresql", "mysql", "mongodb"];

async function restore({ engine, host, port, database, username, password, backupPath, checksumSha256, timeoutMinutes = 60 }) {
  if (!SUPPORTED_ENGINES.includes(engine)) {
    throw new Error(`Unsupported database engine: ${engine}`);
  }

  if (!backupPath) {
    throw new Error("Backup path is required for restore");
  }

  if (!password) {
    throw new Error("Database password is required for restore");
  }

  const timeoutMs = timeoutMinutes * 60 * 1000;

  //WIPE TARGET DATABASE (FULL RESTORE)
  if (engine === "postgresql") {
    await resetPostgresSchema({ host, port, username, password, database });
  }

  if (engine === "mysql") {
    await resetMySQLTables({ host, port, username, password, database });
  }

  if (engine === "mongodb") {
    await dropMongoDatabase({ host, port, username, password, database });
  }

  //REPLAY BACKUP
  await runRestoreCommand({ engine, host, port, database, username, password, backupPath, checksumSha256, timeoutMs });
}

module.exports = { restore };
