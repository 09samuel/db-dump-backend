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
  // if (engine === "postgresql") {
  //   await resetPostgresSchema({ host, port, username, password, database });
  // }

  // if (engine === "mysql") {
  //   await resetMySQLTables({ host, port, username, password, database });
  // }

  // if (engine === "mongodb") {
  //   await dropMongoDatabase({ host, port, username, password, database });
  // }

  // //REPLAY BACKUP
  // await runRestoreCommand({ engine, host, port, database, username, password, backupPath, checksumSha256, timeoutMs });


  console.log("RESTORE: starting", engine);

  if (engine === "postgresql") {
    console.log("RESTORE: resetting postgres schema");
    await resetPostgresSchema({ host, port, username, password, database });
    console.log("RESTORE: postgres reset done");
  }

  if (engine === "mysql") {
    console.log("RESTORE: resetting mysql");
    await resetMySQLTables({ host, port, username, password, database });
    console.log("RESTORE: mysql reset done");
  }

  if (engine === "mongodb") {
    console.log("RESTORE: dropping mongodb db");
    await dropMongoDatabase({ host, port, username, password, database });
    console.log("RESTORE: mongodb drop done");
  }

  console.log("RESTORE: starting restore execution");

  await runRestoreCommand({ engine, host, port, database, username, password, backupPath, checksumSha256, timeoutMs });

  console.log("RESTORE: restore completed");

}

module.exports = { restore };
