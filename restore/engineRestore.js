// const { runRestoreCommand, runSchemaRestore } = require("./executor");
// const { checkCreatePrivilege, createDatabase, createSchema, dropDatabase } = require("./dbAdmin");

// const SUPPORTED_ENGINES = ["postgresql", "mysql", "mongodb"];

// async function restore({ engine, host, port, database, username, password, backupPath, checksumSha256, timeoutMinutes = 60, sslMode, restoreMode }) {
//   if (!SUPPORTED_ENGINES.includes(engine)) {
//     throw new Error(`Unsupported database engine: ${engine}`);
//   }

//   const timeoutMs = timeoutMinutes * 60 * 1000;
  
//   if (restoreMode === "database") {
    
//     //check if user has permission to create new db
//     const canCreate = await checkCreatePrivilege({ engine, host, port, username, password, sslMode });

//     if (!canCreate) {
//       throw new Error("User does not have privilege to create new database");
//     }

//     //generate temp DB name
//     const newDatabase = `restored_${Date.now()}`;

//     try {
//       //create db
//       await createDatabase({ engine, host, port, username, password, database: newDatabase, sslMode });

//       let restoreDatabaseArg = newDatabase;

//       if (engine === "mongodb") {
//         restoreDatabaseArg = {
//           original: database,
//           target: newDatabase
//         };
//       }

//       //restore into new db
//       await runRestoreCommand({ engine, host, port, database: restoreDatabaseArg, username, password, backupPath, checksumSha256, timeoutMs, sslMode });

//       return { restoredDatabase: newDatabase };

//     } catch (err) {
//       //cleanup on failure
//       try {
//         await dropDatabase({ engine, host, port, username, password, database: newDatabase, sslMode });
//       } catch (cleanupErr) {
//         console.warn("Failed to cleanup after restore failure:", cleanupErr.message);
//       }
//       throw err;
//     }
//   }


// if (restoreMode === "schema" && engine === "postgresql") {
//   const newSchema = `restored_${Date.now()}`;

//   console.log(`Restoring into new schema ${newSchema}`);

//   await createSchema({
//     host,
//     port,
//     username,
//     password,
//     database,
//     schema: newSchema,
//     sslMode
//   });

//   await runSchemaRestore({
//     host,
//     port,
//     database,
//     username,
//     password,
//     backupPath,
//     targetSchema: newSchema,
//     sslMode
//   });

//   return { restoredSchema: newSchema };
// }
// }

// module.exports = { restore };


const { runRestoreCommand, runSchemaRestore } = require("./executor");
const {
  checkCreatePrivilege,
  createDatabase,
  createSchema,
  dropDatabase
} = require("./dbAdmin");

const { spawn } = require("child_process");

const SUPPORTED_ENGINES = ["postgresql", "mysql", "mongodb"];

// 🔥 helper: clear MySQL DB (overwrite mode)
async function clearDatabase({ host, port, database, username, password }) {
  const query = `
    SET FOREIGN_KEY_CHECKS = 0;
    SELECT CONCAT('DROP TABLE IF EXISTS \\\`', table_name, '\\\`;')
    FROM information_schema.tables
    WHERE table_schema = '${database}';
    SET FOREIGN_KEY_CHECKS = 1;
  `;

  return new Promise((resolve, reject) => {
    const child = spawn(process.env.MYSQL_RESTORE_PATH || "mysql", [
      "-h", host,
      "-P", String(port),
      "-u", username,
      "-N",
      "-e", query
    ], {
      env: { ...process.env, MYSQL_PWD: password }
    });

    let output = "";

    child.stdout.on("data", d => output += d.toString());
    child.stderr.on("data", d => reject(new Error(d.toString())));

    child.on("close", () => {
      const drop = spawn(process.env.MYSQL_RESTORE_PATH || "mysql", [
        "-h", host,
        "-P", String(port),
        "-u", username,
        database
      ], {
        env: { ...process.env, MYSQL_PWD: password }
      });

      drop.stdin.write(output);
      drop.stdin.end();

      drop.on("close", () => resolve());
    });
  });
}

async function restore({
  engine,
  host,
  port,
  database,
  username,
  password,
  backupPath,
  checksumSha256,
  timeoutMinutes = 60,
  sslMode,
  restoreMode
}) {
  if (!SUPPORTED_ENGINES.includes(engine)) {
    throw new Error(`Unsupported database engine: ${engine}`);
  }

  const timeoutMs = timeoutMinutes * 60 * 1000;

  if (restoreMode === "database") {

    let targetDatabase = database;

    // 🔥 MySQL (RDS-safe overwrite mode)
    if (engine === "mysql") {

  if (isRDS(host)) {
    // 🔥 RDS → overwrite
    console.log("MySQL RDS detected → overwrite mode");

    await clearDatabase({
      host,
      port,
      database,
      username,
      password
    });

    targetDatabase = database;

  } else {
    // ✅ Local MySQL → create new DB
    console.log("Local MySQL detected → create new DB");

    const canCreate = await checkCreatePrivilege({
      engine,
      host,
      port,
      username,
      password,
      sslMode
    });

    if (!canCreate) {
      throw new Error("User does not have privilege to create new database");
    }

    const newDatabase = `restored_${Date.now()}`;
    targetDatabase = newDatabase;

    await createDatabase({
      engine,
      host,
      port,
      username,
      password,
      database: newDatabase,
      sslMode
    });
  }
} else {
      // ✅ PostgreSQL + MongoDB → create new DB

      const canCreate = await checkCreatePrivilege({
        engine,
        host,
        port,
        username,
        password,
        sslMode
      });

      if (!canCreate) {
        throw new Error("User does not have privilege to create new database");
      }

      const newDatabase = `restored_${Date.now()}`;
      targetDatabase = newDatabase;

      try {
        await createDatabase({
          engine,
          host,
          port,
          username,
          password,
          database: newDatabase,
          sslMode
        });
      } catch (err) {
        throw err;
      }
    }

    // Mongo special mapping
    let restoreDatabaseArg = targetDatabase;

    if (engine === "mongodb") {
      restoreDatabaseArg = {
        original: database,
        target: targetDatabase
      };
    }

    try {
      await runRestoreCommand({
        engine,
        host,
        port,
        database: restoreDatabaseArg,
        username,
        password,
        backupPath,
        checksumSha256,
        timeoutMs,
        sslMode
      });

      return { restoredDatabase: targetDatabase };

    } catch (err) {
      // cleanup only for postgres/mongo
      if (engine !== "mysql") {
        try {
          await dropDatabase({
            engine,
            host,
            port,
            username,
            password,
            database: targetDatabase,
            sslMode
          });
        } catch (cleanupErr) {
          console.warn("Cleanup failed:", cleanupErr.message);
        }
      }

      throw err;
    }
  }

  // ✅ PostgreSQL schema restore unchanged
  if (restoreMode === "schema" && engine === "postgresql") {
    const newSchema = `restored_${Date.now()}`;

    console.log(`Restoring into new schema ${newSchema}`);

    await createSchema({
      host,
      port,
      username,
      password,
      database,
      schema: newSchema,
      sslMode
    });

    await runSchemaRestore({
      host,
      port,
      database,
      username,
      password,
      backupPath,
      targetSchema: newSchema,
      sslMode
    });

    return { restoredSchema: newSchema };
  }
}

function isRDS(host) {
  return host.includes("rds.amazonaws.com");
}

module.exports = { restore };