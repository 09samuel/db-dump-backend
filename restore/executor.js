const { spawn } = require("child_process");
const logger = require("../utils/logger");
const { pipeline } = require("stream/promises");
const { PassThrough, Transform } = require("stream");
const fs = require("fs");
const zlib = require("zlib");
const crypto = require("crypto");
const { buildRestoreCommand } = require("./strategy");
const { execSync } = require("child_process");
const path = require("path");

async function runRestoreCommand({ engine, host, port, database, username, password, backupPath, checksumSha256, timeoutMs = 30 * 60 * 1000, sslMode, targetSchema }) {

  const { command, args, env, stdinFile } =  buildRestoreCommand({ engine, host, port, database, username, password, backupPath, sslMode, targetSchema });

  logger.info(`RESTORE CMD: ${command}`);
  logger.info(`RESTORE ARGS: ${JSON.stringify(args)}`);

  const proc = spawn(command, args, {
    env,
    stdio: ["pipe", "pipe", "pipe"]
  });

  const label =
    engine === "postgresql" ? "pg_restore" :
    engine === "mysql" ? "mysql" :
    engine === "mongodb" ? "mongorestore" :
    "restore";

  proc.on("error", err => {
    logger.error(`${label} spawn error:`, err);
  });


  let stderr = "";
  let stdout = "";

  proc.stderr.on("data", chunk => {
    stderr += chunk.toString();
    logger.error(`${label} stderr: ${chunk.toString()}`);
  });

  proc.stdout.on("data", chunk => {
    stdout += chunk.toString();
  });


  //let stderr = "";
  // proc.stderr.on("data", chunk => {
  //   stderr += chunk.toString();
  // });

  const timeout = setTimeout(() => {
    proc.kill("SIGKILL");
  }, timeoutMs);

  try {

    //if restore uses direct file argument (like pg_restore)
    if (!stdinFile) {
      await waitForProcess(proc, () => stderr, () => stdout, label, engine);
      return;
    }

    if (!fs.existsSync(stdinFile)) {
      throw new Error("Backup file not found");
    }

    const stats = fs.statSync(stdinFile);
    logger.info(`File size: ${stats.size}`);

    if (stats.size === 0) {
      throw new Error("Backup file is empty");
    }


    //streamed restore
    const fileStream = fs.createReadStream(stdinFile);

    const hasher = checksumSha256
      ? new Transform({
          transform(chunk, enc, cb) {
            this.hash.update(chunk);
            cb(null, chunk);
          }
        })
      : new PassThrough();

    if (checksumSha256) {
      hasher.hash = crypto.createHash("sha256");
    }

    const isGzip = stdinFile.endsWith(".gz") || stdinFile.endsWith(".archive.gz");
    const decompressor = isGzip ? zlib.createGunzip() : new PassThrough();


    // inject process exit failure into stdin stream
    // proc.once("close", code => {
    //   //console.log(`Restore process exited with code ${code}`);
    //   if (code !== 0) {
    //     proc.stdin.destroy(
    //       new Error(`Restore process failed (${code}): ${stderr}`)
    //     );
    //   }
    // });

    await Promise.all([
      pipeline(fileStream, decompressor, hasher, proc.stdin),
      waitForProcess(proc, () => stderr, () => stdout, label, engine)
    ]);

    proc.stdin.end();

   if (checksumSha256) {
      const actual = hasher.hash.digest("hex"); 

      // // MySQL empty dump check
      // if (engine === "mysql") {
      //   if (actual === "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855") {
      //     throw new Error("MySQL restore failed: no data streamed");
      //   }
      // }

      // Checksum validation
      if (actual !== checksumSha256) {
        throw new Error(
          `Backup checksum mismatch (expected ${checksumSha256}, got ${actual})`
        );
      }
    }

  } finally {
    clearTimeout(timeout);
    if (!proc.killed) {
      try { proc.kill("SIGKILL"); } catch {}
    }
  }
}

// function waitForProcess(proc, stderr, stdout, label) {
//   return new Promise((resolve, reject) => {
//     proc.once("close", code => {
//       // if (code !== 0) {
//       //   const message = stderr?.trim() || stdout?.trim() || `Restore failed with exit code ${code}`;

//       //   return reject(new Error(sanitizeError(message)));
//       // }
//       // if (code === 0) {
//       //   return resolve();
//       // }

//       // if (code === 1) {
//       //   const normalized = stderr.toLowerCase();

//       //   const harmless = [
//       //     "unrecognized configuration parameter",
//       //     "errors ignored on restore"
//       //   ];

//       //   const isHarmless = harmless.some(msg =>
//       //     normalized.includes(msg)
//       //   );

//       //   if (isHarmless) {
//       //     console.warn("pg_restore completed with warnings (ignored)");
//       //     return resolve();
//       //   }
//       // }

//       if (code === 0 || code === 1) {
//         console.warn(`${label} completed (warnings possible)`);
//         return resolve();
//       }

//       return reject(new Error(sanitizeError(stderr || stdout)));
//     });
//   });
// }

function waitForProcess(proc, getStderr, getStdout, label, engine) {
  return new Promise((resolve, reject) => {
    proc.once("close", code => {
      const stderr = getStderr();
      const stdout = getStdout();
      
      // Hard failure (process-level)
      if (code !== 0 && code !== 1) {
        return reject(new Error(sanitizeError(stderr || stdout)));
      }

      // Mongo-specific validation
      if (engine === "mongodb") {

        // permission error
        if (stderr.includes("not allowed to do action")) {
          return reject(new Error("Mongo restore failed: insufficient permissions"));
        }

        // parse summary
        const match = stderr.match(
          /(\d+)\s+document\(s\)\s+restored successfully\.\s+(\d+)\s+document\(s\)\s+failed to restore/
        );

        if (!match) {
          return reject(new Error("Mongo restore failed: no summary found"));
        }

        const restored = parseInt(match[1], 10);
        const failed = parseInt(match[2], 10);

        if (failed > 0) {
          return reject(new Error(`Mongo restore failed: ${failed} documents failed`));
        }

        if (restored === 0) {
          return reject(new Error("Mongo restore failed: no documents restored"));
        }

        logger.info(`Mongo restore success: ${restored} docs restored`);
        return resolve();
      }

      //Postgres/MySQL
      logger.warn(`${label} completed (warnings possible)`);
      resolve();
    });
  });
}

function sanitizeError(msg) {
  return msg
    .replace(/PGPASSWORD=\S+/g, "PGPASSWORD=****")
    .replace(/MYSQL_PWD=\S+/g, "MYSQL_PWD=****");
}


// async function runSchemaRestore({
//   host,
//   port,
//   database,
//   username,
//   password,
//   backupPath,
//   targetSchema,
//   sslMode
// }) {
//   return new Promise((resolve, reject) => {

//     // ✅ 1. pg_restore process
//     const restore = spawn("pg_restore", [
//       "--no-owner",
//       "--no-privileges",
//       "--schema=public",
//       backupPath
//     ]);

//     // ✅ 2. Transform (PUT IT HERE)
//     const rewrite = new Transform({
//       transform(chunk, enc, cb) {
//         const updated = chunk
//           .toString()
//           .replace(/public\./g, `${targetSchema}.`);
//         cb(null, updated);
//       }
//     });

//     // ✅ 3. psql process
//     const psql = spawn("psql", [
//       "-h", host,
//       "-p", String(port),
//       "-U", username,
//       "-d", database
//     ], {
//       env: {
//         ...process.env,
//         PGPASSWORD: password,
//         PGSSLMODE: sslMode || "require"
//       }
//     });

//     // ✅ 4. PIPELINE (critical)
//     restore.stdout
//       .pipe(rewrite)
//       .pipe(psql.stdin);

//     let stderr = "";

//     restore.stderr.on("data", d => stderr += d.toString());
//     psql.stderr.on("data", d => stderr += d.toString());

//     psql.on("close", code => {
//       if (code !== 0) {
//         return reject(new Error(stderr));
//       }
//       resolve();
//     });
//   });
// }


async function runSchemaRestore({ host, port, database, username, password, backupPath, targetSchema, sslMode}) {
  return new Promise((resolve, reject) => {

    logger.info("Starting schema restore");
    logger.info(`Backup: ${backupPath}`);
    logger.info(`Target schema: ${targetSchema}`);

    // 1. pg_restore
    const restore = spawn(
      process.env.PG_RESTORE_PATH || "pg_restore",
      [
        "--no-owner",
        "--no-privileges",
        "--schema=public",
        "-f", "-",
        backupPath
      ]
    );

    logger.info("pg_restore started");

    // 2. Transform (rewrite schema)
    let chunkCount = 0;

    const rewrite = new Transform({
      transform(chunk, enc, cb) {
        chunkCount++;

        let str = chunk.toString();

        str = str
          // remove psql meta commands
          .replace(/^\\restrict.*$/gm, "")
          .replace(/^\\unrestrict.*$/gm, "")

          // remove unsupported SET configs
          .replace(/^SET transaction_timeout = .*;$/gm, "")

          // schema rewrite
          .replace(/"public"\./g, `"${targetSchema}".`)
          .replace(/\bpublic\./g, `${targetSchema}.`);

        cb(null, str);
      }
    });

    // 3. psql
    const psql = spawn("psql", [
      "-h", host,
      "-p", String(port),
      "-U", username,
      "-d", database
    ], {
      env: {
        ...process.env,
        PGPASSWORD: password,
        PGSSLMODE: sslMode || "require"
      }
    });

    logger.info("🗄️ psql started");

    // 4. PIPELINE
    restore.stdout
      .pipe(rewrite)
      .pipe(psql.stdin);

    let stderr = "";
    let restoreLogs = "";

    // pg_restore logs
    restore.stderr.on("data", d => {
      const msg = d.toString();
      restoreLogs += msg;
      logger.info(`pg_restore: ${msg.trim()}`);
    });

    // psql logs
    psql.stderr.on("data", d => {
      const msg = d.toString();
      stderr += msg;
      logger.error(`psql: ${msg.trim()}`);
    });

    // process exit logs
    let restoreExitCode = null;

    restore.on("close", code => {
      restoreExitCode = code;
      logger.info(`pg_restore exited with code ${code}`);
    });

    psql.on("close", code => {
      logger.info(`psql exited with code ${code}`);
      logger.info(`Chunks processed: ${chunkCount}`);

      if (restoreExitCode !== 0) {
        return reject(new Error("pg_restore failed"));
      }

      if (code !== 0) {
        return reject(new Error(stderr || "psql restore failed"));
      }

      if (chunkCount === 0) {
        return reject(new Error("No data restored (empty stream)"));
      }

      logger.info("Restore completed successfully");
      resolve();
    });

    // error handlers
    restore.on("error", err => {
      logger.error("pg_restore error:", err);
      reject(err);
    });

    psql.on("error", err => {
      logger.error("psql error:", err);
      reject(err);
    });

  });
}
module.exports = { runRestoreCommand, runSchemaRestore };