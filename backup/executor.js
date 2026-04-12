const { spawn } = require("child_process");
const { pipeline } = require("stream/promises");
const { PassThrough, Transform } = require("stream");
const zlib = require("zlib");
const crypto = require("crypto");

async function runBackup(command, createStorage, options = {}) {
  const { timeoutMs = 60 * 60 * 1000, db_type } = options;

  const maxAttempts = command.cmd === "mongodump" ? 2 : 1;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const storage = await createStorage();
    
    const proc = spawn(command.cmd, command.args, {
      env: { ...process.env, ...command.env },
      stdio: ["ignore", "pipe", "pipe"],
    });

    const label =
      db_type === "postgresql" ? "pg_dump" :
      db_type === "mysql" ? "mysqldump" :
      db_type === "mongodb" ? "mongodump" :
      "backup";

    proc.on("error", err => {
      throw new Error(`Failed to start ${label} : ${err.message}`);
    });

    let stderr = "";

    proc.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
      console.error(`${label} stderr:`, chunk.toString());
    });

    const hash = crypto.createHash("sha256");

    const hasher = new Transform({
      transform(chunk, enc, cb) {
        hash.update(chunk);
        cb(null, chunk);
      }
    });

    const compressor = !command.alreadyCompressed ? zlib.createGzip({ level: zlib.constants.Z_BEST_COMPRESSION }) : new PassThrough();

    //inject process exit error into stdout stream
    // proc.once("close", (code) => {
    //   if (code !== 0) {
    //     proc.stdout.destroy(
    //       new Error(`Backup process failed (${code}): ${stderr}`)
    //     );
    //   }
    // });

    const exitPromise = new Promise((resolve, reject) => {
      proc.on("close", (code) => {
        if (code === 0) {
          resolve();
        } else {
          reject(new Error(`Backup process failed (${code}): ${stderr}`));
        }
      });

      proc.on("error", reject);
    });

    //timeout
    const timeout = setTimeout(() => {
      proc.kill("SIGKILL");
    }, timeoutMs);

    try {
      await Promise.all([
        pipeline(proc.stdout, hasher, compressor, storage.stream),
        exitPromise
      ]);

      // storage.stream.end();

      if (storage.waitForUpload) {
        await storage.waitForUpload();
      }

      const bytesWritten = storage.getBytesWritten();
      if (!bytesWritten && command.cmd === "mongodump" && attempt < maxAttempts) {
        console.warn("Empty Mongo dump detected. Retrying once...");
        continue;
      }

      if (!bytesWritten) {
        if (stderr.includes("could not connect")) {
          throw new Error("DB_UNREACHABLE");
        }
        if (stderr.includes("password authentication failed")) {
          throw new Error("INVALID_CREDENTIALS");
        }
        throw new Error("EMPTY_BACKUP");
      }

      return {
        bytesWritten,
        checksumSha256: hash.digest("hex"),
        storagePath: storage.path
      };

    } finally {
      clearTimeout(timeout);
      if (!proc.killed) {
        try { proc.kill("SIGKILL"); } catch {}
      }
    }
  }
}

module.exports = { runBackup }