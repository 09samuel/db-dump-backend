const { spawn } = require("child_process");
const { pipeline } = require("stream/promises");
const { PassThrough, Transform } = require("stream");
const zlib = require("zlib");
const crypto = require("crypto");

async function runBackup(command, createStorage, options = {}) {
  const { timeoutMs = 60 * 60 * 1000 } = options;

  const maxAttempts = command.cmd === "mongodump" ? 2 : 1;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const storage = await createStorage();
    
    const proc = spawn(command.cmd, command.args, {
      env: { ...process.env, ...command.env },
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stderr = "";
    proc.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
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
    proc.once("close", (code) => {
      if (code !== 0) {
        proc.stdout.destroy(
          new Error(`Backup process failed (${code}): ${stderr}`)
        );
      }
    });

    //timeout
    const timeout = setTimeout(() => {
      proc.kill("SIGKILL");
    }, timeoutMs);

    try {
      await pipeline( proc.stdout, compressor, hasher, storage.stream );

      if (storage.waitForUpload) {
        await storage.waitForUpload();
      }

      const bytesWritten = storage.getBytesWritten();
      if (!bytesWritten && command.cmd === "mongodump" && attempt < maxAttempts) {
        console.warn("Empty Mongo dump detected. Retrying once...");
        continue;
      }

      if (!bytesWritten) {
        throw new Error("Backup produced zero bytes");
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