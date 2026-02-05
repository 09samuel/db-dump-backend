const { spawn } = require("child_process");
const zlib = require("zlib");
const crypto = require("crypto");

function runBackup(command, storage, options = {}) {
  const {
    timeoutMs = 60 * 60 * 1000, // 1 hour default
    maxStderrBytes = 64 * 1024, // 64 KB
  } = options;

  return new Promise((resolve, reject) => {
    let settled = false;

    const fail = (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (proc && !proc.killed) {
        try { proc.kill("SIGKILL"); } catch {}
      }
      reject(err);
    };

    const proc = spawn(command.cmd, command.args, {
      env: { ...process.env, ...command.env },
      stdio: ["ignore", "pipe", "pipe"],
    });

    //timeout protection
    const timer = setTimeout(() => {
      fail(new Error("Backup process timed out"));
    }, timeoutMs);

    //compress and pipe db dump to storage
    const gzip = zlib.createGzip({
      level: zlib.constants.Z_BEST_COMPRESSION,
    });

    //compression failure
    gzip.on("error", (err) => {
      storage.stream.destroy(err);
    });

    // checksum (compressed bytes)
    const hash = crypto.createHash("sha256");

    gzip.on("data", (chunk) => {
      hash.update(chunk);
    });

    proc.stdout.pipe(gzip).pipe(storage.stream);

    //capture stderr (bounded)
    let stderrBytes = 0;
    proc.stderr.on("data", (chunk) => {
      if (stderrBytes < maxStderrBytes) {
        stderr += chunk.toString();
        stderrBytes += chunk.length;
      }
    });

    //storage failure
    storage.stream.on("error", (err) => {
      clearTimeout(timer);
      fail(err);
    });

    //process error
    proc.on("error", (err) => {
      clearTimeout(timer);
      fail(err);
    });

    proc.stdout.on("error", (err) => {
      clearTimeout(timer);
      fail(err);
    });

    //process exit
    proc.on("close", async (code) => {
      clearTimeout(timer);

      if (settled) return;

      if (code !== 0) {
        return fail(
          new Error(stderr || `Backup process exited with code ${code}`)
        );
      }
      try {
        //signal end of data
        storage.stream.end()

        //wait for s3 upload to finish
        if (storage.waitForUpload) {
          const uploadTimeout = setTimeout(() => {
            fail(new Error("Upload finalization timed out"));
          }, timeoutMs);

          try {
            await storage.waitForUpload();
          } finally {
            clearTimeout(uploadTimeout);
          }
        }
        
        const checksumSha256 = hash.digest("hex");

        settled = true;
        resolve(storage.getBytesWritten(), checksumSha256);
      } catch (err) {
        fail(err)
      }
    });
  });
}

module.exports = { runBackup };


