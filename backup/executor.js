const { spawn } = require("child_process");

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
      try {
        proc.kill("SIGKILL");
      } catch {}
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

    //pipe stdout to storage
    proc.stdout.pipe(storage.stream);

    //capture stderr (bounded)
    let stderr = "";
    proc.stderr.on("data", (chunk) => {
      if (stderr.length < maxStderrBytes) {
        stderr += chunk.toString();
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

    //process exit
    proc.on("close", (code) => {
      clearTimeout(timer);

      if (settled) return;

      if (code !== 0) {
        return fail(
          new Error(stderr || `Backup process exited with code ${code}`)
        );
      }

      settled = true;
      resolve(storage.getBytesWritten());
    });
  });
}

module.exports = { runBackup };
