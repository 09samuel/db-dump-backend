const { spawn } = require("child_process");

function runBackup(command, storage) {
  return new Promise((resolve, reject) => {
    const proc = spawn(command.cmd, command.args, {
      env: { ...process.env, ...command.env },
      stdio: ["ignore", "pipe", "pipe"],
    });

    proc.stdout.pipe(storage.stream);

    let stderr = "";

    proc.stderr.on("data", (d) => {
      stderr += d.toString();
    });

    storage.stream.on("error", (err) => {
      proc.kill("SIGKILL");
      reject(err);
    });

    proc.on("close", (code) => {
      if (code !== 0) {
        return reject(new Error(stderr || "Backup process failed"));
      }
      resolve(storage.getBytesWritten());
    });

    proc.on("error", reject);
  });
}

module.exports = { runBackup };
