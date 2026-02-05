const { spawn } = require("child_process");
const fs = require("fs");
const { buildRestoreCommand } = require("../restore/strategy");
const zlib = require("zlib");

function runRestoreCommand({ engine, host, port, database, username, password, backupPath, timeoutMs = 30 * 60 * 1000 }) {
  return new Promise((resolve, reject) => {
    const { command, args, env, stdinFile } = buildRestoreCommand({ engine, host, port, database, username, password, backupPath });

    const child = spawn(command, args, {
      env,
      stdio: ["pipe", "pipe", "pipe"]
    });

    if (stdinFile) {
      const input = fs.createReadStream(stdinFile);

      if (stdinFile.endsWith(".gz")) {
        const gunzip = zlib.createGunzip();

        gunzip.on("error", (err) => {
          child.kill("SIGKILL");
          reject(err);
        });

        input.pipe(gunzip).pipe(child.stdin);
      } else {
        input.pipe(child.stdin);
      }
    } else {
      child.stdin.end();
    }


    let stderr = "";
    let timedOut = false;

    const timeout = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, timeoutMs);

    child.stderr.on("data", d => {
      stderr += d.toString();
    });

    child.on("error", err => {
      clearTimeout(timeout);
      reject(err);
    });

    child.on("close", code => {
      clearTimeout(timeout);

      if (timedOut) {
        return reject(new Error("Restore exceeded maximum runtime"));
      }

      if (code !== 0) {
        return reject(
          new Error(
            sanitizeError(`Restore failed with exit code ${code}: ${stderr}`)
          )
        );
      }

      resolve();
    });
  });
}

function sanitizeError(msg) {
  return msg
    .replace(/PGPASSWORD=\S+/g, "PGPASSWORD=****")
    .replace(/MYSQL_PWD=\S+/g, "MYSQL_PWD=****");
}


module.exports = { runRestoreCommand }