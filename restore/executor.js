const { spawn } = require("child_process");
const fs = require("fs");
const { buildRestoreCommand } = require("../restore/strategy");
const zlib = require("zlib");

function runRestoreCommand({ engine, host, port, database, username, password, backupPath, checksumSha256, timeoutMs = 30 * 60 * 1000 }) {
  return new Promise((resolve, reject) => {
    const { command, args, env, stdinFile } = buildRestoreCommand({ engine, host, port, database, username, password, backupPath });

    const child = spawn(command, args, {
      env,
      stdio: ["pipe", "pipe", "pipe"]
    });

    // if (stdinFile) {
    //   const input = fs.createReadStream(stdinFile);

    //   if (stdinFile.endsWith(".gz")) {
    //     const gunzip = zlib.createGunzip();

    //     gunzip.on("error", (err) => {
    //       child.kill("SIGKILL");
    //       reject(err);
    //     });

    //     input.pipe(gunzip).pipe(child.stdin);
    //   } else {
    //     input.pipe(child.stdin);
    //   }
    // } else {
    //   child.stdin.end();
    // }


    let stderr = "";
    let timedOut = false;

    const timeout = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, timeoutMs);

    //calculate hash on stored file
    const hash = checksumSha256 ? crypto.createHash("sha256") : null;

    const input = stdinFile ? fs.createReadStream(stdinFile) : null;

    if (input && hash) {
      input.on("data", chunk => {
        hash.update(chunk);
      });

      input.on("error", err => {
        child.kill("SIGKILL");
        reject(err);
      });
    }

    if (input) {
      if (stdinFile.endsWith(".gz")) {
        const gunzip = zlib.createGunzip();

        gunzip.on("error", err => {
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

      //verification of hashes
      if (hash) {
        const actual = hash.digest("hex");

        if (actual !== checksumSha256) {
          return reject(
            new Error(
              `Backup checksum mismatch (expected ${checksumSha256}, got ${actual})`
            )
          );
        }
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