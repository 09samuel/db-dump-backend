const { spawn } = require("child_process");

const PSQL_CMD = process.env.PG_PATH || "psql";
const MYSQL_CMD = process.env.MYSQL_RESTORE_PATH || "mysql";

function spawnPromise(command, args, env) {
    return new Promise((resolve, reject) => {
        const child = spawn(command, args, { env });
        let stderr = "";

        child.stderr.on("data", d => stderr += d.toString());

        child.on("error", err => {
          return reject(new Error(`Spawn failed: ${err.message}`));
        });

        child.on("close", code => {
        if (code !== 0) return reject(new Error(stderr));
        resolve();
        });
    });
}

async function checkCreatePrivilege({ engine, host, port, username, password, sslMode }) {
    if (engine === "postgresql") {

      const args = [
        "-h", host,
        "-p", String(port),
        "-U", username,
        "-d", "postgres",
        "-t",
        "-c", "SELECT rolcreatedb FROM pg_roles WHERE rolname = current_user;"
      ];

      let output = "";

      return new Promise((resolve, reject) => {
        const child = spawn(PSQL_CMD, args, {
          env: { ...process.env, PGPASSWORD: password, PGSSLMODE: sslMode || "prefer" }
        });

        child.stdout.on("data", d => output += d.toString());
        
        let stderr = "";

        child.stderr.on("data", d => stderr += d.toString());

        child.on("close", code => {
          if (code !== 0) return reject(new Error(stderr || "Privilege check failed"));
          resolve(output.trim() === "t");
        });

        child.on("close", code => {
          if (code !== 0) return reject(new Error("Privilege check failed"));
            resolve(output.trim() === "t");
          });
        });
      }

    

    if (engine === "mysql") {
      const args = [
        "-h", host,
        "-P", String(port),
        "-u", username,
        "-e", "SHOW GRANTS FOR CURRENT_USER();"
      ];

      let output = "";

      return new Promise((resolve, reject) => {
        const child = spawn(MYSQL_CMD, args, {
          env: { ...process.env, MYSQL_PWD: password }
        });

        child.stdout.on("data", d => output += d.toString());
        child.stderr.on("data", d => reject(new Error(d.toString())));

        child.on("close", () => {
          resolve(
            output.includes("ALL PRIVILEGES") ||
            output.includes("GRANT CREATE ON *.*")
          );
        });
      });
    }

    if (engine === "mongodb") {
        // return new Promise((resolve, reject) => {
        //     const args = [
        //     "--host", host,
        //     "--port", String(port),
        //     "--username", username,
        //     "--password", password,
        //     "--authenticationDatabase", "admin",
        //     "--eval", "db.runCommand({ connectionStatus: 1 })"
        //     ];

        //     const child = spawn(process.env.MONGO_SHELL_PATH || "mongosh", args);

        //     let output = "";

        //     child.stdout.on("data", d => output += d.toString());
        //     child.stderr.on("data", d => reject(new Error(d.toString())));

        //     child.on("close", () => {
        //     const hasAdmin =  output.includes('"role" : "root"') || output.includes('"role" : "dbAdmin"') || output.includes('"role" : "restore"');

        //     resolve(hasAdmin);
        //     });
        // });
        return true
    }


  return false;
}


async function createDatabase({ engine, host, port, username, password, database, sslMode }) {

  if (engine === "postgresql") {
    return spawnPromise(
      PSQL_CMD,
      [
        "-h", host,
        "-p", String(port),
        "-U", username,
        "-d", "postgres",
        "-c", `CREATE DATABASE ${database};`
      ],
      { ...process.env, PGPASSWORD: password, PGSSLMODE: sslMode || "prefer" }
    );
  }

  if (engine === "mysql") {
    return spawnPromise(
      MYSQL_CMD,
      [
        "-h", host,
        "-P", String(port),
        "-u", username,
        "-e", `CREATE DATABASE ${database};`
      ],
      { ...process.env, MYSQL_PWD: password }
    );
  }

  if (engine === "mongodb") {
    // Mongo DB auto-creates on restore
    return;
  }
}

//for managed pg (supabase)
async function createSchema({ host, port, username, password, database, schema, sslMode }) {
  return spawnPromise(
    PSQL_CMD,
    [
      "-h", host,
      "-p", String(port),
      "-U", username,
      "-d", database,
      "-c", `CREATE SCHEMA IF NOT EXISTS ${schema};`
    ],
    {
      ...process.env,
      PGPASSWORD: password,
      PGSSLMODE: sslMode || "require"
    }
  );
}


async function dropDatabase({ engine, host, port, username, password, database, sslMode }) {

  if (engine === "postgresql") {
    return spawnPromise(
      PSQL_CMD,
      [
        "-h", host,
        "-p", String(port),
        "-U", username,
        "-d", "postgres",
        "-c", `DROP DATABASE IF EXISTS ${database};`
      ],
      { ...process.env, PGPASSWORD: password, PGSSLMODE: sslMode || "prefer" }
    );
  }

  if (engine === "mysql") {
    return spawnPromise(
      MYSQL_CMD,
      [
        "-h", host,
        "-P", String(port),
        "-u", username,
        "-e", `DROP DATABASE IF EXISTS ${database};`
      ],
      { ...process.env, MYSQL_PWD: password }
    );
  }

  if (engine === "mongodb") {
    // return spawnPromise(
    //   "mongo",
    //   [
    //     "--host", host,
    //     "--port", String(port),
    //     "--username", username,
    //     "--password", password,
    //     "--authenticationDatabase", "admin",
    //     "--eval", `db.getSiblingDB("${database}").dropDatabase()`
    //   ]
    // );
    return
  }
}

module.exports = { checkCreatePrivilege, createDatabase, dropDatabase, createSchema };