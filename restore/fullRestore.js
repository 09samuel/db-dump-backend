const { spawn } = require("child_process");

//POSTGRES
async function resetPostgresSchema({ host, port, username, password, database }) {
  return new Promise((resolve, reject) => {
    const sql = `
      DROP SCHEMA public CASCADE;
      CREATE SCHEMA public;
    `;

    const args = [
      "-h", host,
      "-p", String(port),
      "-U", username,
      "-d", database,
      "-c", sql
    ];

    const child = spawn("psql", args, {
      env: { ...process.env, PGPASSWORD: password }
    });

    let stderr = "";
    child.stderr.on("data", d => stderr += d.toString());

    child.on("close", code => {
      if (code !== 0) return reject(new Error(stderr));
      resolve();
    });
  });
}


//MYSQL
async function resetMySQLTables({ host, port, username, password, database }) {
  return new Promise((resolve, reject) => {
    const sql = `
      SET FOREIGN_KEY_CHECKS = 0;
      SET @tables = (
        SELECT GROUP_CONCAT(table_name)
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
      );
      SET @stmt = CONCAT('DROP TABLE IF EXISTS ', @tables);
      PREPARE s FROM @stmt;
      EXECUTE s;
      DEALLOCATE PREPARE s;
      SET FOREIGN_KEY_CHECKS = 1;
    `;

    const args = [
      "-h", host,
      "-P", String(port),
      "-u", username,
      database,
      "-e", sql
    ];

    const child = spawn("mysql", args, {
      env: { ...process.env, MYSQL_PWD: password }
    });

    let stderr = "";
    child.stderr.on("data", d => stderr += d.toString());

    child.on("close", code => {
      if (code !== 0) return reject(new Error(stderr));
      resolve();
    });
  });
}


//MONGODB
function dropMongoDatabase({
  host,
  port,
  username,
  password,
  database
}) {
  return new Promise((resolve, reject) => {
    const js = `db.getSiblingDB("${database}").dropDatabase()`;

    const args = [
      "--host", host,
      "--port", String(port),
      "--username", username,
      "--password", password,
      "--authenticationDatabase", "admin",
      "--eval", js
    ];

    const child = spawn("mongo", args);

    let stderr = "";
    child.stderr.on("data", d => stderr += d.toString());

    child.on("close", code => {
      if (code !== 0) return reject(new Error(stderr));
      resolve();
    });
  });
}

module.exports = {
  resetPostgresSchema,
  resetMySQLTables,
  dropMongoDatabase
};
