const logger = require("../utils/logger");
const localHosts = (process.env.LOCALHOST_HOSTS || "")
  .split(",")
  .map((host) => host.trim())
  .filter(Boolean);

function buildRestoreCommand({ engine, host, port, database, username, password, backupPath, sslMode, targetSchema }) {

  if (engine === "postgresql") {
    const args = [
      "-h", host,
      "-p", String(port),
      "-U", username,
      "-d", database,
      "--no-owner",
      "--no-privileges",
      "--verbose",
    ];

    if (targetSchema) {
      args.push(`--schema=public`);
    }

    args.push(backupPath);

    logger.info(`PG RESTORE CMD: ${process.env.PG_RESTORE_PATH || "pg_restore"} ${args.join(" ")}`);
    return {
      command: process.env.PG_RESTORE_PATH || "pg_restore",
      args,
      env: {
        ...process.env,
        PGPASSWORD: password,
        PGSSLMODE: sslMode || "require",
        ...(targetSchema && {
          PGOPTIONS: `-c search_path=${targetSchema}`
        })
      }
    };
  }
  


  if (engine === "mysql") {
    return {
      command: process.env.MYSQL_RESTORE_PATH || "mysql",
      args: [
        "-h", host,
        "-P", String(port),
        "-u", username,
        "--database", database,
        "-f"
      ],
      env: {
        ...process.env,
        MYSQL_PWD: password
      },
      stdinFile: backupPath
    };
  }


  if (engine === "mongodb") {
    const originalDb = database.original;
    const newDb = database.target;

    const hasCredentials =
      username &&
      password &&
      username.trim() !== "" &&
      password.trim() !== "";

    const isSrv = !port;

    const isLocalhost = localHosts.includes(host);

    const uri = isSrv
      ? (() => {
          if (!hasCredentials) {
            throw new Error("MongoDB Atlas requires username and password");
          }
          const user = encodeURIComponent(username);
          const pass = encodeURIComponent(password);
          return `mongodb+srv://${user}:${pass}@${host}/${originalDb}?authSource=admin`;
        })()
      : hasCredentials
        ? `mongodb://${encodeURIComponent(username)}:${encodeURIComponent(password)}@${host}:${port}/${originalDb}?authSource=admin`
        : `mongodb://${host}:${port}/${originalDb}${isLocalhost ? "" : "?tls=true"}`;


    return {
      command: process.env.MONGO_RESTORE_PATH || "mongorestore",
      args: [
        `--uri=${uri}`,
        `--archive=${backupPath}`,
        "--gzip",
        `--nsFrom=${originalDb}.*`,
        `--nsTo=${newDb}.*`
      ],
      env: { ...process.env }
    };
  }


  throw new Error(`Unsupported engine: ${engine}`);
}


module.exports = { buildRestoreCommand };