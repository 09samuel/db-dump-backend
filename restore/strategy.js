function buildRestoreCommand({ engine, host, port, database, username, password, backupPath }) {




if (engine === "postgresql") {
  return {
    command: "psql",
    args: [
      "-h", host,
      "-p", String(port),
      "-U", username,
      "-d", database
    ],
    env: {
      ...process.env,
      PGPASSWORD: password
    },
    stdinFile: backupPath   // 🔥 REQUIRED
  };
}



    if (engine === "mysql") {
        return {
            command: "mysql",
            args: [
                "-h", host,
                "-P", String(port),
                "-u", username,
                database
            ],
            env: {
                ...process.env,
                MYSQL_PWD: password
            },
            stdinFile: backupPath
        };
    }


if (engine === "mongodb") {
  return {
    command: "mongorestore",
    args: [
      "--drop",
      "--archive"
    ],
    env: { ...process.env },
    stdinFile: backupPath   // 🔥 REQUIRED
  };
}


    throw new Error(`Unsupported engine: ${engine}`);
}


module.exports = { buildRestoreCommand };