function getBackupCommand(dbType, backupType, config) {

  switch (dbType.toLowerCase()) {
    case "postgresql": {
      const args = [
        "-h", config.host,
        "-p", String(config.port),
        "-U", config.user,
        "-Fc",
        "--no-owner",
        "--no-privileges",
      ];

      if (backupType === "STRUCTURE_ONLY") {
        args.push("--schema-only");
      } else if (backupType === "DATA_ONLY") {
        args.push("--data-only");
      }

      args.push(config.database);

      return {
        cmd: "pg_dump",
        args,
        env: { 
          PGPASSWORD: config.password,
          PGSSLMODE: config.sslMode || "disable", 
       },
        alreadyCompressed: true,
        extension: ".pgdump"
      };
    }


    case "mysql": {
      const args = [
        "-h", config.host,
        "-P", String(config.port),
        "-u", config.user,
      ];

      if (config.sslMode === "require") {
        args.push("--ssl-mode=REQUIRED");
      }

      if (backupType === "STRUCTURE_ONLY") {
        args.push("--no-data");
      } else if (backupType === "DATA_ONLY") {
        args.push("--no-create-info");
      }

      if (config.password) {
        args.push(`--password=${config.password}`);
      }

      args.push(config.database);

      return {
        cmd: process.env.MYSQLDUMP_PATH || "mysqldump",
        args,
        alreadyCompressed: false,
        extension: ".sql"
      };
    }

  
    case "mongodb": {
      if (backupType === "STRUCTURE_ONLY") {
        throw new Error("MongoDB does not support STRUCTURE_ONLY backups");
      }

      const hasCredentials =
        config.user &&
        config.password &&
        config.user.trim() !== "" &&
        config.password.trim() !== "";

      const isSrv = !config.port;

      const uri = isSrv
        ? (() => {
            if (!hasCredentials) {
              throw new Error("MongoDB Atlas requires username and password");
            }
            const user = encodeURIComponent(config.user);
            const pass = encodeURIComponent(config.password);
            return `mongodb+srv://${user}:${pass}@${config.host}/${config.database}`;
          })()
        : hasCredentials
          ? `mongodb://${encodeURIComponent(config.user)}:${encodeURIComponent(config.password)}@${config.host}:${config.port}/${config.database}?authSource=admin`
          : `mongodb://${config.host}:${config.port}/${config.database}`;

      return {
        cmd: process.env.MONGODUMP_PATH || "mongodump",
        args: [
          `--uri=${uri}`,
          "--archive",
          "--gzip"
        ],
        alreadyCompressed: true,
        extension: ".archive.gz"
      };
    }
  }
}

module.exports = { getBackupCommand };
