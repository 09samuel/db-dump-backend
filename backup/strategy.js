function getBackupCommand(dbType, backupType, config) {

  switch (dbType.toLowerCase()) {
    case "postgresql": {
      const args = [
        "-h", config.host,
        "-p", String(config.port),
        "-U", config.user,
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
        },
      };
    }


    case "mysql": {
      const args = [
        "-h", config.host,
        "-P", String(config.port),
        "-u", config.user,
      ];

      if (backupType === "STRUCTURE_ONLY") {
        args.push("--no-data");
      } else if (backupType === "DATA_ONLY") {
        args.push("--no-create-info");
      }

      args.push(`--password=${config.password}`);
      args.push(config.database);

      return {
        cmd: "mysqldump",
        args,
      };
    }

    case "mongodb": {
      if (backupType === "STRUCTURE_ONLY") {
        throw new Error("MongoDB does not support STRUCTURE_ONLY backups");
      }

      return {
        cmd: "mongodump",
        args: [
          `--uri=${config.uri}`,
          "--archive",
        ],
      };
    }



    default:
      throw new Error(`Unsupported db_type: ${dbType}`);
  }
}

module.exports = { getBackupCommand };
