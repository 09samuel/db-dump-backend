function buildRestoreCommand({ engine, host, port, database, username, password, backupPath }) {
    if (engine === "postgresql") {
        return {
            command: "psql",
            args: [
                "-h", host,
                "-p", String(port),
                "-U", username,
                "-d", database,
                "-f", backupPath
            ],
            env: {
                ...process.env,
                PGPASSWORD: password
            }
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
        const isArchive = backupPath.endsWith(".archive") || backupPath.endsWith(".gz");

        const args = [
            "--host", host,
            "--port", String(port),
            "--username", username,
            "--password", password,
            "--authenticationDatabase", "admin",
            "--drop"
        ];

        if (isArchive) {
            args.push("--archive=" + backupPath);
        } else {
            args.push("--dir=" + backupPath);
        }

        if (database) {
            args.push("--nsInclude", `${database}.*`);
        }

        return {
            command: "mongorestore",
            args,
            env: {
                ...process.env
            }
        };
    }

    throw new Error(`Unsupported engine: ${engine}`);
}


module.exports = { buildRestoreCommand };