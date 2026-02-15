const { MongoClient } = require("mongodb");

async function verifyMongodb(
  { db_host, db_port, db_name, db_user_name, db_user_secret },
  options = {}
) {
  const hasCredentials =
    db_user_name &&
    db_user_secret &&
    db_user_name.trim() !== "" &&
    db_user_secret.trim() !== "";

  const isSrv = !db_port; // if no port, assume SRV (Atlas)

  const uri = isSrv
    ? (() => {
        if (!hasCredentials) {
          throw new Error("MongoDB Atlas requires username and password");
        }
        const user = encodeURIComponent(db_user_name);
        const pass = encodeURIComponent(db_user_secret);
        return `mongodb+srv://${user}:${pass}@${db_host}/${db_name}`;
      })()
    : hasCredentials
      ? `mongodb://${encodeURIComponent(db_user_name)}:${encodeURIComponent(db_user_secret)}@${db_host}:${db_port}/${db_name}?authSource=admin`
      : `mongodb://${db_host}:${db_port}/${db_name}`;

  const client = new MongoClient(uri, {
    serverSelectionTimeoutMS: options.timeoutMs || 5000,
  });

  try {
    await client.connect();
    await client.db(db_name).command({ ping: 1 });
    return { status: "OK" };
  } finally {
    await client.close();
  }
}

module.exports = { verifyMongodb };
