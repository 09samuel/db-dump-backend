const { verifyPostgres } = require("./postgres");

async function verifyConnectionCredentials({ db_type, db_host, db_port, db_name, db_user_name, db_user_secret },  options = {}) {
  switch (db_type) {
    case "postgresql":
      await verifyPostgres({ db_host, db_port, db_name, db_user_name, db_user_secret }, options);
      break;

    default:
      throw new Error(`Unsupported db_type: ${db_type}`);
  }
}

module.exports = { verifyConnectionCredentials };
