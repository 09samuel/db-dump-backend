const { verifyPostgres } = require("./postgres");
const { verifyMySQL } = require("./mysql");
const { verifyMongodb } = require("./mongodb");

async function verifyConnectionCredentials({ db_type, db_host, db_port, db_name, db_user_name, db_user_secret, ssl_mode },  options = {}) {
  switch (db_type) {
    case "postgresql":
      await verifyPostgres({ db_host, db_port, db_name, db_user_name, db_user_secret, ssl_mode }, options);
      break;
    
    case "mysql":
      await verifyMySQL({ db_host, db_port, db_name, db_user_name, db_user_secret, ssl_mode }, options);
      break;


    case "mongodb":
      await verifyMongodb({ db_host, db_port, db_name, db_user_name, db_user_secret }, options);
      break;

    default:
      throw new Error(`Unsupported db_type: ${db_type}`);
  }
}

module.exports = { verifyConnectionCredentials };
