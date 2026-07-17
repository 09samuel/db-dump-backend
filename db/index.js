const { Pool } = require("pg");
const logger = require("../utils/logger");

const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});


(async () => {
  try {
    const result = await pool.query("SELECT NOW()");
    logger.info(`Connected at: ${JSON.stringify(result.rows[0])}`);
  } catch (err) {
    logger.error("DB connection error:", err);
  }
})();


module.exports = { pool };