const { Pool } = require("pg");

const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});


(async () => {
  try {
    const result = await pool.query("SELECT NOW()");
    console.log("Connected at:", result.rows[0]);
  } catch (err) {
    console.error("DB connection error:", err);
  }
})();


module.exports = { pool };