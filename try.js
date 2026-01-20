const { execSync } = require("child_process");

try {
  console.log(execSync("pg_dump --version").toString());
} catch (err) {
  console.error("pg_dump NOT available to Node.js", err.code);
}
