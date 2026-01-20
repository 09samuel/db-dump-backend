const crypto = require("crypto");

const ALGORITHM = "aes-256-gcm";

if (!process.env.ENCRYPTION_KEY) {
  throw new Error("ENCRYPTION_KEY is missing. Did you load dotenv?");
}

const KEY = Buffer.from(process.env.ENCRYPTION_KEY, "hex"); // 32 bytes

function encrypt (text) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv(ALGORITHM, KEY, iv);

  const encrypted = Buffer.concat([
    cipher.update(text, "utf8"),
    cipher.final(),
  ]);

  const authTag = cipher.getAuthTag();

  return Buffer.concat([iv, authTag, encrypted]).toString("hex");
}

function decrypt (encryptedHex) {
  const buffer = Buffer.from(encryptedHex, "hex");

  const iv = buffer.subarray(0, 12);
  const authTag = buffer.subarray(12, 28);
  const encryptedText = buffer.subarray(28);

  const decipher = crypto.createDecipheriv(ALGORITHM, KEY, iv);
  decipher.setAuthTag(authTag);

  return decipher.update(encryptedText, null, "utf8") + decipher.final("utf8");
}

module.exports = { encrypt, decrypt };