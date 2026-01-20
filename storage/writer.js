const fs = require("fs");
const os = require("os");
const path = require("path");
const { PassThrough } = require("stream");
const crypto = require("crypto");

function createStorageStream(storageTarget) {
  const filePath = resolveStoragePath(storageTarget);

  // Ensure directory exists
  fs.mkdirSync(path.dirname(filePath), { recursive: true });

  const fileStream = fs.createWriteStream(filePath);
  let bytesWritten = 0;

  const countingStream = new PassThrough();

  countingStream.on("data", (chunk) => {
    bytesWritten += chunk.length;
  });

  // Propagate write errors
  fileStream.on("error", (err) => {
    countingStream.destroy(err);
  });

  countingStream.pipe(fileStream);

  return {
    stream: countingStream,
    path: filePath,
    getBytesWritten: () => bytesWritten,
  };
}

function resolveStoragePath(storageTarget) {
  const uniqueId = crypto.randomUUID();
  const filename = `backup-${Date.now()}-${uniqueId}.dump`;

  switch (storageTarget) {
    case "LOCAL_DESKTOP": {
      const desktop = path.join(os.homedir(), "Desktop", "db-backups");
      return path.join(desktop, filename);
    }

    case "LOCAL_TMP":
      return path.join(os.tmpdir(), filename);

    default:
      throw new Error(`Unsupported storage target: ${storageTarget}`);
  }
}

module.exports = { createStorageStream };
