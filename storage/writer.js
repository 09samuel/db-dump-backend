const fs = require("fs");
const os = require("os");
const path = require("path");
const { PassThrough } = require("stream");

function createStorageStream(storageTarget) {
  const filePath = resolveStoragePath(storageTarget);
  const fileStream = fs.createWriteStream(filePath);
  let bytesWritten = 0;

  const countingStream = new PassThrough();

  countingStream.on("data", (chunk) => {
    bytesWritten += chunk.length;
  });

  countingStream.pipe(fileStream);

  return {
    stream: countingStream,
    getBytesWritten: () => bytesWritten,
  };
}

function resolveStoragePath(storageTarget) {
  switch (storageTarget) {
    case "LOCAL_DESKTOP": {
      const desktop = path.join(os.homedir(), "Desktop");
      return path.join(desktop, `backup-${Date.now()}.dump`);
    }

    case "LOCAL_TMP":
      return path.join("/tmp", `backup-${Date.now()}.dump`);

    default:
      throw new Error(`Unsupported storage target: ${storageTarget}`);
  }
}


module.exports = { createStorageStream };