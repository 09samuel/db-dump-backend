const fs = require("fs");
const path = require("path");
const { S3Client } = require("@aws-sdk/client-s3");
const { Upload } = require("@aws-sdk/lib-storage");
const { PassThrough } = require("stream");
const { assumeClientRole } = require("../config/assumeClientRole")

async function createStorageStream(config) {
  const { storageTarget } = config;

  if (storageTarget === "LOCAL") {
    return createLocalStream(config);
  }

  if (storageTarget === "S3") {
    return await createClientS3Stream(config);
  }

  throw new Error(`Unsupported storage target: ${storageTarget}`);
}


//local
function createLocalStream({ resolvedPath }) {
  fs.mkdirSync(path.dirname(resolvedPath), { recursive: true });

  const fileStream = fs.createWriteStream(resolvedPath);
  let bytesWritten = 0;

  const countingStream = new PassThrough();

  countingStream.on("data", (chunk) => {
    bytesWritten += chunk.length;
  });

  fileStream.on("error", (err) => {
    countingStream.destroy(err);
  });

  countingStream.pipe(fileStream);

  return {
    stream: countingStream,
    path: resolvedPath,
    getBytesWritten: () => bytesWritten,
  };
}

//s3
async function createClientS3Stream({
  s3Bucket,
  s3Region,
  roleArn,
}) {
  //Assume client role
  const creds = await assumeClientRole({
    roleArn,
    region: s3Region,
  });

  //Create S3 client with assumed creds
  const s3 = new S3Client({
    region: s3Region, 
    credentials: {
      accessKeyId: creds.accessKeyId,
      secretAccessKey: creds.secretAccessKey,
      sessionToken: creds.sessionToken,
    },
  });

  const stream = new PassThrough();
  let bytesWritten = 0;

  stream.on("data", (chunk) => {
    bytesWritten += chunk.length;
  });

  const objectKey = `backups/${Date.now()}.dump`;

  //Start upload
  const upload = new Upload({
    client: s3,
    params: {
      Bucket: s3Bucket,
      Key: objectKey,
      Body: stream,
    },
  });

  upload.done()
    .then(() => {
      console.log("Client S3 upload complete");
    })
    .catch((err) => {
      console.error("Client S3 upload failed", err);
      stream.destroy(err);
    });

  return {
    stream,
    path: `s3://${s3Bucket}/${objectKey}`,
    getBytesWritten: () => bytesWritten,
  };
}


module.exports = { createStorageStream };
