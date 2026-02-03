const fs = require("fs");
const fsPromises = require("fs/promises");
const path = require("path");
const os = require("os");
const { assumeClientRole } = require("../config/assumeClientRole")

const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");


//Download a backup from S3 and return a local filesystem path
async function downloadFromS3({ bucket, region, key, roleArn }) {
  if (!bucket || !region || !key || !roleArn) {
    throw new Error("Missing required S3 download parameters");
  }

  const creds = await assumeClientRole({ roleArn, region });

  //create S3 client
  const s3 = new S3Client({
    region: region, 
    credentials: {
      accessKeyId: creds.accessKeyId,
      secretAccessKey: creds.secretAccessKey,
      sessionToken: creds.sessionToken,
    },
  });

  //prepare local temp path
  const tmpDir = path.join(os.tmpdir(), "db-restores");
  await fsPromises.mkdir(tmpDir, { recursive: true });

  const fileName = path.basename(key);
  const localPath = path.join(
    tmpDir,
    `${Date.now()}-${fileName}`
  );

  //download object
  let bodyStream;

  try {
    const res = await s3.send(
      new GetObjectCommand({
        Bucket: bucket,
        Key: key
      })
    );

    bodyStream = res.Body;

    if (!bodyStream || typeof bodyStream.pipe !== "function") {
      throw new Error("Invalid S3 response body");
    }

    await streamToFile(bodyStream, localPath);
    return localPath;

  } catch (err) {
    // cleanup partial file
    await safeUnlink(localPath);
    throw err;
  } finally {
    await safeUnlink(localPath);
  }

}

module.exports = { downloadFromS3 };
