const fs = require("fs");
const fsPromises = require("fs/promises");
const path = require("path");
const os = require("os");
const { assumeClientRole } = require("../config/assumeClientRole")
const { pipeline } = require("stream/promises");

const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");


//Download a backup from S3 and return a local filesystem path
async function downloadFromS3({ bucket, region, s3Path, roleArn }) {
  if (!bucket || !region || !s3Path || !roleArn) {
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

  const fileName = path.basename(s3Path);
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
        Key: normalizeS3Key(s3Path)
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
  }

}

async function safeUnlink(filePath) {
  if (typeof filePath !== "string" || filePath.length === 0) {
    return;
  }

  try {
    await fsPromises.unlink(filePath);
  } catch (err) {
    // File already gone → OK
    if (err.code === "ENOENT") return;

    // Everything else is a real problem
    console.error("Failed to cleanup temp file:", {
      filePath,
      error: err.message,
    });

    throw err;
  }
}


async function streamToFile(readable, destinationPath) {
  await pipeline(
    readable,
    fs.createWriteStream(destinationPath, { flags: "wx" })
  );
}


function normalizeS3Key(s3Path) {
  if (s3Path.startsWith("s3://")) {
    const [, , , ...rest] = s3Path.split("/");
    return rest.join("/");
  }
  return s3Path;
}



module.exports = { downloadFromS3 };
