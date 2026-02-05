const { S3Client, DeleteObjectCommand } = require("@aws-sdk/client-s3");
const { assumeClientRole } = require("../config/assumeClientRole");

async function deleteFromS3({ s3Path, region, roleArn }) {
  if (!s3Path || !region || !roleArn) {
    throw new Error("Missing required S3 delete parameters");
  }

  const { bucket, key } = parseS3Path(s3Path);

  const creds = await assumeClientRole({ roleArn, region, });

  const s3 = new S3Client({
    region,
    credentials: {
      accessKeyId: creds.accessKeyId,
      secretAccessKey: creds.secretAccessKey,
      sessionToken: creds.sessionToken,
    },
  });

  try {
    await s3.send(
      new DeleteObjectCommand({
        Bucket: bucket,
        Key: key,
      })
    );
  } catch (err) {
    // If object already gone, treat as success
    if (err.name === "NoSuchKey") {
      return;
    }
    throw err;
  }
}


function parseS3Path(s3Path) {
  if (!s3Path.startsWith("s3://")) {
    throw new Error("Invalid S3 path");
  }

  const withoutScheme = s3Path.slice("s3://".length);
  const [bucket, ...keyParts] = withoutScheme.split("/");

  const key = keyParts.join("/");

  if (!bucket || !key) {
    throw new Error("Invalid S3 path format");
  }

  return { bucket, key };
}

module.exports = { deleteFromS3 };
