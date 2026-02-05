const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
const { assumeClientRole } = require("../config/assumeClientRole");

async function generatePresignedDownloadUrl({ bucket, region, path, roleArn, expiresInSeconds = 600 }) {
  const creds = await assumeClientRole({ roleArn, region });

  const s3 = new S3Client({
    region,
    credentials: {
      accessKeyId: creds.accessKeyId,
      secretAccessKey: creds.secretAccessKey,
      sessionToken: creds.sessionToken
    }
  });

  const command = new GetObjectCommand({
    Bucket: bucket,
    Key: normalizeS3Key(path)
  });

  return getSignedUrl(s3, command, { expiresIn: expiresInSeconds });
}

function normalizeS3Key(path) {
  if (path.startsWith("s3://")) {
    const [, , , ...rest] = path.split("/");
    return rest.join("/");
  }
  return path;
}


module.exports = { generatePresignedDownloadUrl };
