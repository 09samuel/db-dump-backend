const { S3Client } = require("@aws-sdk/client-s3");

function createS3Client({ credentials, region }) {
  const s3Config = {
    region,
    credentials,
  };

  if (process.env.NODE_ENV === "development") {
    // Redirect to LocalStack
    s3Config.endpoint = process.env.LOCALSTACK_ENDPOINT || "http://localhost:4566";
    s3Config.forcePathStyle = true;
  }

  return new S3Client(s3Config);
}

module.exports = { createS3Client };
