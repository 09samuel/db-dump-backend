const { STSClient, AssumeRoleCommand, GetCallerIdentityCommand } = require("@aws-sdk/client-sts");

async function assumeClientRole({ roleArn, region }) {
  if (!roleArn) {
    throw new Error("roleArn is required to assume role");
  }

  const stsConfig = { 
    region,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID || "mock-access-key-id",
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "mock-secret-access-key"
    }
  };

  if (process.env.NODE_ENV === "development") {
    stsConfig.endpoint = process.env.LOCALSTACK_ENDPOINT || "http://localhost:4566";
  }

  const sts = new STSClient(stsConfig);

  const command = new AssumeRoleCommand({
    RoleArn: roleArn,
    RoleSessionName: "db-backup-session",
    ExternalId: "database-dump",
    DurationSeconds: 3600,
  });

  const logger = require("../utils/logger");
  const caller = await sts.send(new GetCallerIdentityCommand({}));
  logger.info(`Assumed identity: ${caller.Arn}`);


  const response = await sts.send(command);

  if (!response.Credentials) {
    throw new Error("Failed to assume role: no credentials returned");
  }

  return {
    accessKeyId: response.Credentials.AccessKeyId,
    secretAccessKey: response.Credentials.SecretAccessKey,
    sessionToken: response.Credentials.SessionToken,
    expiration: response.Credentials.Expiration,
  };
}

module.exports = { assumeClientRole };

