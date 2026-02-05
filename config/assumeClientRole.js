const { STSClient, AssumeRoleCommand } = require("@aws-sdk/client-sts");

async function assumeClientRole({ roleArn, region }) {
  if (!roleArn) {
    throw new Error("roleArn is required to assume role");
  }

  const sts = new STSClient({ region });

  const command = new AssumeRoleCommand({
    RoleArn: roleArn,
    RoleSessionName: "db-backup-session",
    DurationSeconds: 3600,
  });

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

