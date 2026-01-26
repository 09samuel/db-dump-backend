const { STSClient, AssumeRoleCommand } = require("@aws-sdk/client-sts");

async function assumeClientRole({ roleArn, region }) {
  const sts = new STSClient({ region });

  const command = new AssumeRoleCommand({
    RoleArn: roleArn,
    RoleSessionName: "db-backup-session",
    DurationSeconds: 3600,
  });

  const response = await sts.send(command);

  return {
    accessKeyId: response.Credentials.AccessKeyId,
    secretAccessKey: response.Credentials.SecretAccessKey,
    sessionToken: response.Credentials.SessionToken,
    expiration: response.Credentials.Expiration,
  };
}

module.exports = { assumeClientRole };
