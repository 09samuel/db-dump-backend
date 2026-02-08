const { STSClient, AssumeRoleCommand, GetCallerIdentityCommand } = require("@aws-sdk/client-sts");

async function assumeClientRole({ roleArn, region }) {
  if (!roleArn) {
    throw new Error("roleArn is required to assume role");
  }

  const sts = new STSClient({ 
    region,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    }
  });

  const command = new AssumeRoleCommand({
    RoleArn: roleArn,
    RoleSessionName: "db-backup-session",
    ExternalId: "database-dump",
    DurationSeconds: 3600,
  });

  const caller = await sts.send(new GetCallerIdentityCommand({}));
  console.log("Assumed identity:", caller.Arn);


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

