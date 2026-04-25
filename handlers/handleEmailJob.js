const { sendVerificationEmail, sendResetEmail } = require("../services/authService");

async function handleEmailJob(job) {
  const { type, email, token } = job.data;

  if (type === "VERIFY_EMAIL") {
    const verifyLink = `${process.env.FRONTEND_URL}/verify-email/${token}`;
    await sendVerificationEmail(email, verifyLink);
  }

  if (type === "RESET_PASSWORD") {
    const resetLink = `${process.env.FRONTEND_URL}/reset-password/${token}`;
    await sendResetEmail(email, resetLink);
  }
}
module.exports = { handleEmailJob };