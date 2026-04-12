const crypto = require("crypto");
const nodemailer = require("nodemailer");
const AWS = require("aws-sdk");
const jwt = require("jsonwebtoken");

AWS.config.update({ region: "us-east-1" });


const createVerificationToken = async (client, email) => {
    const token = crypto.randomBytes(32).toString("hex");
    const hashedToken = crypto.createHash("sha256").update(token).digest("hex");

    const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000);

    await client.query(
        `INSERT INTO email_verification_tokens (email, hashed_token, expires_at)
        VALUES ($1, $2, $3)`,
        [email, hashedToken, expiresAt]
    );

    return token;
};

const sendEmail = async (email, token) => {
    const verificationLink = `${process.env.SERVER_URL}/auth/verify-email?token=${token}`;

    const transporter = nodemailer.createTransport({
        SES: new AWS.SES(),
    });

    await transporter.sendMail({
        from: process.env.SES_EMAIL,
        to: email,
        subject: "Verify your email",
        html: `
            <h2>Email Verification</h2>
            <p>Click the link below to verify your email:</p>
            <a href="${verificationLink}">Verify Email</a>
            <p>This link expires in 24 hours.</p>
        `
    });
};


const generateAccessToken = (userId) => {
  return jwt.sign(
    { userId },
    process.env.ACCESS_TOKEN_SECRET,
    { expiresIn: "15m" }
  );
};

const generateRefreshToken = (userId) => {
  return jwt.sign(
    { userId },
    process.env.REFRESH_TOKEN_SECRET,
    { expiresIn: "7d" }
  );
};

const hashToken = (token) => {
  return crypto.createHash("sha256").update(token).digest("hex");
}

const sendResetEmail = async (email, link) => {
    const transporter = nodemailer.createTransport({
        SES: new AWS.SES(),
    });

    await transporter.sendMail({
        from: process.env.SES_EMAIL,
        to: email,
        subject: "Reset your password",
        html: `
            <h2>Password Reset</h2>
            <a href="${link}">Reset Password</a>
            <p>Expires in 15 minutes</p>
        `,
    });
};

module.exports = { createVerificationToken, sendEmail, generateAccessToken, generateRefreshToken, hashToken, sendResetEmail };