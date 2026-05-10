require('dotenv').config();
const crypto = require("crypto");
// const nodemailer = require("nodemailer");
// const { SESv2Client, SendEmailCommand } = require("@aws-sdk/client-sesv2");
const { Resend } = require("resend");
const jwt = require("jsonwebtoken");

// const sesClient = new SESv2Client({
//     region: "ap-south-1",
//     credentials: {
//         accessKeyId: process.env.AWS_ACCESS_KEY_ID,
//         secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
//     },
// });

//nodemailer transporter
    // const transporter = nodemailer.createTransport({
    //     SES: { sesClient, SendEmailCommand },
    // });

const resend = new Resend(process.env.RESEND_API_KEY);

const createVerificationToken = async (client, id) => {
    const token = crypto.randomBytes(32).toString("hex");
    const hashedToken = crypto.createHash("sha256").update(token).digest("hex");

    const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000);

    await client.query(
        `INSERT INTO email_verification_tokens (user_id, token_hash, expires_at)
        VALUES ($1, $2, $3)`,
        [id, hashedToken, expiresAt]
    );

    return token;
};

const sendVerificationEmail = async (email, verificationLink) => {
    // await transporter.sendMail({
    //     from: process.env.SES_EMAIL,
    //     to: email,
    //     subject: "Verify your email",
    //     html: `
    //         <h2>Email Verification</h2>
    //         <p>Click the link below to verify your email:</p>
    //         <a href="${verificationLink}">Verify Email</a>
    //         <p>This link expires in 24 hours.</p>
    //     `
    // });

    const { data, error } = await resend.emails.send({
        from: "Database Dump <noreply@databasedump.me>",
        to: email,
        subject: "Verify your email",
         html: `
            <h2>Email Verification</h2>
            <p>Click the link below to verify your email:</p>
            <a href="${verificationLink}">Verify Email</a>
            <p>This link expires in 24 hours.</p>
        `
    });

    if (error) {
        console.error("Resend verification email error:", error);
        throw new Error(error.message || "Failed to send verification email");
    }

    console.log("Verification email sent:", data?.id);
    return data;
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

const sendResetEmail = async (email, resetLink) => {

    // await transporter.sendMail({
    //     from: process.env.SES_EMAIL,
    //     to: email,
    //     subject: "Reset your password",
    //     html: `
    //         <h2>Password Reset</h2>
    //         <a href="${resetLink}">Reset Password</a>
    //         <p>Expires in 15 minutes</p>
    //     `,
    // });

    const { data, error } = await resend.emails.send({
        from: "Database Dump <noreply@databasedump.me>",
        to: email,
        subject: "Reset your password",
        html: `
            <h2>Password Reset</h2>
            <p>Click the link below to reset your password:</p>
            <a href="${resetLink}">Reset Password</a>
            <p>This link expires in 15 minutes.</p>
        `
    });

    if (error) {
        console.error("Resend reset email error:", error);
        throw new Error(error.message || "Failed to send reset email");
    }

    console.log("Reset email sent:", data?.id);
    return data;
};

module.exports = { createVerificationToken, sendVerificationEmail, generateAccessToken, generateRefreshToken, hashToken, sendResetEmail };