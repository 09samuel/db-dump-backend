const { pool } = require("../db/index");
const bcrypt = require('bcrypt');
const crypto = require("crypto");
const { createVerificationToken, generateAccessToken, generateRefreshToken, hashToken } = require("../services/authService");
const { enqueueEmailJob } = require("../queue/email.queue");
const { getRequestMeta, insertAuditLog } = require("../utils/auditLogger");

async function logAuthEvent({
    req,
    userId = null,
    userEmail = null,
    roleAtTime = "SYSTEM",
    actionType,
    status,
    message,
    errorMessage = null,
    resourceType = "USER",
    resourceId = null,
    metadata = {},
}) {
    const requestMeta = getRequestMeta(req);

    await insertAuditLog({
        userId,
        userEmail,
        roleAtTime,
        actionType,
        actionCategory: "AUTH",
        resourceType,
        resourceId,
        message,
        status,
        errorMessage,
        metadata,
        ipAddress: requestMeta.ipAddress,
        userAgent: requestMeta.userAgent,
    });
}

// Register user with email verification
const registerUser = async (req, res) => {
    let client;

    try {
        client = await pool.connect();

        const { name, email, password, confirmPassword } = req.body;

        const validationErrors = {};

        if (!name?.trim()) {
            validationErrors.name = "Name is required";
        }

        if (!email?.trim()) {
            validationErrors.email = "Email is required";
        } else if (!/^\S+@\S+\.\S+$/.test(email.trim())) {
            validationErrors.email = "Invalid email format";
        }

        if (!password) {
            validationErrors.password = "Password is required";
        } else if (password.length < 6) {
            validationErrors.password = "Password must be at least 6 characters";
        } else if (password.length > 100) {
            validationErrors.password = "Password must be less than 100 characters";
        } else if (!/[A-Z]/.test(password)) {
            validationErrors.password = "Password must contain at least one uppercase letter";
        } else if (!/[a-z]/.test(password)) {
            validationErrors.password = "Password must contain at least one lowercase letter";
        } else if (!/[0-9]/.test(password)) {
            validationErrors.password = "Password must contain at least one number";
        } else if (!/[!@#$%^&*]/.test(password)) {
            validationErrors.password = "Password must contain at least one special character (!@#$%^&*)";
        }

        if (password !== confirmPassword) {
            validationErrors.confirmPassword = "Passwords do not match";
        }

        if (Object.keys(validationErrors).length > 0) {
            await logAuthEvent({
                req,
                userEmail: email?.trim()?.toLowerCase() || null,
                actionType: "REGISTER_ATTEMPT",
                status: "FAILED",
                message: "Registration failed due to validation errors",
                errorMessage: JSON.stringify(validationErrors),
            });
            return res.status(400).json({ success: false, errors: validationErrors });
        }
        
        const normalizedEmail = email.trim().toLowerCase();
        const normalizedName = name.trim();

        //check if password and confirm password match
        if (password !== confirmPassword) {
            await logAuthEvent({
                req,
                userEmail: normalizedEmail,
                actionType: "REGISTER_ATTEMPT",
                status: "FAILED",
                message: "Registration failed due to password mismatch",
                errorMessage: "Passwords do not match",
            });
            return res.status(400).json({ success: false, message: 'Passwords do not match' });
        }

        await client.query('BEGIN');

        //check if user already exists
        const existingUser = await client.query('SELECT id FROM users WHERE email = $1', [normalizedEmail]);

        if (existingUser.rows.length > 0) {
            await client.query("ROLLBACK");
            await logAuthEvent({
                req,
                userEmail: normalizedEmail,
                actionType: "REGISTER_ATTEMPT",
                status: "DENIED",
                message: "Registration denied because user already exists",
                errorMessage: "User already exists",
            });
            return res.status(400).json({ success: false, message: 'User already exists' });
        }

        const hashedPassword = await bcrypt.hash(password, 10);

        //insert new user into database
        const newUser = await client.query('INSERT INTO users (name, email, password_hash, is_verified) VALUES ($1, $2, $3, $4) RETURNING id', [normalizedName, normalizedEmail, hashedPassword, false]);

        const verificationToken = await createVerificationToken(client, newUser.rows[0].id);
        
        await client.query('COMMIT');

        await enqueueEmailJob({ type: "VERIFY_EMAIL", email: normalizedEmail, token: verificationToken });
        await logAuthEvent({
            req,
            userId: newUser.rows[0].id,
            userEmail: normalizedEmail,
            roleAtTime: "OWNER",
            actionType: "REGISTER_ATTEMPT",
            status: "SUCCESS",
            message: "User registered and verification email queued",
            resourceId: newUser.rows[0].id,
        });

        res.status(201).json({ success: true, message: 'User registered. Please verify your email' });
    } catch (error) {
        if (client) await client.query("ROLLBACK");
        console.error('Error in registerUser:', error);
        await logAuthEvent({
            req,
            userEmail: req.body?.email?.trim()?.toLowerCase() || null,
            actionType: "REGISTER_ATTEMPT",
            status: "FAILED",
            message: "Registration failed due to internal error",
            errorMessage: error.message,
        });
        res.status(500).json({ success: false, message: 'Internal server error' });
    } finally {
        if (client) client.release();
    }
}

// Verify email using token
const verifyEmail = async (req, res) => {
  let client;

  try {
    client = await pool.connect();

    const { token } = req.query;

    if (!token) {
        await logAuthEvent({
            req,
            actionType: "VERIFY_EMAIL",
            status: "FAILED",
            message: "Email verification failed due to missing token",
            errorMessage: "Invalid or missing token",
        });
        return res.status(400).json({
            success: false,
            message: "Invalid or missing token",
        });
    }

    //Hash incoming token (same way as stored)
    const hashedToken = crypto.createHash("sha256").update(token).digest("hex");

    //Find token in DB
    const result = await client.query(
        `SELECT user_id, expires_at
        FROM email_verification_tokens
        WHERE token_hash = $1`,
        [hashedToken]
    );

    if (result.rows.length === 0) {
      await logAuthEvent({
        req,
        actionType: "VERIFY_EMAIL",
        status: "FAILED",
        message: "Email verification failed due to invalid token",
        errorMessage: "Invalid token",
      });
      return res.status(400).json({
        success: false,
        message: "Invalid token",
      });
    }

    const { user_id, expires_at } = result.rows[0];

    //Check expiry
    if (new Date(expires_at) < new Date()) {
      await logAuthEvent({
        req,
        userId: user_id,
        actionType: "VERIFY_EMAIL",
        status: "FAILED",
        message: "Email verification failed due to expired token",
        errorMessage: "Token expired",
        resourceId: user_id,
      });
      return res.status(400).json({
        success: false,
        message: "Token expired",
      });
    }

    await client.query("BEGIN");

    //Mark user as verified
    await client.query(
        `UPDATE users SET is_verified = true WHERE id = $1`,
        [user_id]
    );

    //Delete token
    await client.query(
      `DELETE FROM email_verification_tokens WHERE token_hash = $1`,
      [hashedToken]
    );

    await client.query("COMMIT");
    await logAuthEvent({
      req,
      userId: user_id,
      actionType: "VERIFY_EMAIL",
      status: "SUCCESS",
      message: "Email verified successfully",
      resourceId: user_id,
      roleAtTime: "OWNER",
    });

    return res.status(200).json({
      success: true,
      message: "Email verified successfully",
    });

  } catch (error) {
    if (client) await client.query("ROLLBACK");

    console.error("Error in verifyEmail:", error);
    await logAuthEvent({
      req,
      actionType: "VERIFY_EMAIL",
      status: "FAILED",
      message: "Email verification failed due to internal error",
      errorMessage: error.message,
    });

    return res.status(500).json({
      success: false,
      message: "Internal server error",
    });

  } finally {
    if (client) client.release();
  }
};

// login user
const loginUser = async (req, res) => {
    try {
        const { email, password } = req.body;

        if (!email || !password) {
            await logAuthEvent({
                req,
                userEmail: email?.trim()?.toLowerCase() || null,
                actionType: "LOGIN_ATTEMPT",
                status: "FAILED",
                message: "Login failed due to missing credentials",
                errorMessage: "Email and password are required",
            });
            return res.status(400).json({
                success: false,
                message: "Email and password are required",
            });
        }

        const normalizedEmail = email.trim().toLowerCase();

        const userResult = await pool.query(
            `SELECT id, password_hash, is_verified 
            FROM users WHERE email = $1`,
            [normalizedEmail]
        );

        console.log(userResult.rows);

        if (userResult.rows.length === 0) {
            await logAuthEvent({
                req,
                userEmail: normalizedEmail,
                actionType: "LOGIN_ATTEMPT",
                status: "DENIED",
                message: "Login denied due to invalid credentials",
                errorMessage: "Invalid email or password",
            });
            return res.status(400).json({
                success: false,
                message: "Invalid email or password",
            });
        }

        const user = userResult.rows[0];

        if (!user.is_verified) {
            await logAuthEvent({
                req,
                userId: user.id,
                userEmail: normalizedEmail,
                roleAtTime: "OWNER",
                actionType: "LOGIN_ATTEMPT",
                status: "DENIED",
                message: "Login denied because email is not verified",
                errorMessage: "Please verify your email before logging in",
                resourceId: user.id,
            });
            return res.status(400).json({
                success: false,
                message: "Please verify your email before logging in",
            });
        }

        const passwordMatch = await bcrypt.compare( password, user.password_hash );

        if (!passwordMatch) {
            await logAuthEvent({
                req,
                userId: user.id,
                userEmail: normalizedEmail,
                roleAtTime: "OWNER",
                actionType: "LOGIN_ATTEMPT",
                status: "DENIED",
                message: "Login denied due to invalid credentials",
                errorMessage: "Invalid email or password",
                resourceId: user.id,
            });
            return res.status(400).json({
                success: false,
                message: "Invalid email or password",
            });
        }

        // Generate tokens
        const accessToken = generateAccessToken(user.id);
        const refreshToken = generateRefreshToken(user.id);
        const hashedRefreshToken = crypto.createHash("sha256").update(refreshToken).digest("hex");

        // Store refresh token
        await pool.query(
            `INSERT INTO refresh_tokens (user_id, token_hash, expires_at, user_agent, ip_address)
            VALUES ($1, $2, $3, $4, $5)`,
            [user.id, hashedRefreshToken, new Date(Date.now() + 7 * 24 * 60 * 60 * 1000), req.headers["user-agent"], req.ip]
        );

        await logAuthEvent({
            req,
            userId: user.id,
            userEmail: normalizedEmail,
            roleAtTime: "OWNER",
            actionType: "LOGIN_ATTEMPT",
            status: "SUCCESS",
            message: "Login successful",
            resourceId: user.id,
        });

        // Send cookies
        res
        .cookie("accessToken", accessToken, {
            httpOnly: true,
            secure: process.env.NODE_ENV === "production",
            sameSite: "Lax",
            maxAge: 15 * 60 * 1000, // 15 minutes
        })
        .cookie("refreshToken", refreshToken, {
            httpOnly: true,
            secure: process.env.NODE_ENV === "production",
            sameSite: "Lax",
            maxAge: 7 * 24 * 60 * 60 * 1000, // 7 days
        })
        .status(200)
        .json({
            success: true,
            message: "Login successful",
        });

    } catch (error) {
        console.error("Error in loginUser:", error);
        await logAuthEvent({
            req,
            userEmail: req.body?.email?.trim()?.toLowerCase() || null,
            actionType: "LOGIN_ATTEMPT",
            status: "FAILED",
            message: "Login failed due to internal error",
            errorMessage: error.message,
        });

        return res.status(500).json({
            success: false,
            message: "Internal server error",
        });
    }
};

//refresh tokens
const refreshTokenHandler = async (req, res) => {
    let client;

    try {
        client = await pool.connect();

        const token = req.cookies.refreshToken;

        if (!token) {
            await logAuthEvent({
                req,
                actionType: "TOKEN_REFRESH",
                status: "DENIED",
                message: "Token refresh denied due to missing token",
                errorMessage: "Missing refresh token",
                resourceType: "TOKEN",
            });
            return res.status(403).json({ message: "Invalid token" });
        }

        const hashedToken = crypto.createHash("sha256").update(token).digest("hex");

        await client.query("BEGIN");

        // 1. Find token
        const result = await client.query(
            `SELECT * FROM refresh_tokens 
            WHERE token_hash = $1`,
            [hashedToken]
        );

        if (result.rows.length === 0) {
            await client.query("ROLLBACK");
            await logAuthEvent({
                req,
                actionType: "TOKEN_REFRESH",
                status: "DENIED",
                message: "Token refresh denied due to invalid token",
                errorMessage: "Invalid token",
                resourceType: "TOKEN",
            });
            return res.status(403).json({ message: "Invalid token" });
        }

        const storedToken = result.rows[0];

        // Check revoked
        if (storedToken.revoked) {

            // TOKEN REUSE DETECTED
            await client.query(
                `DELETE FROM refresh_tokens WHERE user_id = $1`,
                [storedToken.user_id]
            );

            await client.query("COMMIT");
            await logAuthEvent({
                req,
                userId: storedToken.user_id,
                roleAtTime: "OWNER",
                actionType: "TOKEN_REFRESH",
                status: "DENIED",
                message: "Token reuse detected and all refresh tokens revoked",
                errorMessage: "Token reuse detected",
                resourceId: storedToken.user_id,
                resourceType: "USER",
            });

            res
                .clearCookie("accessToken")
                .clearCookie("refreshToken");

            return res.status(403).json({
                message: "Token reuse detected. Logged out everywhere.",
            });
        }

        // Check expiry
        if (new Date(storedToken.expires_at) < new Date()) {
            await client.query("ROLLBACK");
            await logAuthEvent({
                req,
                userId: storedToken.user_id,
                roleAtTime: "OWNER",
                actionType: "TOKEN_REFRESH",
                status: "DENIED",
                message: "Token refresh denied due to expired token",
                errorMessage: "Token expired",
                resourceType: "TOKEN",
            });
            return res.status(403).json({ message: "Token expired" });
        }

        // Revoke old token
        await client.query(
            `UPDATE refresh_tokens 
            SET revoked = true 
            WHERE id = $1`,
            [storedToken.id]
        );

        // Generate new tokens
        const newAccessToken = generateAccessToken(storedToken.user_id);
        const newRefreshToken = generateRefreshToken();
        const newHashedToken = hashToken(newRefreshToken);

        // Store new refresh token
        await client.query(
            `INSERT INTO refresh_tokens 
            (user_id, token_hash, expires_at, user_agent, ip_address)
            VALUES ($1, $2, $3, $4, $5)`,
            [
                storedToken.user_id,
                newHashedToken,
                new Date(Date.now() + 7 * 24 * 60 * 60 * 1000),
                req.headers["user-agent"],
                req.ip,
            ]
        );

        await client.query("COMMIT");
        await logAuthEvent({
            req,
            userId: storedToken.user_id,
            roleAtTime: "OWNER",
            actionType: "TOKEN_REFRESH",
            status: "SUCCESS",
            message: "Token refreshed successfully",
            resourceId: storedToken.user_id,
            resourceType: "USER",
        });

        // Send cookies
        res
        .cookie("accessToken", newAccessToken, {
            httpOnly: true,
            secure: process.env.NODE_ENV === "production",
            sameSite: "Strict",
            maxAge: 15 * 60 * 1000,
        })
        .cookie("refreshToken", newRefreshToken, {
            httpOnly: true,
            secure: process.env.NODE_ENV === "production",
            sameSite: "Strict",
            maxAge: 7 * 24 * 60 * 60 * 1000,
        })
        .json({ success: true });

    } catch (error) {
        if (client) await client.query("ROLLBACK");

        console.error("Refresh error:", error);
        await logAuthEvent({
            req,
            actionType: "TOKEN_REFRESH",
            status: "FAILED",
            message: "Token refresh failed due to internal error",
            errorMessage: error.message,
            resourceType: "TOKEN",
        });

        res.status(500).json({ message: "Internal server error" });

    } finally {
        if (client) client.release();
    }
};

// Logout user
const logoutUser = async (req, res) => {
    try {

        const token = req.cookies.refreshToken;

        if (!token) {
            await logAuthEvent({
                req,
                userId: req.user?.userId || null,
                actionType: "LOGOUT",
                status: "FAILED",
                message: "Logout failed due to missing refresh token",
                errorMessage: "No token",
            });
            return res.status(400).json({ message: "No token" });
        }

        const hashedToken = crypto.createHash("sha256").update(token).digest("hex");

        await pool.query(
            `UPDATE refresh_tokens 
            SET revoked = true 
            WHERE token_hash = $1`,
            [hashedToken]
        );

        await logAuthEvent({
            req,
            userId: req.user?.userId || null,
            roleAtTime: "OWNER",
            actionType: "LOGOUT",
            status: "SUCCESS",
            message: "User logged out successfully",
            resourceId: req.user?.userId || null,
        });

        res
            .clearCookie("accessToken")
            .clearCookie("refreshToken")
            .json({ message: "Logged out successfully" });

    } catch (error) {
        console.error("Logout error:", error);
        await logAuthEvent({
            req,
            userId: req.user?.userId || null,
            actionType: "LOGOUT",
            status: "FAILED",
            message: "Logout failed due to internal error",
            errorMessage: error.message,
        });
        res.status(500).json({ message: "Internal server error" });
    }
}

//send reset password email
const forgotPassword = async (req, res) => {
  let client;

    try {
        client = await pool.connect();

        const { email } = req.body;

        if (!email) {
            await logAuthEvent({
                req,
                actionType: "FORGOT_PASSWORD",
                status: "FAILED",
                message: "Forgot password request failed due to missing email",
                errorMessage: "Email is required",
            });
            return res.status(400).json({ message: "Email is required" });
        }

        const normalizedEmail = email.trim().toLowerCase();

        const userResult = await client.query(
            `SELECT id FROM users WHERE email = $1`,
            [normalizedEmail]
        );

        // Dont reveal if user exists
        if (userResult.rows.length === 0) {
            await logAuthEvent({
                req,
                userEmail: normalizedEmail,
                actionType: "FORGOT_PASSWORD",
                status: "SUCCESS",
                message: "Forgot password request accepted for unknown email",
                metadata: { emailExists: false },
            });
            return res.json({
                success: true,
                message: "If account exists, reset link sent",
            });
        }

        const userId = userResult.rows[0].id;

        // Generate token
        const token = crypto.randomBytes(32).toString("hex");

        const hashedToken = crypto.createHash("sha256").update(token).digest("hex");

        const expiresAt = new Date(Date.now() + 15 * 60 * 1000); // 15 min

        //delete old tokens
        await client.query(
            `DELETE FROM password_reset_tokens WHERE user_id = $1`,
            [userId]
        );

        await client.query(
            `INSERT INTO password_reset_tokens (user_id, token_hash, expires_at)
            VALUES ($1, $2, $3)`,
            [userId, hashedToken, expiresAt]
        );

        await enqueueEmailJob({ type: "RESET_PASSWORD", email: normalizedEmail, token: token });
        await logAuthEvent({
            req,
            userId,
            userEmail: normalizedEmail,
            roleAtTime: "OWNER",
            actionType: "FORGOT_PASSWORD",
            status: "SUCCESS",
            message: "Forgot password email queued",
            resourceId: userId,
            metadata: { emailExists: true },
        });

        return res.json({
            success: true,
            message: "If account exists, reset link sent",
        });

    } catch (error) {
        console.error("Forgot password error:", error);
        await logAuthEvent({
            req,
            userEmail: req.body?.email?.trim()?.toLowerCase() || null,
            actionType: "FORGOT_PASSWORD",
            status: "FAILED",
            message: "Forgot password failed due to internal error",
            errorMessage: error.message,
        });
        return res.status(500).json({ message: "Internal server error" });

    } finally {
        if (client) client.release();
    }
};

//reset password
const resetPassword = async (req, res) => {
  let client;

    try {
        client = await pool.connect();

        const { token, newPassword } = req.body;

        if (!token || !newPassword) {
            await logAuthEvent({
                req,
                actionType: "RESET_PASSWORD",
                status: "FAILED",
                message: "Password reset failed due to missing token or password",
                errorMessage: "Token and new password required",
            });
            return res.status(400).json({
                message: "Token and new password required",
            });
        }

        const hashedToken = crypto.createHash("sha256").update(token).digest("hex");

        // Find token
        const result = await client.query(
            `SELECT user_id, expires_at 
            FROM password_reset_tokens 
            WHERE token_hash = $1`,
            [hashedToken]
        );

        if (result.rows.length === 0) {
            await logAuthEvent({
                req,
                actionType: "RESET_PASSWORD",
                status: "DENIED",
                message: "Password reset denied due to invalid token",
                errorMessage: "Invalid token",
            });
            return res.status(400).json({ message: "Invalid token" });
        }

        const { user_id, expires_at } = result.rows[0];

        // Check expiry
        if (new Date(expires_at) < new Date()) {
            await logAuthEvent({
                req,
                userId: user_id,
                roleAtTime: "OWNER",
                actionType: "RESET_PASSWORD",
                status: "DENIED",
                message: "Password reset denied due to expired token",
                errorMessage: "Token expired",
                resourceId: user_id,
            });
            return res.status(400).json({ message: "Token expired" });
        }

        await client.query("BEGIN");

        const hashedPassword = await bcrypt.hash(newPassword, 10);

        //  Update password
        await client.query(
            `UPDATE users SET password_hash = $1 WHERE id = $2`,
            [hashedPassword, user_id]
        );

        // Delete reset tokens
        await client.query(
            `DELETE FROM password_reset_tokens WHERE user_id = $1`,
            [user_id]
        );

        // logout everywhere
        await client.query(
            `UPDATE refresh_tokens SET revoked = true WHERE user_id = $1`,
            [user_id]
        );

        await client.query("COMMIT");
        await logAuthEvent({
            req,
            userId: user_id,
            roleAtTime: "OWNER",
            actionType: "RESET_PASSWORD",
            status: "SUCCESS",
            message: "Password reset successful",
            resourceId: user_id,
        });

        return res.json({
            success: true,
            message: "Password reset successful",
        });

    } catch (error) {
        if (client) await client.query("ROLLBACK");

        console.error("Reset password error:", error);
        await logAuthEvent({
            req,
            actionType: "RESET_PASSWORD",
            status: "FAILED",
            message: "Password reset failed due to internal error",
            errorMessage: error.message,
        });

        return res.status(500).json({
        message: "Internal server error",
        });

    } finally {
        if (client) client.release();
    }
};

const me = async (req, res) => {
    try {
        res.json({ 
            success: true,
            data: {
                userId: req.user.userId
            }
        });
    } catch (error) {
        console.error("Me error:", error);
        res.status(500).json({ success: false, message: "Internal server error" });
    }
}

const getUserInfo = async (req, res) => {
    try {
        const { rows } = await pool.query(`
            SELECT id, name, email
            FROM users
            WHERE id = $1
        `, [req.user.userId]);

        if (rows.length === 0) {
            return res.status(404).json({ success: false, message: "User not found" });
        }

        const user = rows[0];
        res.json({ success: true, data: user });
    } catch (error) {
        console.error("user info error", error);
        res.status(500).json({ success: false, message: "Internal server error" });
    }
}

module.exports = { registerUser, loginUser, verifyEmail, refreshTokenHandler, logoutUser, forgotPassword, resetPassword, me, getUserInfo }
