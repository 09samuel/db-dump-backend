const jwt = require("jsonwebtoken");
const { pool } = require("../db/index");
const { insertAuditLog, getRequestMeta } = require("../utils/auditLogger");

const authenticate = (req, res, next) => {
    const token = req.cookies.accessToken;
    const requestMeta = getRequestMeta(req);

    if (!token) {
        insertAuditLog({
            roleAtTime: "SYSTEM",
            actionType: "AUTHENTICATION_CHECK",
            actionCategory: "SECURITY",
            resourceType: "ROUTE",
            resourceName: req.originalUrl,
            message: "Authentication denied due to missing access token",
            status: "DENIED",
            errorMessage: "Unauthorized",
            ipAddress: requestMeta.ipAddress,
            userAgent: requestMeta.userAgent,
            metadata: { method: req.method },
        });
        return res.status(401).json({ message: "Unauthorized" });
    }

    try {
        const decoded = jwt.verify(token, process.env.ACCESS_TOKEN_SECRET);
        req.user = decoded;
        next();
    } catch (err) {
        insertAuditLog({
            roleAtTime: "SYSTEM",
            actionType: "AUTHENTICATION_CHECK",
            actionCategory: "SECURITY",
            resourceType: "ROUTE",
            resourceName: req.originalUrl,
            message: "Authentication denied due to invalid access token",
            status: "DENIED",
            errorMessage: "Invalid token",
            ipAddress: requestMeta.ipAddress,
            userAgent: requestMeta.userAgent,
            metadata: { method: req.method },
        });
        return res.status(401).json({ message: "Invalid token" });
    }
};

const checkPermission = (requiredPermission) => {
    return async (req, res, next) => {
        try {
        const userId = req.user.userId;
        const connectionId = req.params.connectionId || req.body.connectionId;
        const requestMeta = getRequestMeta(req);
        
        console.log("Checking permission for user:", userId, "on connection:", connectionId, "for permission:", requiredPermission);

        const result = await pool.query(
            `SELECT role 
            FROM user_connection_roles
            WHERE user_id = $1 AND connection_id = $2`,
            [userId, connectionId]
        );

        console.log("Permission check result:", result.rows);

        if (result.rows.length === 0) {
                await insertAuditLog({
                userId,
                roleAtTime: "SYSTEM",
                actionType: "PERMISSION_CHECK",
                actionCategory: "SECURITY",
                resourceType: "CONNECTION",
                resourceId: connectionId || null,
                resourceName: req.originalUrl,
                message: "Permission denied: user has no access to connection",
                status: "DENIED",
                errorMessage: "No access to this connection",
                ipAddress: requestMeta.ipAddress,
                userAgent: requestMeta.userAgent,
                metadata: { requiredPermission },
            });
                return res.status(403).json({
                message: "No access to this connection",
            });
        }

        const role = result.rows[0].role;
        const permissions = ROLE_PERMISSIONS[role];

        if ( permissions.includes("*") || permissions.includes(requiredPermission)) {
            req.userRole = role;
            return next();
        }

        await insertAuditLog({
            userId,
            roleAtTime: role || "SYSTEM",
            actionType: "PERMISSION_CHECK",
            actionCategory: "SECURITY",
            resourceType: "CONNECTION",
            resourceId: connectionId || null,
            resourceName: req.originalUrl,
            message: "Permission denied for required action",
            status: "DENIED",
            errorMessage: "Forbidden",
            ipAddress: requestMeta.ipAddress,
            userAgent: requestMeta.userAgent,
            metadata: { requiredPermission },
        });

        return res.status(403).json({ message: "Forbidden" });

        } catch (err) {
            console.error("Permission error:", err);
            await insertAuditLog({
                userId: req.user?.userId || null,
                roleAtTime: "SYSTEM",
                actionType: "PERMISSION_CHECK",
                actionCategory: "SECURITY",
                resourceType: "ROUTE",
                resourceName: req.originalUrl,
                message: "Permission check failed due to internal error",
                status: "FAILED",
                errorMessage: err.message,
                ipAddress: req.ip || null,
                userAgent: req.headers?.["user-agent"] || null,
                metadata: { requiredPermission },
            });
            return res.status(500).json({ message: "Internal server error" });
        }
    };
};

const attachConnectionIdFromBackup = async (req, res, next) => {
    try {
        const { backupId } = req.params;

        // Query to find the connection_id linked to this specific backup
        const result = await pool.query(
            `SELECT connection_id FROM backups WHERE id = $1`,
            [backupId]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ message: "Backup not found" });
        }

        // Attach it to req.params so checkPermission can see it
        req.params.connectionId = result.rows[0].connection_id;
        next();
    } catch (err) {
        console.error("Discovery error:", err);
        res.status(500).json({ message: "Internal server error" });
    }
};

const ROLE_PERMISSIONS = {
    OWNER: ["*"],

    ADMIN: [
        "connection:create",
        "connection:update", //includes backup settings update
        "connection:delete",
        "backup:execute",
        "backup:read",
        "audit:read",
        "restore:execute",
        "user:manage"
    ],

    OPERATOR: [
        "connection:read",
        "backup:execute",
        "backup:read",
        "audit:read",
        "restore:execute"
    ],

    VIEWER: [
        "connection:read",
        "backup:read",
        "audit:read"
    ]
};

module.exports = { authenticate, checkPermission, attachConnectionIdFromBackup };
