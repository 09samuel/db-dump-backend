const jwt = require("jsonwebtoken");

const authenticate = (req, res, next) => {
    const token = req.cookies.accessToken;

    if (!token) {
        return res.status(401).json({ message: "Unauthorized" });
    }

    try {
        const decoded = jwt.verify(token, process.env.ACCESS_TOKEN_SECRET);
        req.user = decoded;
        next();
    } catch (err) {
        return res.status(401).json({ message: "Invalid token" });
    }
};

const checkPermission = (requiredPermission) => {
    return async (req, res, next) => {
        try {
        const userId = req.user.userId;
        const connectionId = req.params.connectionId;

        const result = await pool.query(
            `SELECT role 
            FROM user_connection_roles
            WHERE user_id = $1 AND connection_id = $2`,
            [userId, connectionId]
        );

        if (result.rows.length === 0) {
                return res.status(403).json({
                message: "No access to this connection",
            });
        }

        const role = result.rows[0].role;
        const permissions = ROLE_PERMISSIONS[role];

        if ( permissions.includes("*") || permissions.includes(requiredPermission)) {
            return next();
        }

        return res.status(403).json({ message: "Forbidden" });

        } catch (err) {
            console.error("Permission error:", err);
            return res.status(500).json({ message: "Internal server error" });
        }
    };
};

const ROLE_PERMISSIONS = {
    OWNER: ["*"],

    ADMIN: [
        "connection:create",
        "connection:update",
        "connection:delete",
        "backup:execute",
        "backup:read",
        "restore:execute",
        "user:manage"
    ],

    OPERATOR: [
        "connection:read",
        "backup:execute",
        "backup:read",
        "restore:execute"
    ],

    VIEWER: [
        "connection:read",
        "backup:read"
    ]
};

module.exports = { authenticate, checkPermission };
