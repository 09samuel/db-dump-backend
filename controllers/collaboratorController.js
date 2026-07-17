const { pool } = require("../db/index");
const logger = require("../utils/logger");
const { insertAuditLog, getRequestMeta } = require("../utils/auditLogger");

const allowedRoles = ["OWNER", "ADMIN", "OPERATOR", "VIEWER"];

async function logUserManagementEvent({
    req,
    userId = null,
    roleAtTime = "SYSTEM",
    actionType,
    status,
    message,
    resourceId = null,
    errorMessage = null,
    metadata = {},
}) {
    const requestMeta = getRequestMeta(req);

    await insertAuditLog({
        userId,
        roleAtTime,
        actionType,
        actionCategory: "USER_MANAGEMENT",
        resourceType: "CONNECTION",
        resourceId,
        message,
        status,
        errorMessage,
        metadata,
        ipAddress: requestMeta.ipAddress,
        userAgent: requestMeta.userAgent,
    });
}


// Add collaborator
const addCollaborator = async (req, res) => {
    const { email, role } = req.body;
    const { connectionId } = req.params;

    if (!email || !role) {
        await logUserManagementEvent({
            req,
            userId: req.user?.userId || null,
            roleAtTime: req.userRole || "SYSTEM",
            actionType: "COLLABORATOR_ADD",
            status: "FAILED",
            message: "Add collaborator failed due to missing email or role",
            resourceId: connectionId,
            errorMessage: "Email and role are required",
        });
        return res.status(400).json({ message: "Email and role are required" });
    }

    if (!allowedRoles.includes(role)) {
        await logUserManagementEvent({
            req,
            userId: req.user?.userId || null,
            roleAtTime: req.userRole || "SYSTEM",
            actionType: "COLLABORATOR_ADD",
            status: "FAILED",
            message: "Add collaborator failed due to invalid role",
            resourceId: connectionId,
            errorMessage: "Invalid role",
            metadata: { requestedRole: role },
        });
        return res.status(400).json({ message: "Invalid role" });
    }

    try {
        // Find user by email
        const userResult = await pool.query(
            `SELECT id FROM users WHERE email = $1`,
            [email]
        );

        if (userResult.rows.length === 0) {
            await logUserManagementEvent({
                req,
                userId: req.user?.userId || null,
                roleAtTime: req.userRole || "SYSTEM",
                actionType: "COLLABORATOR_ADD",
                status: "FAILED",
                message: "Add collaborator failed because target user was not found",
                resourceId: connectionId,
                errorMessage: "User not found",
                metadata: { collaboratorEmail: email },
            });
            return res.status(404).json({ message: "User not found" });
        }

        const userId = userResult.rows[0].id;

        // Insert collaborator
        await pool.query(
            `INSERT INTO user_connection_roles (user_id, connection_id, role)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id, connection_id) DO UPDATE SET role = EXCLUDED.role`,
            [userId, connectionId, role]
        );

        await logUserManagementEvent({
            req,
            userId: req.user?.userId || null,
            roleAtTime: req.userRole || "SYSTEM",
            actionType: "COLLABORATOR_ADD",
            status: "SUCCESS",
            message: "Collaborator added or updated successfully",
            resourceId: connectionId,
            metadata: { collaboratorUserId: userId, role },
        });

        res.status(201).json({ message: "Collaborator added/updated" });

    } catch (err) {
        logger.error("Error in addCollaborator:", err);
        await logUserManagementEvent({
            req,
            userId: req.user?.userId || null,
            roleAtTime: req.userRole || "SYSTEM",
            actionType: "COLLABORATOR_ADD",
            status: "FAILED",
            message: "Add collaborator failed due to internal error",
            resourceId: connectionId,
            errorMessage: err.message,
        });
        res.status(500).json({ message: "Error adding collaborator" });
    }
};


// Remove collaborator
const removeCollaborator = async (req, res) => {
    const { connectionId, userId } = req.params;

    try {
        // Get role
        const result = await pool.query(
            `SELECT role FROM user_connection_roles
            WHERE user_id = $1 AND connection_id = $2`,
            [userId, connectionId]
        );

        if (result.rows.length === 0) {
            await logUserManagementEvent({
                req,
                userId: req.user?.userId || null,
                roleAtTime: req.userRole || "SYSTEM",
                actionType: "COLLABORATOR_REMOVE",
                status: "FAILED",
                message: "Remove collaborator failed because collaborator was not found",
                resourceId: connectionId,
                errorMessage: "Collaborator not found",
                metadata: { collaboratorUserId: userId },
            });
            return res.status(404).json({ message: "Collaborator not found" });
        }

        const role = result.rows[0].role;

        // Prevent removing OWNER
        if (role === "OWNER") {
            await logUserManagementEvent({
                req,
                userId: req.user?.userId || null,
                roleAtTime: req.userRole || "SYSTEM",
                actionType: "COLLABORATOR_REMOVE",
                status: "DENIED",
                message: "Remove collaborator denied because target role is OWNER",
                resourceId: connectionId,
                errorMessage: "Cannot remove owner",
                metadata: { collaboratorUserId: userId },
            });
            return res.status(400).json({ message: "Cannot remove owner" });
        }

        // Delete
        await pool.query(
            `DELETE FROM user_connection_roles
            WHERE user_id = $1 AND connection_id = $2`,
            [userId, connectionId]
        );

        await logUserManagementEvent({
            req,
            userId: req.user?.userId || null,
            roleAtTime: req.userRole || "SYSTEM",
            actionType: "COLLABORATOR_REMOVE",
            status: "SUCCESS",
            message: "Collaborator removed successfully",
            resourceId: connectionId,
            metadata: { collaboratorUserId: userId },
        });

        res.status(200).json({ message: "Collaborator removed" });

    } catch (err) {
        logger.error("Error in removeCollaborator:", err);
        await logUserManagementEvent({
            req,
            userId: req.user?.userId || null,
            roleAtTime: req.userRole || "SYSTEM",
            actionType: "COLLABORATOR_REMOVE",
            status: "FAILED",
            message: "Remove collaborator failed due to internal error",
            resourceId: connectionId,
            errorMessage: err.message,
            metadata: { collaboratorUserId: userId },
        });
        res.status(500).json({ message: "Error removing collaborator" });
    }
};


// Get collaborators
const getCollaborators = async (req, res) => {
    const { connectionId } = req.params;

    try {
        const result = await pool.query(
            `SELECT u.id, u.name, u.email, ucr.role
            FROM user_connection_roles ucr
            JOIN users u ON u.id = ucr.user_id
            WHERE ucr.connection_id = $1`,
            [connectionId]
        );

        res.status(200).json(result.rows);

    } catch (err) {
        logger.error("Error in getCollaborators:", err);
        res.status(500).json({ message: "Error fetching collaborators" });
    }
};


// Update collaborator role
const updateRole = async (req, res) => {
    const { connectionId, userId } = req.params;
    const { role } = req.body;

    if (!allowedRoles.includes(role)) {
        await logUserManagementEvent({
            req,
            userId: req.user?.userId || null,
            roleAtTime: req.userRole || "SYSTEM",
            actionType: "COLLABORATOR_ROLE_UPDATE",
            status: "FAILED",
            message: "Role update failed due to invalid role",
            resourceId: connectionId,
            errorMessage: "Invalid role",
            metadata: { collaboratorUserId: userId, requestedRole: role },
        });
        return res.status(400).json({ message: "Invalid role" });
    }

    try {
        //Prevent self role change
        if (req.user.userId === userId) {
            await logUserManagementEvent({
                req,
                userId: req.user?.userId || null,
                roleAtTime: req.userRole || "SYSTEM",
                actionType: "COLLABORATOR_ROLE_UPDATE",
                status: "DENIED",
                message: "Role update denied because users cannot change their own role",
                resourceId: connectionId,
                errorMessage: "Cannot change your own role",
                metadata: { collaboratorUserId: userId, requestedRole: role },
            });
            return res.status(400).json({
                message: "Cannot change your own role"
            });
        }

        // Check current role
        const result = await pool.query(
            `SELECT role FROM user_connection_roles
            WHERE user_id = $1 AND connection_id = $2`,
            [userId, connectionId]
        );

        if (result.rows.length === 0) {
            await logUserManagementEvent({
                req,
                userId: req.user?.userId || null,
                roleAtTime: req.userRole || "SYSTEM",
                actionType: "COLLABORATOR_ROLE_UPDATE",
                status: "FAILED",
                message: "Role update failed because collaborator was not found",
                resourceId: connectionId,
                errorMessage: "Collaborator not found",
                metadata: { collaboratorUserId: userId, requestedRole: role },
            });
            return res.status(404).json({ message: "Collaborator not found" });
        }

        const currentRole = result.rows[0].role;

        // Prevent changing OWNER
        if (currentRole === "OWNER") {
            await logUserManagementEvent({
                req,
                userId: req.user?.userId || null,
                roleAtTime: req.userRole || "SYSTEM",
                actionType: "COLLABORATOR_ROLE_UPDATE",
                status: "DENIED",
                message: "Role update denied because OWNER role cannot be changed",
                resourceId: connectionId,
                errorMessage: "Cannot change owner role",
                metadata: { collaboratorUserId: userId, requestedRole: role },
            });
            return res.status(400).json({
                message: "Cannot change owner role"
            });
        }

        // Update
        await pool.query(
            `UPDATE user_connection_roles
            SET role = $1
            WHERE user_id = $2 AND connection_id = $3`,
            [role, userId, connectionId]
        );

        await logUserManagementEvent({
            req,
            userId: req.user?.userId || null,
            roleAtTime: req.userRole || "SYSTEM",
            actionType: "COLLABORATOR_ROLE_UPDATE",
            status: "SUCCESS",
            message: "Collaborator role updated successfully",
            resourceId: connectionId,
            metadata: { collaboratorUserId: userId, role },
        });

        res.status(200).json({ message: "Role updated" });

    } catch (err) {
        logger.error("Error in updateRole:", err);
        await logUserManagementEvent({
            req,
            userId: req.user?.userId || null,
            roleAtTime: req.userRole || "SYSTEM",
            actionType: "COLLABORATOR_ROLE_UPDATE",
            status: "FAILED",
            message: "Role update failed due to internal error",
            resourceId: connectionId,
            errorMessage: err.message,
            metadata: { collaboratorUserId: userId, requestedRole: role },
        });
        res.status(500).json({ message: "Error updating role" });
    }
};

module.exports = { addCollaborator, removeCollaborator, getCollaborators, updateRole };
