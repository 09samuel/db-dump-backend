const { pool } = require("../db/index");

const allowedRoles = ["OWNER", "ADMIN", "OPERATOR", "VIEWER"];


// Add collaborator
const addCollaborator = async (req, res) => {
    const { email, role } = req.body;
    const { connectionId } = req.params;

    if (!email || !role) {
        return res.status(400).json({ message: "Email and role are required" });
    }

    if (!allowedRoles.includes(role)) {
        return res.status(400).json({ message: "Invalid role" });
    }

    try {
        // Find user by email
        const userResult = await pool.query(
            `SELECT id FROM users WHERE email = $1`,
            [email]
        );

        if (userResult.rows.length === 0) {
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

        res.status(201).json({ message: "Collaborator added/updated" });

    } catch (err) {
        console.error(err);
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
            return res.status(404).json({ message: "Collaborator not found" });
        }

        const role = result.rows[0].role;

        // Prevent removing OWNER
        if (role === "OWNER") {
            return res.status(400).json({ message: "Cannot remove owner" });
        }

        // Delete
        await pool.query(
            `DELETE FROM user_connection_roles
            WHERE user_id = $1 AND connection_id = $2`,
            [userId, connectionId]
        );

        res.status(200).json({ message: "Collaborator removed" });

    } catch (err) {
        console.error(err);
        res.status(500).json({ message: "Error removing collaborator" });
    }
};


// Get collaborators
const getCollaborators = async (req, res) => {
    const { connectionId } = req.params;

    try {
        const result = await pool.query(
            `SELECT u.id, u.email, ucr.role
            FROM user_connection_roles ucr
            JOIN users u ON u.id = ucr.user_id
            WHERE ucr.connection_id = $1`,
            [connectionId]
        );

        res.status(200).json(result.rows);

    } catch (err) {
        console.error(err);
        res.status(500).json({ message: "Error fetching collaborators" });
    }
};


// Update collaborator role
const updateRole = async (req, res) => {
    const { connectionId, userId } = req.params;
    const { role } = req.body;

    if (!allowedRoles.includes(role)) {
        return res.status(400).json({ message: "Invalid role" });
    }

    try {
        //Prevent self role change
        if (req.user.userId === userId) {
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
            return res.status(404).json({ message: "Collaborator not found" });
        }

        const currentRole = result.rows[0].role;

        // Prevent changing OWNER
        if (currentRole === "OWNER") {
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

        res.status(200).json({ message: "Role updated" });

    } catch (err) {
        console.error(err);
        res.status(500).json({ message: "Error updating role" });
    }
};

module.exports = { addCollaborator, removeCollaborator, getCollaborators, updateRole };