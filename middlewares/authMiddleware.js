const jwt = require("jsonwebtoken");
const jwksClient = require("jwks-rsa");
const status = require("http-status");
const dotenv = require("dotenv");

dotenv.config();

// ✅ Configure JWKS (JSON Web Key Set) client
const client = jwksClient({
    jwksUri: "https://www.googleapis.com/oauth2/v3/certs",
    cache: true,
    rateLimit: true
});

// ✅ Function to retrieve signing key
function getKey(header, callback) {
    client.getSigningKey(header.kid, (err, key) => {
        if (err) {
            return callback(err);
        }
        const signingKey = key.publicKey || key.rsaPublicKey;
        callback(null, signingKey);
    });
}

// ✅ Middleware to validate JWT
module.exports = async (req, res, next) => {
    try {
        let token = req.headers["authorization"];

        // ✅ Check if Authorization header is missing or incorrectly formatted
        if (!token || !token.startsWith("Bearer ")) {
            return res.status(401).json({
                status: "failed",
                message: "Unauthorized: Invalid or missing Authorization header"
            });
        }

        // ✅ Extract Bearer token
        token = token.split(" ")[1];

        // ✅ Verify the token
        jwt.verify(token, getKey, { algorithms: ["RS256"], audience: process.env.GOOGLE_CLIENT_ID }, (err, decoded) => {
            if (err) {
                if (err.name === "TokenExpiredError") {
                    return res.status(401).json({
                        status: "failed",
                        message: "Unauthorized: Token has expired"
                    });
                }
                return res.status(401).json({
                    status: "failed",
                    message: "Unauthorized: Invalid token"
                });
            }

            // ✅ Token is valid, attach user data
            req.user = decoded;
            next();
        });
    } catch (error) {
        console.error("❌ Authentication Error:", error);

        // ✅ Ensure a proper JSON response with 401 Unauthorized
        return res.status(401).json({
            status: "failed",
            message: "Unauthorized: Failed to validate user",
            error: error.message // Debugging details
        });
    }
};
