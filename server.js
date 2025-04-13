const express = require("express");
const path = require("path");
const bodyParser = require("body-parser");
const dotenv = require("dotenv");
const authenticate = require("./middlewares/authMiddleware");
const { createPlanIndex } = require("./services/elastic.service");

const { connectRabbitMQ } = require("./services/rabbit.service");

// Load environment variables
dotenv.config();

// Initialize Express App
const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, "public")));

connectRabbitMQ().catch(err => console.error("RabbitMQ Init Error:", err));
createPlanIndex().catch(err => {
    console.error("Failed to create Elasticsearch index:", err);
});

// Import Routes
const indexRouter = require("./routes/index");
const planRouter = require("./routes/planEndpoint");
const usersRouter = require("./routes/users");

// Use Routes
app.use("/", indexRouter);         // Handles home requests
//app.use("/v1/data", planRouter);      // Handles plan-related API



//app.use("/", indexRouter);
app.use("/v1/plan", authenticate, planRouter);  // Secure plan routes with JWT


// Error Handling Middleware
app.use((req, res, next) => {
    res.status(404).json({ error: "Route not found" });
});

// Start Server
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});
