const express = require("express");
const router = express.Router();
const redis = require("redis");
const crypto = require("crypto");
const Ajv = require("ajv");
const addFormats = require("ajv-formats");
const authenticate = require("../middlewares/authMiddleware");
const { esClient } = require("../services/elastic.service");
const { sendToQueue } = require("../services/rabbit.service");

const redisClient = redis.createClient({
    socket: {
        host: "127.0.0.1",
        port: 6379
    }
});

redisClient.connect()
    .then(() => console.log("Redis Connected Successfully"))
    .catch(err => console.error("Redis Connection Error:", err));

redisClient.on("error", (err) => {
    console.error(" Redis Error:", err);
});

const ajv = new Ajv();
addFormats(ajv);

// JSON Schema Validation
const dataSchema = {
    type: "object",
    properties: {
        planCostShares: {
            type: "object",
            properties: {
                deductible: { type: "integer" },
                _org: { type: "string" },
                copay: { type: "integer" },
                objectId: { type: "string" },
                objectType: { type: "string" }
            },
            required: ["deductible", "_org", "copay", "objectId", "objectType"]
        },
        linkedPlanServices: { type: "array" },
        _org: { type: "string" },
        objectId: { type: "string" },
        objectType: { type: "string" },
        planType: { type: "string" },
        creationDate: { type: "string" }
    },
    required: ["planCostShares", "linkedPlanServices", "_org", "objectId", "objectType", "planType", "creationDate"]
};

// Helper function to generate ETag (Hash of JSON)
function generateEtag(data) {
    return `"${crypto.createHash("md5").update(JSON.stringify(data)).digest("hex")}"`;
}

// POST: Create a New Plan
router.post("/", authenticate, async (req, res) => {
    try {
      const validate = ajv.compile(dataSchema);
      if (!validate(req.body)) {
        return res.status(400).json({ error: "Invalid data", details: validate.errors });
      }
  
      if (!req.body.objectId) {
        return res.status(400).json({ error: "Missing required field: objectId" });
      }
  
      const { objectId } = req.body;
  
      if (await redisClient.exists(objectId)) {
        return res.status(409).json({ error: "Resource already exists" });
      }
  
      const etag = generateEtag(req.body);
      const storeData = { data: req.body, etag };
      await redisClient.set(objectId, JSON.stringify(storeData));
  
      // Index the parent plan
      // Replace esClient.index(...) with this:
await sendToQueue({
    type: "indexPlan",
    payload: req.body
  });
  
  // Send each child to queue
  for (const service of req.body.linkedPlanServices) {
    await sendToQueue({
      type: "indexService",
      payload: {
        parentId: req.body.objectId,
        data: service,
        objectId: service.objectId
      }
    });  
      }
  
      res.setHeader("ETag", etag);
      res.status(201).json({ message: "Resource created and indexed", etag, plan: req.body });
    } catch (error) {
      console.error("Error in POST /v1/plan:", error);
      res.status(500).json({ error: "Internal Server Error" });
    }
  });
  

// GET: Retrieve Data with Conditional Fetch
router.get("/:id", authenticate, async (req, res) => {
    try {
        const { id } = req.params;
        const storedData = await redisClient.get(id);

        if (!storedData) {
            return res.status(404).json({ error: "Resource not found" });
        }

        const { data, etag: storedEtag } = JSON.parse(storedData);
        const clientEtag = req.headers["if-none-match"];

        if (clientEtag && clientEtag === storedEtag) {
            return res.status(304).send();
        }

        res.setHeader("ETag", storedEtag);
        res.json(data);
    } catch (error) {
        console.error(" Error in GET /:id", error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

router.patch("/:id", authenticate, async (req, res) => {
    try {
      const storedData = await redisClient.get(req.params.id);
      if (!storedData) return res.status(404).json({ error: "Resource not found" });
  
      const { data, etag: storedEtag } = JSON.parse(storedData);
      const ifMatchHeader = req.headers["if-match"] ? req.headers["if-match"].trim() : null;
  
      if (!ifMatchHeader) {
        return res.status(428).json({ error: "Precondition Required: If-Match header is missing" });
      }
  
      if (ifMatchHeader !== storedEtag) {
        return res.status(412).json({ error: "Precondition Failed: ETag mismatch" });
      }
  
      const validatePatch = ajv.compile({
        type: "object",
        properties: {
          planCostShares: { type: "object" },
          linkedPlanServices: { type: "array" },
          _org: { type: "string" },
          objectId: { type: "string" },
          objectType: { type: "string" },
          planType: { type: "string" },
          creationDate: { type: "string" }
        },
        additionalProperties: false
      });
  
      if (!validatePatch(req.body)) {
        return res.status(400).json({ error: "Invalid patch data", details: validatePatch.errors });
      }
  
      let updatedData = { ...data, ...req.body };
  
      if (req.body.linkedPlanServices) {
        let existingServices = Array.isArray(data.linkedPlanServices) ? data.linkedPlanServices : [];
  
        req.body.linkedPlanServices.forEach(newService => {
          if (!newService.objectId) {
            return res.status(400).json({ error: "Each linkedPlanService must have an objectId" });
          }
  
          const index = existingServices.findIndex(service => service.objectId === newService.objectId);
          if (index >= 0) {
            existingServices[index] = { ...existingServices[index], ...newService };
          } else {
            existingServices.push(newService);
          }
        });
  
        updatedData.linkedPlanServices = existingServices;
      }
  
      const validateFinal = ajv.compile(dataSchema);
      if (!validateFinal(updatedData)) {
        return res.status(400).json({ error: "Invalid merged data", details: validateFinal.errors });
      }
  
      const newEtag = generateEtag(updatedData);
      await redisClient.set(req.params.id, JSON.stringify({ data: updatedData, etag: newEtag }));
  
      // Elasticsearch: update parent
      await sendToQueue({
        type: "updatePlan",
        payload: updatedData,
        objectId: req.params.id
      });
      
      if (req.body.linkedPlanServices) {
        for (const service of req.body.linkedPlanServices) {
          await sendToQueue({
            type: "updateService",
            payload: {
              parentId: req.params.id,
              data: service,
              objectId: service.objectId
            }
          });
        }
        }
      res.setHeader("ETag", newEtag);
      res.json({ message: "Resource updated", etag: newEtag, data: updatedData });
    } catch (error) {
      console.error("Error in PATCH /:id", error);
      res.status(500).json({ error: "Internal Server Error" });
    }
  });
  

// DELETE: Remove Data
router.delete("/:id", authenticate, async (req, res) => {
    try {
      const { id } = req.params;
  
      // Try fetching from Redis
      const storedData = await redisClient.get(id);
  
      if (storedData) {
        const parsed = JSON.parse(storedData);
  
        // Check if it's a parent (plan) by seeing if it has linkedPlanServices
        if (parsed.data?.linkedPlanServices) {
          const linkedServices = parsed.data.linkedPlanServices;
  
          // Delete parent from Redis
          await redisClient.del(id);
  
          // Send delete message for parent
          await sendToQueue({
            type: "deletePlan",
            objectId: id,
          });
  
          // Send delete messages for all children
          for (const service of linkedServices) {
            await sendToQueue({
              type: "deleteService",
              objectId: service.objectId,
              parentId: id,
            });
          }
  
          return res.status(204).send();
        }
      }
  
      // If not found in Redis, fallback to Elasticsearch
      const esResp = await esClient.get({
        index: "plans",
        id,
      }, { ignore: [404] });
  
      if (!esResp.found) {
        return res.status(404).json({ error: "Resource not found" });
      }
  
      const doc = esResp._source;
  
      // If it's a child (has joinField with parent)
      if (doc.joinField?.name === "linkedPlanService" && doc.joinField?.parent) {
        await sendToQueue({
          type: "deleteService",
          objectId: id,
          parentId: doc.joinField.parent,
        });
  
        return res.status(204).send();
      }
  
      // Else assume it's a plan without Redis cache (fallback mode)
      const searchResult = await esClient.search({
        index: "plans",
        query: {
          term: {
            "joinField.parent": id
          }
        }
      });
  
      // Delete the plan
      await sendToQueue({ type: "deletePlan", objectId: id });
  
      // Delete children if found
      for (const child of searchResult.hits.hits) {
        await sendToQueue({
          type: "deleteService",
          objectId: child._id,
          parentId: id
        });
      }
  
      return res.status(204).send();
    } catch (error) {
      console.error("Error in DELETE /:id", error);
      res.status(500).json({ error: "Internal Server Error" });
    }
  });
  
  
  
module.exports = router;
