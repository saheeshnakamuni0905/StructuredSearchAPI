const { createPlanIndex } = require("./services/elastic.service");

(async () => {
  try {
    await createPlanIndex();
    console.log("Elasticsearch index created.");
  } catch (error) {
    console.error("Failed to create index:", error);
  } finally {
    process.exit();
  }
})();
