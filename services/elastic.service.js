const { Client } = require('@elastic/elasticsearch');

const esClient = new Client({
  node: 'http://localhost:9200', // Elasticsearch URL
});

async function createPlanIndex() {
  const indexExists = await esClient.indices.exists({ index: 'plans' });

  if (!indexExists) {
    await esClient.indices.create({
        index: 'plans',
        body: {
          mappings: {
            properties: {
              joinField: {
                type: 'join',
                relations: {
                  plan: 'linkedPlanService'
                }
              },
              objectId: { type: 'keyword' },
              objectType: { type: 'keyword' },
              _org: { type: 'keyword' },
              planType: { type: 'keyword' },
              creationDate: { type: 'date', format: "dd-MM-yyyy" },
              copay: { type: 'integer' },
              deductible: { type: 'integer' },
              name: { type: 'text' }
            }
          }
        }
      });
      

    console.log("Elasticsearch 'plans' index with parent-child mapping created.");
  } else {
    console.log("Index already exists.");
  }
}

module.exports = {
  esClient,
  createPlanIndex
};
