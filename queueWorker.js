// const amqp = require("amqplib");
// const { esClient } = require("./services/elastic.service");
// const { QUEUE_NAME } = require("./services/rabbit.service");

// (async () => {
//   try {
//     const connection = await amqp.connect("amqp://localhost");
//     const channel = await connection.createChannel();

//     await channel.assertQueue(QUEUE_NAME, { durable: true });
//     console.log("Waiting for messages in queue:", QUEUE_NAME);

//     channel.consume(QUEUE_NAME, async (msg) => {
//       if (msg !== null) {
//         try {
//           const job = JSON.parse(msg.content.toString());
//           console.log("Received job:", job); 

//           switch (job.type) {
//             case "indexPlan":
//               await esClient.index({
//                 index: "plans",
//                 id: job.payload.objectId,
//                 routing: job.payload.parentId,
//                 document: {
//                   ...job.payload,
//                   joinField: { name: "plan" },
//                   copay: job.payload.planCostShares?.copay || 0,
//                   deductible: job.payload.planCostShares?.deductible || 0
//                 }
//               });
//               break;

//             case "indexService":
//               await esClient.index({
//                 index: "plans",
//                 id: job.payload.objectId,
//                 routing: job.payload.parentId, // required for child docs
//                 document: {
//                   ...job.payload.data,
//                   joinField: {
//                     name: "linkedPlanService",
//                     parent: job.payload.parentId
//                   },
//                   copay: job.payload.data.planserviceCostShares?.copay || 0,
//                   deductible: job.payload.data.planserviceCostShares?.deductible || 0
//                 }
//               });
//               break;

//             case "updatePlan":
//               await esClient.update({
//                 index: "plans",
//                 id: job.payload.objectId,
//                 doc: {
//                   ...job.payload,
//                   joinField: { name: "plan" },
//                   copay: job.payload.planCostShares?.copay || 0,
//                   deductible: job.payload.planCostShares?.deductible || 0
//                 }
//               });
//               break;

//             case "updateService":
//               await esClient.update({
//                 index: "plans",
//                 id: job.payload.objectId,
//                 routing: job.payload.parentId,
//                 doc: {
//                   ...job.payload.data,
//                   joinField: {
//                     name: "linkedPlanService",
//                     parent: job.payload.parentId
//                   },
//                   copay: job.payload.data.planserviceCostShares?.copay || 0,
//                   deductible: job.payload.data.planserviceCostShares?.deductible || 0
//                 }
//               });
//               break;

//               case "deletePlan":
//                 try {
//                   await esClient.delete({
//                     index: "plans",
//                     id: job.objectId
//                   });
//                 } catch (err) {
//                   if (err.meta?.statusCode !== 404) throw err;
//                   console.warn(`Plan ${job.objectId} not found.`);
//                 }
              
//                 const childResults = await esClient.search({
//                   index: "plans",
//                   query: {
//                     term: { "joinField.parent": job.objectId }
//                   }
//                 });
              
//                 for (const hit of childResults.hits.hits) {
//                   try {
//                     await esClient.delete({
//                       index: "plans",
//                       id: hit._id,
//                       routing: job.objectId
//                     });
//                   } catch (err) {
//                     if (err.meta?.statusCode !== 404) throw err;
//                     console.warn(`Child ${hit._id} already deleted.`);
//                   }
//                 }
              
//             break;
//             case "deleteService":
//                 try {
//                   await esClient.delete({
//                     index: "plans",
//                     id: job.objectId,
//                     routing: job.parentId // required for child docs
//                   });
//                 } catch (err) {
//                   if (err.meta?.statusCode !== 404) throw err;
//                   console.warn(`Service ${job.objectId} not found.`);
//                 }
//             break;              
//             default:
//               console.warn(" Unknown job type:", job.type);
//           }

//           channel.ack(msg);
//         } catch (err) {
//           console.error("Error processing job:", err);
//           channel.nack(msg, false, false); //discard the message
//         }
//       }
//     });
//   } catch (err) {
//     console.error("Worker failed to connect or consume:", err);
//   }
// })();

// const amqp = require("amqplib");
// const { esClient } = require("./services/elastic.service");
// const { QUEUE_NAME } = require("./services/rabbit.service");

// (async () => {
//   try {
//     const connection = await amqp.connect("amqp://localhost");
//     const channel = await connection.createChannel();

//     await channel.assertQueue(QUEUE_NAME, { durable: true });
//     console.log("Waiting for messages in queue:", QUEUE_NAME);

//     channel.consume(QUEUE_NAME, async (msg) => {
//       if (!msg) return;

//       try {
//         const job = JSON.parse(msg.content.toString());
//         console.log("Received job:", job);

//         switch (job.type) {
//           case "indexPlan":
//             await esClient.index({
//               index: "plans",
//               id: job.payload.objectId,
//               document: {
//                 ...job.payload,
//                 joinField: { name: "plan" },
//                 copay: job.payload.planCostShares?.copay || 0,
//                 deductible: job.payload.planCostShares?.deductible || 0
//               }
//             });

//             await esClient.indices.refresh({ index: "plans" }); // Ensure immediate availability
//             console.log(`✅ Indexed parent plan: ${job.payload.objectId}`);
//             break;

//           case "indexService":
//             const parentId = job.payload.parentId;
//             let parentFound = false;

//             for (let i = 0; i < 5; i++) {
//               const exists = await esClient.exists({ index: "plans", id: parentId });
//               if (exists) {
//                 parentFound = true;
//                 break;
//               }
//               await new Promise((resolve) => setTimeout(resolve, 300)); // Wait before retry
//             }

//             if (!parentFound) {
//               console.warn(`❌ Parent ${parentId} still not found in Elasticsearch. Requeuing child ${job.payload.objectId}`);
//               return channel.nack(msg, false, true); // Requeue the message
//             }

//             await esClient.index({
//               index: "plans",
//               id: job.payload.objectId,
//               routing: parentId,
//               document: {
//                 ...job.payload.data,
//                 joinField: {
//                   name: "linkedPlanService",
//                   parent: parentId
//                 },
//                 copay: job.payload.data.planserviceCostShares?.copay || 0,
//                 deductible: job.payload.data.planserviceCostShares?.deductible || 0
//               }
//             });

//             console.log(`✅ Indexed child service: ${job.payload.objectId}`);
//             break;
//         }

//         channel.ack(msg);
//       } catch (err) {
//         console.error("❌ Error processing job:", err);
//         channel.nack(msg, false, false); // Discard bad message
//       }
//     });
//   } catch (err) {
//     console.error("❌ Worker failed to connect or consume:", err);
//   }
// })();


const amqp = require("amqplib");
const { esClient } = require("./services/elastic.service");
const { QUEUE_NAME } = require("./services/rabbit.service");

(async () => {
  try {
    const connection = await amqp.connect("amqp://localhost");
    const channel = await connection.createChannel();

    await channel.assertQueue(QUEUE_NAME, { durable: true });
    console.log("Waiting for messages in queue:", QUEUE_NAME);

    channel.consume(QUEUE_NAME, async (msg) => {
      if (!msg) return;

      try {
        const job = JSON.parse(msg.content.toString());
        console.log("Received job:", job);

        switch (job.type) {
          case "indexPlan":
            await esClient.index({
              index: "plans",
              id: job.payload.objectId,
              document: {
                ...job.payload,
                joinField: { name: "plan" },
                copay: job.payload.planCostShares?.copay || 0,
                deductible: job.payload.planCostShares?.deductible || 0
              }
            });
            await esClient.indices.refresh({ index: "plans" });
            console.log(`✅ Indexed parent plan: ${job.payload.objectId}`);
            break;

          case "indexService":
            const parentId = job.payload.parentId;
            let parentFound = false;

            for (let i = 0; i < 5; i++) {
              const exists = await esClient.exists({ index: "plans", id: parentId });
              if (exists) {
                parentFound = true;
                break;
              }
              await new Promise((resolve) => setTimeout(resolve, 300));
            }

            if (!parentFound) {
              console.warn(`❌ Parent ${parentId} still not found in Elasticsearch. Requeuing child ${job.payload.objectId}`);
              return channel.nack(msg, false, true);
            }

            await esClient.index({
              index: "plans",
              id: job.payload.objectId,
              routing: parentId,
              document: {
                ...job.payload.data,
                joinField: {
                  name: "linkedPlanService",
                  parent: parentId
                },
                copay: job.payload.data.planserviceCostShares?.copay || 0,
                deductible: job.payload.data.planserviceCostShares?.deductible || 0
              }
            });
            console.log(`✅ Indexed child service: ${job.payload.objectId}`);
            break;

          case "updatePlan":
            await esClient.update({
              index: "plans",
              id: job.payload.objectId,
              doc: {
                ...job.payload,
                joinField: { name: "plan" },
                copay: job.payload.planCostShares?.copay || 0,
                deductible: job.payload.planCostShares?.deductible || 0
              }
            });
            console.log(`✅ Updated parent plan: ${job.payload.objectId}`);
            break;

            case "updateService":
                try {
                  const exists = await esClient.exists({
                    index: "plans",
                    id: job.payload.objectId
                  });
              
                  if (!exists) {
                    console.warn(`Service ${job.payload.objectId} not found. Indexing as new.`);
                    await esClient.index({
                      index: "plans",
                      id: job.payload.objectId,
                      routing: job.payload.parentId,
                      document: {
                        ...job.payload.data,
                        joinField: {
                          name: "linkedPlanService",
                          parent: job.payload.parentId
                        },
                        copay: job.payload.data.planserviceCostShares?.copay || 0,
                        deductible: job.payload.data.planserviceCostShares?.deductible || 0
                      }
                    });
                  } else {
                    await esClient.update({
                      index: "plans",
                      id: job.payload.objectId,
                      routing: job.payload.parentId,
                      doc: {
                        ...job.payload.data,
                        joinField: {
                          name: "linkedPlanService",
                          parent: job.payload.parentId
                        },
                        copay: job.payload.data.planserviceCostShares?.copay || 0,
                        deductible: job.payload.data.planserviceCostShares?.deductible || 0
                      }
                    });
                  }
              
                  console.log(`✅ Upserted child service: ${job.payload.objectId}`);
                } catch (err) {
                  console.error("❌ Error upserting service:", err);
                }
                break;              

          case "deletePlan":
            try {
              await esClient.delete({
                index: "plans",
                id: job.objectId
              });
              console.log(`🗑️ Deleted parent plan: ${job.objectId}`);
            } catch (err) {
              if (err.meta?.statusCode !== 404) throw err;
              console.warn(`Plan ${job.objectId} not found.`);
            }

            const childResults = await esClient.search({
              index: "plans",
              query: {
                term: { "joinField.parent": job.objectId }
              }
            });

            for (const hit of childResults.hits.hits) {
              try {
                await esClient.delete({
                  index: "plans",
                  id: hit._id,
                  routing: job.objectId
                });
                console.log(`🗑️ Deleted child: ${hit._id}`);
              } catch (err) {
                if (err.meta?.statusCode !== 404) throw err;
                console.warn(`Child ${hit._id} already deleted.`);
              }
            }
            break;

            case "deleteService":
                try {
                  await esClient.delete({
                    index: "plans",
                    id: job.objectId,
                    routing: job.parentId
                  });
              
                  // 🔁 Update parent doc to remove this service from linkedPlanServices
                  const parent = await esClient.get({
                    index: "plans",
                    id: job.parentId
                  });
              
                  const updatedServices = (parent._source.linkedPlanServices || []).filter(
                    svc => svc.objectId !== job.objectId
                  );
              
                  await esClient.update({
                    index: "plans",
                    id: job.parentId,
                    doc: {
                      linkedPlanServices: updatedServices
                    }
                  });
              
                  console.log(`✅ Deleted child ${job.objectId} and updated parent ${job.parentId}`);
                } catch (err) {
                  if (err.meta?.statusCode !== 404) throw err;
                  console.warn(`Service ${job.objectId} not found.`);
                }
                break;
              

          default:
            console.warn("Unknown job type:", job.type);
        }

        channel.ack(msg);
      } catch (err) {
        console.error("❌ Error processing job:", err);
        channel.nack(msg, false, false);
      }
    });
  } catch (err) {
    console.error("❌ Worker failed to connect or consume:", err);
  }
})();
