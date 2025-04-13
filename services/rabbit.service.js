const amqp = require("amqplib");

let channel = null;
let connection = null;
const QUEUE_NAME = "plan-index-queue";

// Connect to RabbitMQ
async function connectRabbitMQ() {
  try {
    connection = await amqp.connect("amqp://localhost");
    channel = await connection.createChannel();
    await channel.assertQueue(QUEUE_NAME, { durable: true });

    console.log("Connected to RabbitMQ and queue declared.");
  } catch (error) {
    console.error("Failed to connect to RabbitMQ:", error);
    throw error;
  }
}

// Send data to the queue
async function sendToQueue(data) {
  if (!channel) {
    console.warn("RabbitMQ channel not ready, reconnecting...");
    await connectRabbitMQ();
  }

  channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(data)), {
    persistent: true
  });
}

module.exports = {
  connectRabbitMQ,
  sendToQueue,
  QUEUE_NAME
};
