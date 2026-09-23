require("dotenv").config();

const { Kafka, logLevel } = require("kafkajs");
const {
  SchemaRegistry,
} = require("@kafkajs/confluent-schema-registry");

const kafka = new Kafka({
  clientId: "journeyiq-dashboard",
  brokers: [process.env.KAFKA_BROKERS],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_API_KEY,
    password: process.env.KAFKA_API_SECRET,
  },
  logLevel: logLevel.INFO,
});

const registry = new SchemaRegistry({
  host: process.env.SCHEMA_REGISTRY_URL,
  auth: {
    username: process.env.SCHEMA_REGISTRY_API_KEY,
    password: process.env.SCHEMA_REGISTRY_API_SECRET,
  },
});

const consumer = kafka.consumer({
  groupId: "journeyiq-dashboard-v1",
});

async function startConsumer(onRecommendation) {

  await consumer.connect();

  await consumer.subscribe({
    topic: "journey_recommendations",
    fromBeginning: false,
  });

  console.log("✓ Listening to journey_recommendations");

  await consumer.run({
    eachMessage: async ({ message }) => {

      if (!message.value) {
        return;
      }

      try {

        const recommendation =
          await registry.decode(message.value);

        console.log(
          "→ Flink recommendation:",
          recommendation
        );

        onRecommendation(recommendation);

      } catch (error) {
        console.error(
          "Failed to decode recommendation:",
          error.message
        );
      }
    },
  });
}

module.exports = {
  startConsumer,
};