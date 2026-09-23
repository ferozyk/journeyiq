require("dotenv").config();

const { Kafka, logLevel } = require("kafkajs");
const {
  SchemaRegistry,
  SchemaType,
} = require("@kafkajs/confluent-schema-registry");

const kafka = new Kafka({
  clientId: "journeyiq-simulator",
  brokers: [process.env.KAFKA_BROKERS],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_API_KEY,
    password: process.env.KAFKA_API_SECRET,
  },
  connectionTimeout: 10000,
  authenticationTimeout: 10000,
  logLevel: logLevel.INFO,
});

const producer = kafka.producer();

const registry = new SchemaRegistry({
  host: process.env.SCHEMA_REGISTRY_URL,
  auth: {
    username: process.env.SCHEMA_REGISTRY_API_KEY,
    password: process.env.SCHEMA_REGISTRY_API_SECRET,
  },
});

const sleep = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

function createEvent(customerId, eventType, metadata = {}) {
  return {
    event_id: `evt-${Date.now()}-${Math.random()
      .toString(36)
      .slice(2, 7)}`,
    customer_id: customerId,
    event_type: eventType,
    timestamp: new Date().toISOString(),
    metadata,
  };
}

async function publish(topic, event, schemaId) {
  const encodedValue = await registry.encode(schemaId, event);

  await producer.send({
    topic,
    messages: [
      {
        key: event.customer_id,
        value: encodedValue,
      },
    ],
  });

  console.log(`✓ ${topic}: ${event.event_type}`);
}

async function main() {
  await producer.connect();

  console.log("\n🚀 Starting JourneyIQ...\n");

  /*
   * We already registered these schemas in Confluent Cloud.
   *
   * Replace these values with the actual schema IDs shown
   * in Schema Registry.
   */

  const customerEventSchemaId = process.env.CUSTOMER_EVENT_SCHEMA_ID;
  const transactionSchemaId = process.env.TRANSACTION_SCHEMA_ID;

  const customerId = "C1001";

  // 1. Login
  await publish(
    "customer_events",
    createEvent(customerId, "LOGIN", {
      channel: "mobile_app",
    }),
    customerEventSchemaId
  );

  await sleep(1500);

  // 2. Travel card viewed
  await publish(
    "customer_events",
    createEvent(customerId, "PRODUCT_VIEW", {
      channel: "mobile_app",
      product: "TRAVEL_CARD",
    }),
    customerEventSchemaId
  );

  await sleep(1500);

  // 3. USD FX search
  await publish(
    "customer_events",
    createEvent(customerId, "FX_SEARCH", {
      channel: "mobile_app",
      currency: "USD",
    }),
    customerEventSchemaId
  );

  await sleep(1500);

  // 4. Airline purchase
  await publish(
    "transactions",
    {
      ...createEvent(customerId, "AIRLINE_PURCHASE", {
        merchant_category: "AIRLINE",
        merchant: "Emirates Airlines",
      }),
      amount: 4500,
      currency: "AED",
    },
    transactionSchemaId
  );

  await sleep(1500);

  await publish(
    "customer_events",
    createEvent("C1001", "AIRLINE_PURCHASE", {
      merchant_category: "AIRLINE",
      merchant: "Emirates Airlines",
      amount: 4500,
      currency: "AED",
    }),
    100009
  );

  // 5. Hotel search
  await publish(
    "customer_events",
    createEvent(customerId, "HOTEL_SEARCH", {
      channel: "mobile_app",
      destination: "New York",
    }),
    customerEventSchemaId
  );

  console.log("\n🎯 Journey simulation completed.\n");

  await producer.disconnect();
}

main().catch(async (error) => {
  console.error("Producer error:", error);

  try {
    await producer.disconnect();
  } catch {}

  process.exit(1);
});
