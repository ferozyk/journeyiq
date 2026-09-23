# JourneyIQ — Real-Time Customer Journey Intelligence

JourneyIQ is a real-time customer journey intelligence application built using the **Confluent Data Streaming Platform**.

It transforms individual customer events into an evolving customer journey, calculates intent in real time, and triggers a next-best action when the right signals come together.

For the current demo, JourneyIQ identifies an **international travel journey** and recommends a travel-card offer.

---

## The Problem

Traditional customer engagement systems often evaluate customer behaviour in batches or rely on isolated events.

A customer might:

- View a travel card
- Search for foreign exchange rates
- Purchase an airline ticket
- Search for a hotel

Each event by itself provides limited context.

The opportunity is to recognize that these events, when correlated in real time, represent a much stronger customer intent.

JourneyIQ demonstrates how streaming data can turn these individual events into an actionable customer journey.

---

## The Solution

JourneyIQ continuously processes customer events through Confluent Kafka and Flink.

The system:

1. Receives customer activity as streaming events.
2. Correlates events belonging to the same customer.
3. Detects a meaningful customer journey.
4. Calculates an intent score.
5. Determines a next-best action using deterministic rules.
6. Publishes the recommendation back to Kafka.
7. Uses AI to generate a concise explanation for the recommendation.
8. Displays the result in a real-time dashboard.

The important design principle is that **AI does not make the business decision**.

Flink determines the journey, intent score, and recommendation. AI is used only to explain the already-determined recommendation.

---

## Demo Scenario

The demo follows customer `C1001` — Ahmed.

The customer generates the following events:

1. `LOGIN`
2. `PRODUCT_VIEW` — Travel Card
3. `FX_SEARCH` — USD
4. `AIRLINE_PURCHASE` — Emirates Airlines
5. `HOTEL_SEARCH`

Flink correlates these events within a one-minute streaming window.

The resulting journey is:

**INTERNATIONAL_TRAVEL**

Intent score:

**0.92**

Next best action:

**OFFER_TRAVEL_CARD**

The AI layer then generates an explanation suitable for a banking employee or customer-facing workflow.

---

## Architecture

Customer Event Simulator

↓

Confluent Kafka

↓

Flink SQL

↓

Journey Detection

↓

Intent Scoring

↓

Next Best Action

↓

`journey_recommendations`

↓

Node.js Consumer

↓

Real-Time Dashboard

↓

AI Explanation

---

## Confluent Components

JourneyIQ uses the following Confluent capabilities:

### Confluent Kafka

Kafka acts as the real-time event backbone.

The application uses topics including:

- `customer_events`
- `transactions`
- `customer_profiles`
- `journey_recommendations`

Customer events are keyed by `customer_id`, allowing events from the same customer to be correlated by the streaming application.

### Schema Registry

Schema Registry provides a governed schema contract for customer and transaction events.

The application uses schema-based serialization and deserialization rather than sending unstructured payloads between services.

### Confluent Flink

Flink performs the real-time stream processing and journey detection.

The current implementation uses a one-minute tumbling window to correlate customer activity.

The rules detect the combination of:

- Travel card interaction
- FX search
- Airline purchase
- Hotel search

When all required signals are present, the customer is classified as being on an `INTERNATIONAL_TRAVEL` journey.

### Materialized Tables

Flink materialized tables are used to expose the derived streaming state.

The key derived dataset for the dashboard is:

`journey_recommendations`

Example result:

    customer_id: C1001
    journey: INTERNATIONAL_TRAVEL
    intent_score: 0.92
    next_action: OFFER_TRAVEL_CARD

---

## Why Streaming?

The value of JourneyIQ comes from combining events rather than evaluating them independently.

For example:

`FX_SEARCH`

alone may indicate curiosity.

`AIRLINE_PURCHASE`

alone indicates a transaction.

`HOTEL_SEARCH`

alone indicates an accommodation search.

But when these events occur together with a travel-card interaction, they provide a much stronger indication of an active international-travel journey.

A streaming architecture allows the system to recognize this context while the journey is happening rather than waiting for a batch process.

---

## AI Architecture

The AI layer is intentionally constrained.

Flink is responsible for:

- Event correlation
- Journey detection
- Intent scoring
- Next-best-action selection

The AI layer is responsible for:

- Explaining the detected journey
- Explaining the recommendation
- Generating a concise customer-facing message

The AI prompt explicitly instructs the model not to:

- Change the journey
- Change the intent score
- Change the recommended action
- Invent customer activity
- Infer unsupported customer information

This keeps the AI component explainable and prevents the language model from becoming the source of truth for business decisions.

---

## Technology Stack

### Backend

- Node.js
- JavaScript
- Express
- KafkaJS

### Streaming

- Confluent Cloud
- Apache Kafka
- Confluent Schema Registry
- Confluent Flink

### AI

- OpenRouter-compatible API
- Configurable AI model
- Temperature: `0`

### Frontend

- HTML
- CSS
- JavaScript

The dashboard intentionally uses a lightweight frontend to keep the focus on the streaming architecture.

---

## Project Structure

    journeyiq/
    ├── README.md
    ├── package.json
    ├── package-lock.json
    ├── public/
    │   └── index.html
    └── src/
        ├── ai/
        │   ├── explain.js
        │   └── test.js
        ├── consumer/
        │   └── flinkConsumer.js
        ├── producer/
        │   └── publish.js
        └── server.js

---

## Configuration

Create a `.env` file in the project root.

Required configuration:

    KAFKA_BROKERS=<confluent-bootstrap-server>
    KAFKA_API_KEY=<kafka-api-key>
    KAFKA_API_SECRET=<kafka-api-secret>

    SCHEMA_REGISTRY_URL=<schema-registry-url>
    SCHEMA_REGISTRY_API_KEY=<schema-registry-api-key>
    SCHEMA_REGISTRY_API_SECRET=<schema-registry-api-secret>

    AI_API_KEY=<openrouter-api-key>
    AI_BASE_URL=https://openrouter.ai/api/v1
    AI_MODEL=openrouter/free

Do not commit the `.env` file to source control.

---

## Installation

Install the dependencies:

    npm install

The main dependencies are:

- `kafkajs`
- `@kafkajs/confluent-schema-registry`
- `express`
- `dotenv`

---

## Running the Application

Start the dashboard/server:

    node src/server.js

The application starts the Kafka consumer and the web server.

Open the dashboard in a browser:

    http://localhost:3000

---

## Running the Journey Simulation

The demo uses a controlled event simulator rather than relying on random event generation.

This is intentional.

A deterministic event sequence ensures that all events belonging to the demo journey arrive within the Flink processing window and makes the demonstration reproducible.

The simulator publishes the customer journey to the `customer_events` topic.

The sequence is:

    LOGIN
    PRODUCT_VIEW
    FX_SEARCH
    AIRLINE_PURCHASE
    HOTEL_SEARCH

Flink processes the events and produces the corresponding recommendation.

Demo UI:

<img width="1291" height="838" alt="Screenshot 2026-09-24 at 2 56 45 AM" src="https://github.com/user-attachments/assets/76a93b5b-5b0b-422e-ad23-cef05d621878" />


---

## End-to-End Flow

The complete flow is:

    1. Customer event is generated
       ↓
    2. Event is published to Confluent Kafka
       ↓
    3. Flink consumes the streaming event
       ↓
    4. Events are correlated by customer
       ↓
    5. Journey signals are calculated
       ↓
    6. Journey is detected
       ↓
    7. Intent score is calculated
       ↓
    8. Next-best action is determined
       ↓
    9. Recommendation is written to Kafka
       ↓
    10. Node.js consumer receives the recommendation
        ↓
    11. AI generates an explanation
        ↓
    12. Dashboard displays the result

---

## Example Output

For the demo customer:

    Customer: C1001
    Journey: INTERNATIONAL_TRAVEL
    Intent Score: 0.92
    Next Best Action: OFFER_TRAVEL_CARD

Example explanation:

    The customer has shown multiple signals associated with
    international travel, including travel-card interest,
    FX activity, an airline purchase and hotel search.
    These signals support the recommendation to offer a
    travel card.

The exact AI-generated wording may vary depending on the model.

---

## Design Principles

### Streaming First

Customer behaviour is processed as an event stream rather than waiting for batch processing.

### Deterministic Business Logic

Business decisions such as journey classification and next-best-action selection are handled by Flink rules.

### AI as an Explanation Layer

AI enhances the experience without becoming the decision engine.

### Schema Governance

Schema Registry provides a contract for event structures and helps prevent incompatible data from entering the streaming pipeline.

### Reproducible Demonstration

The demo journey is deliberately controlled so that the same customer journey can be reproduced reliably.

---

## Future Extensions

The architecture can be extended to support many additional customer journeys.

Examples include:

- Savings opportunity
- Gold investment interest
- Mortgage intent
- Personal-loan intent
- Credit-card upgrade opportunity
- International remittance
- Wealth-management opportunity

Additional event sources could also be connected to the platform, including:

- Mobile application events
- Web activity
- Core banking transactions
- CRM systems
- Customer support interactions
- Payment events
- External partner events

With additional streaming signals, JourneyIQ could evolve from a single journey detector into a broader real-time customer decisioning platform.

---

## Why Confluent?

JourneyIQ demonstrates how a streaming platform can act as the real-time foundation for customer intelligence.

Instead of moving customer data into a batch analytics system and evaluating it later, the platform continuously processes events as they happen.

This enables:

**Event → Context → Intent → Action**

in a single streaming flow.

That creates the foundation for real-time personalization, customer engagement and next-best-action use cases.

---

## Demo Takeaway

JourneyIQ demonstrates a simple idea:

> **A single customer event may not mean much. A stream of related events can reveal intent.**

By combining Kafka, Schema Registry, Flink and AI, JourneyIQ turns that intent into an actionable recommendation in real time.
