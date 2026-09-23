require("dotenv").config();

const express = require("express");
const path = require("path");
const { explainJourney } = require("./ai/explain");
const { startConsumer } = require("./consumer/flinkConsumer");

const app = express();
const PORT = 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, "..", "public")));

let state = {
    customerId: "C1001",
    customerName: "Ahmed",
    journey: "GENERAL",
    intentScore: 0.2,
    nextAction: "NO_ACTION",
    signals: [],
    events: [],
    aiExplanation: null,
    running: false,
  };

const journeySteps = [
  {
    delay: 0,
    eventType: "LOGIN",
    label: "Customer logged in",
    journey: "GENERAL",
    score: 0.2,
  },
  {
    delay: 3000,
    eventType: "PRODUCT_VIEW",
    label: "Viewed Travel Card",
    journey: "GENERAL",
    score: 0.2,
  },
  {
    delay: 6000,
    eventType: "FX_SEARCH",
    label: "Searched USD FX rate",
    journey: "TRAVEL",
    score: 0.7,
  },
  {
    delay: 9000,
    eventType: "AIRLINE_PURCHASE",
    label: "Airline purchase detected",
    journey: "TRAVEL",
    score: 0.7,
  },
  {
    delay: 12000,
    eventType: "HOTEL_SEARCH",
    label: "Hotel search detected",
    journey: "INTERNATIONAL_TRAVEL",
    score: 0.92,
  },
];

function resetState() {
  state = {
    customerId: "C1001",
    customerName: "Ahmed",
    journey: "GENERAL",
    intentScore: 0.2,
    nextAction: "NO_ACTION",
    signals: [],
    events: [],
    aiExplanation: null,
    running: true,
  };
}

async function handleRecommendation(recommendation) {

    state.customerId = recommendation.customer_id;
    state.journey = recommendation.journey;
    state.intentScore = Number(recommendation.intent_score);
    state.nextAction = recommendation.next_action;
  
    state.signals = [];
  
    if (recommendation.travel_card_views > 0) {
      state.signals.push("PRODUCT_VIEW");
    }
  
    if (recommendation.fx_searches > 0) {
      state.signals.push("FX_SEARCH");
    }
  
    if (recommendation.airline_purchases > 0) {
      state.signals.push("AIRLINE_PURCHASE");
    }
  
    if (recommendation.hotel_searches > 0) {
      state.signals.push("HOTEL_SEARCH");
    }
  
    // Only ask AI once the meaningful journey is detected.
    if (
      recommendation.journey === "INTERNATIONAL_TRAVEL"
    ) {
  
      try {
  
        state.aiExplanation = await explainJourney({
          customer_id: recommendation.customer_id,
  
          journey: recommendation.journey,
  
          intent_score: Number(
            recommendation.intent_score
          ),
  
          signals: state.signals,
  
          recommended_action:
            recommendation.next_action,
        });
  
      } catch (error) {
  
        console.error(
          "AI explanation failed:",
          error.message
        );
      }
    }
  }

async function runSimulation() {
  resetState();

  for (const step of journeySteps) {
    if (step.delay > 0) {
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }

    state.journey = step.journey;
    state.intentScore = step.score;

    state.events.unshift({
      type: step.eventType,
      label: step.label,
      timestamp: new Date().toLocaleTimeString(),
    });

    if (!state.signals.includes(step.eventType)) {
      state.signals.push(step.eventType);
    }

    if (step.journey === "TRAVEL") {
      state.nextAction = "OFFER_FX_BENEFITS";
    }

    if (step.journey === "INTERNATIONAL_TRAVEL") {
      state.nextAction = "OFFER_TRAVEL_CARD";
    }
  }

  try {
    state.aiExplanation = await explainJourney({
      customer_id: state.customerId,
      journey: state.journey,
      intent_score: state.intentScore,
      signals: [
        "Viewed travel card",
        "Searched USD FX rate",
        "Purchased airline ticket",
        "Searched hotel",
      ],
      recommended_action: state.nextAction,
    });
  } catch (error) {
    console.error("AI explanation failed:", error.message);
  }

  state.running = false;
}

app.get("/api/state", (req, res) => {
  res.json(state);
});

app.post("/api/simulate", (req, res) => {
  if (state.running) {
    return res.status(409).json({
      error: "Simulation already running",
    });
  }

  runSimulation();

  res.json({
    started: true,
  });
});

startConsumer(handleRecommendation)
  .catch((error) => {
    console.error(
      "Flink consumer failed:",
      error
    );
  });

app.listen(PORT, () => {
  console.log(`\n🚀 JourneyIQ running at http://localhost:${PORT}\n`);
});