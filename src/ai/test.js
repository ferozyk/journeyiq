const { explainJourney } = require("./explain");

async function main() {
  const journey = {
    customer_id: "C1001",
    journey: "INTERNATIONAL_TRAVEL",
    intent_score: 0.92,
    signals: [
      "Viewed travel card",
      "Searched USD FX rate",
      "Purchased airline ticket",
      "Searched hotel in New York",
    ],
    recommended_action: "OFFER_TRAVEL_CARD",
  };

  const explanation = await explainJourney(journey);

  console.log("\nAI EXPLANATION\n");
  console.log(JSON.stringify(explanation, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});