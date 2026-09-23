require("dotenv").config();

async function explainJourney(journeyData) {
  const prompt = `
You are an AI assistant for a banking customer-journey intelligence system.

Your job is ONLY to explain a recommendation that has already been
determined by a deterministic streaming rules engine.

Do not change the journey.
Do not change the intent score.
Do not invent customer activity.
Do not recommend a different action.

Given this data:

${JSON.stringify(journeyData, null, 2)}

Write a concise, professional explanation for a banking employee.

Return ONLY a valid JSON object.

Do not include:
- safety classifications
- explanations outside the JSON
- markdown
- code fences
- introductory text

The response must start with { and end with }.

Use exactly this structure:

{
  "summary": "...",
  "reasoning": "...",
  "customer_message": "..."
}
`;

  const response = await fetch(
    `${process.env.AI_BASE_URL}/chat/completions`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.AI_API_KEY}`,
      },
      body: JSON.stringify({
        model: process.env.AI_MODEL,
        temperature: 0,
        messages: [
          {
            role: "system",
            content:
              "You explain deterministic customer journey recommendations. Never alter the supplied recommendation.",
          },
          {
            role: "user",
            content: prompt,
          },
        ],
      }),
    }
  );

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`AI API error ${response.status}: ${error}`);
  }

  const data = await response.json();

  const content = data.choices?.[0]?.message?.content;

  if (!content) {
    throw new Error("AI response did not contain content");
  }

  // Handle models that wrap JSON in markdown fences.
  let cleaned = content
  .replace(/^```json\s*/i, "")
  .replace(/^```\s*/i, "")
  .replace(/\s*```$/i, "")
  .trim();

// Some free models prepend safety/classification text.
// Extract the JSON object instead of assuming the entire
// response is valid JSON.
const jsonStart = cleaned.indexOf("{");
const jsonEnd = cleaned.lastIndexOf("}");

if (jsonStart === -1 || jsonEnd === -1 || jsonEnd <= jsonStart) {
  throw new Error(
    `AI response did not contain valid JSON: ${content}`
  );
}

cleaned = cleaned.slice(jsonStart, jsonEnd + 1);

return JSON.parse(cleaned);
}

module.exports = {
  explainJourney,
};