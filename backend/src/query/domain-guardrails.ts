const DOMAIN_TERMS = [
  "order",
  "sales order",
  "delivery",
  "billing",
  "invoice",
  "payment",
  "journal",
  "customer",
  "product",
  "plant",
  "flow",
  "o2c",
  "order to cash",
];

const OFFTOPIC_TERMS = [
  "poem",
  "joke",
  "recipe",
  "movie",
  "weather",
  "stock market",
  "politics",
  "sports",
  "travel",
  "general knowledge",
  "fiction",
  "story",
  "codeforces",
];

export type DomainDecision = {
  allowed: boolean;
  reason: string;
};

export function evaluateDomainPrompt(prompt: string): DomainDecision {
  const text = prompt.toLowerCase().trim();

  if (text.length < 3) {
    return {
      allowed: false,
      reason: "Prompt is too short to classify against the O2C dataset domain.",
    };
  }

  if (OFFTOPIC_TERMS.some((term) => text.includes(term))) {
    return {
      allowed: false,
      reason: "Prompt appears unrelated to order-to-cash dataset analysis.",
    };
  }

  if (DOMAIN_TERMS.some((term) => text.includes(term))) {
    return {
      allowed: true,
      reason: "Prompt matches order-to-cash analysis domain.",
    };
  }

  return {
    allowed: false,
    reason: "Prompt does not clearly reference the provided order-to-cash dataset domain.",
  };
}
