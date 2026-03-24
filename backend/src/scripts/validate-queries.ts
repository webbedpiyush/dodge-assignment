import assert from "node:assert/strict";
import { runNaturalLanguageQuery } from "../query/service.js";

type ValidationCase = {
  id: string;
  prompt: string;
  expectSuccess: boolean;
  expectGuardrail?: boolean;
  expectRows?: boolean;
};

const cases: ValidationCase[] = [
  {
    id: "highest-billed-products",
    prompt: "Which products are associated with the highest number of billing documents?",
    expectSuccess: true,
    expectRows: true,
  },
  {
    id: "trace-billing-flow",
    prompt: "Trace the full flow of billing document 90504298",
    expectSuccess: true,
    expectRows: true,
  },
  {
    id: "broken-flow-detection",
    prompt: "Identify sales orders that are delivered but not billed or have incomplete flows.",
    expectSuccess: true,
    expectRows: true,
  },
  {
    id: "off-topic-guardrail",
    prompt: "Write a poem about the moon and stars.",
    expectSuccess: false,
    expectGuardrail: true,
  },
];

async function main() {
  const report: Array<Record<string, unknown>> = [];

  for (const test of cases) {
    const result = await runNaturalLanguageQuery(test.prompt);
    report.push({
      id: test.id,
      ok: result.ok,
      guardrailTriggered: result.guardrailTriggered,
      rowCount: result.rowCount ?? 0,
      source: result.source ?? null,
      intent: result.intent ?? null,
    });

    assert.equal(
      result.ok,
      test.expectSuccess,
      `${test.id}: expected ok=${test.expectSuccess}, got ${result.ok}. Message: ${result.message}`,
    );

    if (test.expectGuardrail !== undefined) {
      assert.equal(
        result.guardrailTriggered,
        test.expectGuardrail,
        `${test.id}: expected guardrailTriggered=${test.expectGuardrail}, got ${result.guardrailTriggered}`,
      );
    }

    if (test.expectRows) {
      assert.ok(
        (result.rowCount ?? 0) > 0,
        `${test.id}: expected rowCount > 0, got ${result.rowCount ?? 0}. Message: ${result.message}`,
      );
    }
  }

  console.log("Validation report:");
  console.table(report);
  console.log("Milestone 6 validation checks passed.");
}

void main();
