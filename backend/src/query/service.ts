import { sql } from "kysely";
import { getKysely } from "../db/client.js";
import { evaluateDomainPrompt } from "./domain-guardrails.js";
import { buildSqlPlanFromPrompt } from "./gemini.js";
import { validateAndNormalizeSql } from "./sql-safety.js";

type QueryResultRow = Record<string, unknown>;

export type QueryResponsePayload = {
  ok: boolean;
  guardrailTriggered: boolean;
  message: string;
  intent?: string;
  sql?: string;
  rowCount?: number;
  rowsPreview?: QueryResultRow[];
  confidence?: number;
  source?: "gemini" | "fallback";
};

function formatRowsAnswer(rows: QueryResultRow[], rowCount: number): string {
  if (rowCount === 0) {
    return "No matching records were found in the dataset for this question.";
  }

  const sample = rows.slice(0, Math.min(3, rows.length));
  const sampleText = sample
    .map((row, idx) => {
      const values = Object.entries(row)
        .slice(0, 4)
        .map(([k, v]) => `${k}=${String(v)}`)
        .join(", ");
      return `${idx + 1}) ${values}`;
    })
    .join(" | ");

  return `Found ${rowCount} matching records. Sample: ${sampleText}`;
}

export async function runNaturalLanguageQuery(prompt: string): Promise<QueryResponsePayload> {
  const domain = evaluateDomainPrompt(prompt);
  if (!domain.allowed) {
    return {
      ok: false,
      guardrailTriggered: true,
      message: "This system is designed to answer questions related to the provided dataset only.",
    };
  }

  let plan;
  try {
    plan = await buildSqlPlanFromPrompt(prompt);
  } catch (error) {
    return {
      ok: false,
      guardrailTriggered: false,
      message: error instanceof Error ? error.message : "Failed to generate SQL plan.",
    };
  }

  const sqlCheck = validateAndNormalizeSql(plan.sql, 200);
  if (!sqlCheck.ok || !sqlCheck.safeSql) {
    return {
      ok: false,
      guardrailTriggered: true,
      message: `Generated SQL failed safety checks: ${sqlCheck.reason ?? "Unknown reason"}`,
      intent: plan.intent,
      confidence: plan.confidence,
      source: plan.source,
    };
  }

  try {
    const db = getKysely();
    const result = await sql.raw(sqlCheck.safeSql).execute(db);
    const rows = (result.rows ?? []) as QueryResultRow[];
    const rowCount = rows.length;

    return {
      ok: true,
      guardrailTriggered: false,
      message: formatRowsAnswer(rows, rowCount),
      intent: plan.intent,
      sql: sqlCheck.safeSql,
      rowCount,
      rowsPreview: rows.slice(0, 20),
      confidence: plan.confidence,
      source: plan.source,
    };
  } catch (error) {
    return {
      ok: false,
      guardrailTriggered: false,
      message: `Failed to execute generated SQL: ${error instanceof Error ? error.message : "Unknown error"}`,
      intent: plan.intent,
      sql: sqlCheck.safeSql,
      confidence: plan.confidence,
      source: plan.source,
    };
  }
}
