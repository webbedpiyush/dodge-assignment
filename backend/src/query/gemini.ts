import { env } from "../config.js";
import { QUERY_SCHEMA_CONTEXT } from "./schema-context.js";

export type LlmPlan = {
  intent: string;
  sql: string;
  explanation: string;
  confidence: number;
  source: "gemini" | "fallback";
};

type GeminiResponse = {
  candidates?: Array<{
    content?: {
      parts?: Array<{
        text?: string;
      }>;
    };
  }>;
};

function stripCodeFences(text: string): string {
  return text.replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/, "").trim();
}

function fallbackPlan(prompt: string): LlmPlan | null {
  const p = prompt.toLowerCase();

  if (p.includes("highest") && (p.includes("billing") || p.includes("invoice")) && p.includes("product")) {
    return {
      source: "fallback",
      intent: "top_billed_products",
      confidence: 0.65,
      explanation: "Top products by billing document item frequency.",
      sql: `
        SELECT
          bdi.material AS product,
          COALESCE(pd.product_description, p.product_old_id, bdi.material) AS product_name,
          COUNT(*) AS billing_document_item_count,
          COUNT(DISTINCT bdi.billing_document) AS billing_document_count
        FROM billing_document_items bdi
        LEFT JOIN products p ON p.product = bdi.material
        LEFT JOIN product_descriptions pd ON pd.product = bdi.material AND pd.language = 'EN'
        GROUP BY bdi.material, product_name
        ORDER BY billing_document_count DESC, billing_document_item_count DESC
      `.trim(),
    };
  }

  if (p.includes("broken") || p.includes("incomplete") || p.includes("not billed") || p.includes("without delivery")) {
    return {
      source: "fallback",
      intent: "broken_flow_detection",
      confidence: 0.62,
      explanation: "Find sales order items with missing downstream delivery or billing links.",
      sql: `
        WITH so_item AS (
          SELECT soi.sales_order, soi.sales_order_item
          FROM sales_order_items soi
        ),
        with_delivery AS (
          SELECT DISTINCT odi.reference_sd_document AS sales_order, odi.reference_sd_document_item AS sales_order_item
          FROM outbound_delivery_items odi
          WHERE odi.reference_sd_document IS NOT NULL AND odi.reference_sd_document_item IS NOT NULL
        ),
        with_billing AS (
          SELECT DISTINCT odi.reference_sd_document AS sales_order, odi.reference_sd_document_item AS sales_order_item
          FROM outbound_delivery_items odi
          JOIN billing_document_items bdi
            ON bdi.reference_sd_document = odi.delivery_document
           AND bdi.reference_sd_document_item = odi.delivery_document_item
        )
        SELECT
          s.sales_order,
          s.sales_order_item,
          CASE WHEN d.sales_order IS NULL THEN 1 ELSE 0 END AS missing_delivery,
          CASE WHEN b.sales_order IS NULL THEN 1 ELSE 0 END AS missing_billing
        FROM so_item s
        LEFT JOIN with_delivery d
          ON d.sales_order = s.sales_order AND d.sales_order_item = s.sales_order_item
        LEFT JOIN with_billing b
          ON b.sales_order = s.sales_order AND b.sales_order_item = s.sales_order_item
        WHERE d.sales_order IS NULL OR b.sales_order IS NULL
        ORDER BY s.sales_order, s.sales_order_item
      `.trim(),
    };
  }

  if (p.includes("trace") && (p.includes("billing") || p.includes("invoice"))) {
    const docMatch = prompt.match(/\b(9\d{6,})\b/);
    const billingDocument = docMatch?.[1];
    if (billingDocument) {
      return {
        source: "fallback",
        intent: "trace_billing_flow",
        confidence: 0.7,
        explanation: "Trace billing document through delivery, sales order, journal entry, and payment links.",
        sql: `
          SELECT
            bdh.billing_document,
            bdi.billing_document_item,
            bdi.reference_sd_document AS delivery_document,
            bdi.reference_sd_document_item AS delivery_document_item,
            odi.reference_sd_document AS sales_order,
            odi.reference_sd_document_item AS sales_order_item,
            bdh.accounting_document AS journal_entry_document,
            je.customer AS journal_customer,
            pay.accounting_document AS payment_document,
            pay.clearing_date AS payment_clearing_date
          FROM billing_document_headers bdh
          JOIN billing_document_items bdi
            ON bdi.billing_document = bdh.billing_document
          LEFT JOIN outbound_delivery_items odi
            ON odi.delivery_document = bdi.reference_sd_document
           AND odi.delivery_document_item = bdi.reference_sd_document_item
          LEFT JOIN journal_entry_items_ar je
            ON je.accounting_document = bdh.accounting_document
          LEFT JOIN payments_ar pay
            ON pay.accounting_document = je.accounting_document
           AND (pay.customer = je.customer OR je.customer IS NULL)
          WHERE bdh.billing_document = '${billingDocument}'
        `.trim(),
      };
    }
  }

  return null;
}

export async function buildSqlPlanFromPrompt(prompt: string): Promise<LlmPlan> {
  const fallback = fallbackPlan(prompt);

  if (!env.GEMINI_API_KEY) {
    if (fallback) {
      return fallback;
    }
    throw new Error("Gemini API key missing. Set GEMINI_API_KEY or ask a dataset-specific question.");
  }

  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
    env.GEMINI_MODEL,
  )}:generateContent?key=${encodeURIComponent(env.GEMINI_API_KEY)}`;

  const fullPrompt = `${QUERY_SCHEMA_CONTEXT}\n\nUser question:\n${prompt}`;
  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      contents: [{ role: "user", parts: [{ text: fullPrompt }] }],
      generationConfig: {
        temperature: 0.1,
        responseMimeType: "application/json",
      },
    }),
  });

  if (!response.ok) {
    if (fallback) {
      return fallback;
    }
    throw new Error(`Gemini request failed: ${response.status} ${response.statusText}`);
  }

  const payload = (await response.json()) as GeminiResponse;
  const candidateText = payload.candidates?.[0]?.content?.parts?.[0]?.text;
  if (!candidateText) {
    if (fallback) {
      return fallback;
    }
    throw new Error("Gemini returned an empty response.");
  }

  const parsed = JSON.parse(stripCodeFences(candidateText)) as {
    intent?: string;
    sql?: string;
    explanation?: string;
    confidence?: number;
  };

  if (!parsed.sql) {
    if (fallback) {
      return fallback;
    }
    throw new Error("Gemini response did not include SQL.");
  }

  return {
    source: "gemini",
    intent: parsed.intent ?? "dataset_analysis",
    sql: parsed.sql,
    explanation: parsed.explanation ?? "Generated query from natural language prompt.",
    confidence: typeof parsed.confidence === "number" ? parsed.confidence : 0.5,
  };
}
