const ALLOWED_TABLES = new Set([
  "sales_order_headers",
  "sales_order_items",
  "sales_order_schedule_lines",
  "outbound_delivery_headers",
  "outbound_delivery_items",
  "billing_document_headers",
  "billing_document_items",
  "billing_document_cancellations",
  "journal_entry_items_ar",
  "payments_ar",
  "business_partners",
  "business_partner_addresses",
  "customer_sales_area_assignments",
  "customer_company_assignments",
  "products",
  "product_descriptions",
  "product_plants",
  "product_storage_locations",
  "plants",
]);

const FORBIDDEN_PATTERN =
  /\b(insert|update|delete|drop|alter|truncate|create|replace|attach|detach|pragma|vacuum|reindex)\b/i;

const COMMENT_PATTERN = /(--|\/\*)/;

export type SqlValidationResult = {
  ok: boolean;
  reason?: string;
  safeSql?: string;
};

export function validateAndNormalizeSql(inputSql: string, maxLimit = 200): SqlValidationResult {
  const sql = inputSql.trim().replace(/;+\s*$/, "");
  if (!sql) {
    return { ok: false, reason: "Generated SQL is empty." };
  }

  if (!/^(select|with)\b/i.test(sql)) {
    return { ok: false, reason: "Only SELECT/CTE statements are allowed." };
  }

  if (FORBIDDEN_PATTERN.test(sql)) {
    return { ok: false, reason: "SQL contains forbidden write or admin operations." };
  }

  if (COMMENT_PATTERN.test(sql)) {
    return { ok: false, reason: "SQL comments are not allowed in generated statements." };
  }

  const tableMatches = [...sql.matchAll(/\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)\b/gi)];
  const tables = tableMatches.map((match) => match[1]);
  for (const table of tables) {
    if (!ALLOWED_TABLES.has(table)) {
      return {
        ok: false,
        reason: `Table "${table}" is outside the allowed dataset scope.`,
      };
    }
  }

  let safeSql = sql;
  if (!/\blimit\s+\d+\b/i.test(safeSql)) {
    safeSql = `${safeSql}\nLIMIT ${maxLimit}`;
  } else {
    safeSql = safeSql.replace(/\blimit\s+(\d+)\b/i, (_full, count) => {
      const bounded = Math.min(maxLimit, Number(count));
      return `LIMIT ${bounded}`;
    });
  }

  return {
    ok: true,
    safeSql,
  };
}

export function allowedTablesList(): string[] {
  return Array.from(ALLOWED_TABLES).sort();
}
