type RowValue = string | number | boolean | null | undefined;
type QueryRow = Record<string, RowValue>;

function asText(value: RowValue): string | null {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text.length ? text : null;
}

export function deriveGraphNodeIdsFromRows(rows: QueryRow[]): string[] {
  const out = new Set<string>();

  for (const row of rows) {
    const salesOrder = asText(row.sales_order ?? row.salesOrder);
    const salesOrderItem = asText(row.sales_order_item ?? row.salesOrderItem);
    const deliveryDocument = asText(row.delivery_document ?? row.deliveryDocument);
    const deliveryItem = asText(row.delivery_document_item ?? row.deliveryDocumentItem);
    const billingDocument = asText(row.billing_document ?? row.billingDocument);
    const billingItem = asText(row.billing_document_item ?? row.billingDocumentItem);
    const journalEntry = asText(
      row.journal_entry_document ?? row.accounting_document ?? row.accountingDocument,
    );
    const customer = asText(row.customer ?? row.sold_to_party ?? row.soldToParty ?? row.journal_customer);
    const product = asText(row.product ?? row.material);
    const plant = asText(row.plant);

    if (salesOrder) out.add(`SO:${salesOrder}`);
    if (salesOrder && salesOrderItem) out.add(`SOI:${salesOrder}:${salesOrderItem}`);
    if (deliveryDocument) out.add(`D:${deliveryDocument}`);
    if (deliveryDocument && deliveryItem) out.add(`DI:${deliveryDocument}:${deliveryItem}`);
    if (billingDocument) out.add(`B:${billingDocument}`);
    if (billingDocument && billingItem) out.add(`BI:${billingDocument}:${billingItem}`);
    if (journalEntry) out.add(`JE:${journalEntry}`);
    if (customer) out.add(`C:${customer}`);
    if (product) out.add(`P:${product}`);
    if (plant) out.add(`PLANT:${plant}`);
  }

  return Array.from(out);
}
