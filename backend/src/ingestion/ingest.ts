import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import {
  getBoolean,
  getNumber,
  getString,
  normalizeDocItem,
  normalizeDocNumber,
  toRawJson,
} from "./normalizers.js";
import { createIngestionSchema } from "./schema.js";

type JsonRecord = Record<string, unknown>;

export type IngestionSummary = {
  datasetDir: string;
  startedAt: string;
  finishedAt: string;
  totalInserted: number;
  totalFailed: number;
  tables: Record<string, { inserted: number; failed: number }>;
};

type TableCounter = { inserted: number; failed: number };
type TableCounters = Record<string, TableCounter>;

function defaultCounters(): TableCounters {
  const names = [
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
  ] as const;

  return Object.fromEntries(names.map((name) => [name, { inserted: 0, failed: 0 }]));
}

function ingestFolder(
  datasetDir: string,
  folderName: string,
  onRecord: (record: JsonRecord, sourceFile: string, sourceRow: number) => void,
  counters: TableCounters,
  tableName: string,
): void {
  const folderPath = path.join(datasetDir, folderName);
  if (!fs.existsSync(folderPath)) {
    return;
  }

  const files = fs
    .readdirSync(folderPath)
    .filter((file) => file.endsWith(".jsonl"))
    .sort((a, b) => a.localeCompare(b));

  for (const fileName of files) {
    const sourceFile = path.join(folderName, fileName);
    const absoluteFile = path.join(folderPath, fileName);
    const lines = fs.readFileSync(absoluteFile, "utf8").split(/\r?\n/);

    lines.forEach((line, idx) => {
      const sourceRow = idx + 1;
      if (!line.trim()) {
        return;
      }

      try {
        const parsed = JSON.parse(line) as JsonRecord;
        onRecord(parsed, sourceFile, sourceRow);
        counters[tableName].inserted += 1;
      } catch (_error) {
        counters[tableName].failed += 1;
      }
    });
  }
}

export function ingestDataset(
  db: Database.Database,
  options: { datasetDir: string; reset: boolean },
): IngestionSummary {
  const startedAt = new Date().toISOString();
  const counters = defaultCounters();

  createIngestionSchema(db, options.reset);

  const statements = {
    salesOrderHeaders: db.prepare(`
      INSERT OR REPLACE INTO sales_order_headers (
        sales_order, sold_to_party, sales_organization, distribution_channel, organization_division,
        total_net_amount, transaction_currency, creation_date, requested_delivery_date,
        overall_delivery_status, overall_billing_status, source_file, source_row, raw_json
      ) VALUES (
        @sales_order, @sold_to_party, @sales_organization, @distribution_channel, @organization_division,
        @total_net_amount, @transaction_currency, @creation_date, @requested_delivery_date,
        @overall_delivery_status, @overall_billing_status, @source_file, @source_row, @raw_json
      );
    `),
    salesOrderItems: db.prepare(`
      INSERT OR REPLACE INTO sales_order_items (
        sales_order, sales_order_item, material, requested_quantity, requested_quantity_unit,
        net_amount, transaction_currency, production_plant, storage_location, source_file, source_row, raw_json
      ) VALUES (
        @sales_order, @sales_order_item, @material, @requested_quantity, @requested_quantity_unit,
        @net_amount, @transaction_currency, @production_plant, @storage_location, @source_file, @source_row, @raw_json
      );
    `),
    salesOrderScheduleLines: db.prepare(`
      INSERT OR REPLACE INTO sales_order_schedule_lines (
        sales_order, sales_order_item, schedule_line, confirmed_delivery_date, order_quantity_unit,
        confirmed_quantity, source_file, source_row, raw_json
      ) VALUES (
        @sales_order, @sales_order_item, @schedule_line, @confirmed_delivery_date, @order_quantity_unit,
        @confirmed_quantity, @source_file, @source_row, @raw_json
      );
    `),
    outboundDeliveryHeaders: db.prepare(`
      INSERT OR REPLACE INTO outbound_delivery_headers (
        delivery_document, creation_date, shipping_point, overall_goods_movement_status,
        overall_picking_status, source_file, source_row, raw_json
      ) VALUES (
        @delivery_document, @creation_date, @shipping_point, @overall_goods_movement_status,
        @overall_picking_status, @source_file, @source_row, @raw_json
      );
    `),
    outboundDeliveryItems: db.prepare(`
      INSERT OR REPLACE INTO outbound_delivery_items (
        delivery_document, delivery_document_item, reference_sd_document, reference_sd_document_item,
        plant, storage_location, actual_delivery_quantity, delivery_quantity_unit, source_file, source_row, raw_json
      ) VALUES (
        @delivery_document, @delivery_document_item, @reference_sd_document, @reference_sd_document_item,
        @plant, @storage_location, @actual_delivery_quantity, @delivery_quantity_unit, @source_file, @source_row, @raw_json
      );
    `),
    billingDocumentHeaders: db.prepare(`
      INSERT OR REPLACE INTO billing_document_headers (
        billing_document, billing_document_type, billing_document_date, company_code, fiscal_year,
        accounting_document, sold_to_party, total_net_amount, transaction_currency,
        billing_document_is_cancelled, source_file, source_row, raw_json
      ) VALUES (
        @billing_document, @billing_document_type, @billing_document_date, @company_code, @fiscal_year,
        @accounting_document, @sold_to_party, @total_net_amount, @transaction_currency,
        @billing_document_is_cancelled, @source_file, @source_row, @raw_json
      );
    `),
    billingDocumentItems: db.prepare(`
      INSERT OR REPLACE INTO billing_document_items (
        billing_document, billing_document_item, material, reference_sd_document, reference_sd_document_item,
        billing_quantity, billing_quantity_unit, net_amount, transaction_currency, source_file, source_row, raw_json
      ) VALUES (
        @billing_document, @billing_document_item, @material, @reference_sd_document, @reference_sd_document_item,
        @billing_quantity, @billing_quantity_unit, @net_amount, @transaction_currency, @source_file, @source_row, @raw_json
      );
    `),
    billingDocumentCancellations: db.prepare(`
      INSERT OR REPLACE INTO billing_document_cancellations (
        billing_document, cancelled_billing_document, accounting_document, sold_to_party,
        billing_document_date, total_net_amount, transaction_currency, source_file, source_row, raw_json
      ) VALUES (
        @billing_document, @cancelled_billing_document, @accounting_document, @sold_to_party,
        @billing_document_date, @total_net_amount, @transaction_currency, @source_file, @source_row, @raw_json
      );
    `),
    journalEntryItemsAr: db.prepare(`
      INSERT OR REPLACE INTO journal_entry_items_ar (
        company_code, fiscal_year, accounting_document, accounting_document_item, gl_account,
        reference_document, customer, posting_date, document_date, amount_in_transaction_currency,
        transaction_currency, clearing_accounting_document, clearing_date, source_file, source_row, raw_json
      ) VALUES (
        @company_code, @fiscal_year, @accounting_document, @accounting_document_item, @gl_account,
        @reference_document, @customer, @posting_date, @document_date, @amount_in_transaction_currency,
        @transaction_currency, @clearing_accounting_document, @clearing_date, @source_file, @source_row, @raw_json
      );
    `),
    paymentsAr: db.prepare(`
      INSERT OR REPLACE INTO payments_ar (
        company_code, fiscal_year, accounting_document, accounting_document_item, customer,
        clearing_accounting_document, clearing_date, amount_in_transaction_currency, transaction_currency,
        posting_date, document_date, source_file, source_row, raw_json
      ) VALUES (
        @company_code, @fiscal_year, @accounting_document, @accounting_document_item, @customer,
        @clearing_accounting_document, @clearing_date, @amount_in_transaction_currency, @transaction_currency,
        @posting_date, @document_date, @source_file, @source_row, @raw_json
      );
    `),
    businessPartners: db.prepare(`
      INSERT OR REPLACE INTO business_partners (
        customer, business_partner, business_partner_name, business_partner_full_name,
        business_partner_category, business_partner_grouping, blocked, marked_for_archiving,
        source_file, source_row, raw_json
      ) VALUES (
        @customer, @business_partner, @business_partner_name, @business_partner_full_name,
        @business_partner_category, @business_partner_grouping, @blocked, @marked_for_archiving,
        @source_file, @source_row, @raw_json
      );
    `),
    businessPartnerAddresses: db.prepare(`
      INSERT OR REPLACE INTO business_partner_addresses (
        business_partner, address_id, city_name, country, region, postal_code, street_name,
        validity_start_date, validity_end_date, source_file, source_row, raw_json
      ) VALUES (
        @business_partner, @address_id, @city_name, @country, @region, @postal_code, @street_name,
        @validity_start_date, @validity_end_date, @source_file, @source_row, @raw_json
      );
    `),
    customerSalesAreaAssignments: db.prepare(`
      INSERT OR REPLACE INTO customer_sales_area_assignments (
        customer, sales_organization, distribution_channel, division, currency, customer_payment_terms,
        incoterms_classification, incoterms_location1, source_file, source_row, raw_json
      ) VALUES (
        @customer, @sales_organization, @distribution_channel, @division, @currency, @customer_payment_terms,
        @incoterms_classification, @incoterms_location1, @source_file, @source_row, @raw_json
      );
    `),
    customerCompanyAssignments: db.prepare(`
      INSERT OR REPLACE INTO customer_company_assignments (
        customer, company_code, reconciliation_account, customer_account_group, deletion_indicator,
        payment_blocking_reason, source_file, source_row, raw_json
      ) VALUES (
        @customer, @company_code, @reconciliation_account, @customer_account_group, @deletion_indicator,
        @payment_blocking_reason, @source_file, @source_row, @raw_json
      );
    `),
    products: db.prepare(`
      INSERT OR REPLACE INTO products (
        product, product_type, product_old_id, product_group, base_unit, division, gross_weight,
        net_weight, weight_unit, is_marked_for_deletion, source_file, source_row, raw_json
      ) VALUES (
        @product, @product_type, @product_old_id, @product_group, @base_unit, @division, @gross_weight,
        @net_weight, @weight_unit, @is_marked_for_deletion, @source_file, @source_row, @raw_json
      );
    `),
    productDescriptions: db.prepare(`
      INSERT OR REPLACE INTO product_descriptions (
        product, language, product_description, source_file, source_row, raw_json
      ) VALUES (
        @product, @language, @product_description, @source_file, @source_row, @raw_json
      );
    `),
    productPlants: db.prepare(`
      INSERT OR REPLACE INTO product_plants (
        product, plant, availability_check_type, profit_center, mrp_type, source_file, source_row, raw_json
      ) VALUES (
        @product, @plant, @availability_check_type, @profit_center, @mrp_type, @source_file, @source_row, @raw_json
      );
    `),
    productStorageLocations: db.prepare(`
      INSERT OR REPLACE INTO product_storage_locations (
        product, plant, storage_location, physical_inventory_block_ind, source_file, source_row, raw_json
      ) VALUES (
        @product, @plant, @storage_location, @physical_inventory_block_ind, @source_file, @source_row, @raw_json
      );
    `),
    plants: db.prepare(`
      INSERT OR REPLACE INTO plants (
        plant, plant_name, valuation_area, sales_organization, distribution_channel, division,
        language, is_marked_for_archiving, source_file, source_row, raw_json
      ) VALUES (
        @plant, @plant_name, @valuation_area, @sales_organization, @distribution_channel, @division,
        @language, @is_marked_for_archiving, @source_file, @source_row, @raw_json
      );
    `),
  };

  const tx = db.transaction(() => {
    ingestFolder(
      options.datasetDir,
      "sales_order_headers",
      (record, sourceFile, sourceRow) => {
        statements.salesOrderHeaders.run({
          sales_order: normalizeDocNumber(getString(record, "salesOrder")),
          sold_to_party: getString(record, "soldToParty"),
          sales_organization: getString(record, "salesOrganization"),
          distribution_channel: getString(record, "distributionChannel"),
          organization_division: getString(record, "organizationDivision"),
          total_net_amount: getNumber(record, "totalNetAmount"),
          transaction_currency: getString(record, "transactionCurrency"),
          creation_date: getString(record, "creationDate"),
          requested_delivery_date: getString(record, "requestedDeliveryDate"),
          overall_delivery_status: getString(record, "overallDeliveryStatus"),
          overall_billing_status: getString(record, "overallOrdReltdBillgStatus"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "sales_order_headers",
    );

    ingestFolder(
      options.datasetDir,
      "sales_order_items",
      (record, sourceFile, sourceRow) => {
        statements.salesOrderItems.run({
          sales_order: normalizeDocNumber(getString(record, "salesOrder")),
          sales_order_item: normalizeDocItem(getString(record, "salesOrderItem")),
          material: getString(record, "material"),
          requested_quantity: getNumber(record, "requestedQuantity"),
          requested_quantity_unit: getString(record, "requestedQuantityUnit"),
          net_amount: getNumber(record, "netAmount"),
          transaction_currency: getString(record, "transactionCurrency"),
          production_plant: getString(record, "productionPlant"),
          storage_location: getString(record, "storageLocation"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "sales_order_items",
    );

    ingestFolder(
      options.datasetDir,
      "sales_order_schedule_lines",
      (record, sourceFile, sourceRow) => {
        statements.salesOrderScheduleLines.run({
          sales_order: normalizeDocNumber(getString(record, "salesOrder")),
          sales_order_item: normalizeDocItem(getString(record, "salesOrderItem")),
          schedule_line: getString(record, "scheduleLine"),
          confirmed_delivery_date: getString(record, "confirmedDeliveryDate"),
          order_quantity_unit: getString(record, "orderQuantityUnit"),
          confirmed_quantity: getNumber(record, "confdOrderQtyByMatlAvailCheck"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "sales_order_schedule_lines",
    );

    ingestFolder(
      options.datasetDir,
      "outbound_delivery_headers",
      (record, sourceFile, sourceRow) => {
        statements.outboundDeliveryHeaders.run({
          delivery_document: normalizeDocNumber(getString(record, "deliveryDocument")),
          creation_date: getString(record, "creationDate"),
          shipping_point: getString(record, "shippingPoint"),
          overall_goods_movement_status: getString(record, "overallGoodsMovementStatus"),
          overall_picking_status: getString(record, "overallPickingStatus"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "outbound_delivery_headers",
    );

    ingestFolder(
      options.datasetDir,
      "outbound_delivery_items",
      (record, sourceFile, sourceRow) => {
        statements.outboundDeliveryItems.run({
          delivery_document: normalizeDocNumber(getString(record, "deliveryDocument")),
          delivery_document_item: normalizeDocItem(getString(record, "deliveryDocumentItem")),
          reference_sd_document: normalizeDocNumber(getString(record, "referenceSdDocument")),
          reference_sd_document_item: normalizeDocItem(getString(record, "referenceSdDocumentItem")),
          plant: getString(record, "plant"),
          storage_location: getString(record, "storageLocation"),
          actual_delivery_quantity: getNumber(record, "actualDeliveryQuantity"),
          delivery_quantity_unit: getString(record, "deliveryQuantityUnit"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "outbound_delivery_items",
    );

    ingestFolder(
      options.datasetDir,
      "billing_document_headers",
      (record, sourceFile, sourceRow) => {
        statements.billingDocumentHeaders.run({
          billing_document: normalizeDocNumber(getString(record, "billingDocument")),
          billing_document_type: getString(record, "billingDocumentType"),
          billing_document_date: getString(record, "billingDocumentDate"),
          company_code: getString(record, "companyCode"),
          fiscal_year: getString(record, "fiscalYear"),
          accounting_document: normalizeDocNumber(getString(record, "accountingDocument")),
          sold_to_party: getString(record, "soldToParty"),
          total_net_amount: getNumber(record, "totalNetAmount"),
          transaction_currency: getString(record, "transactionCurrency"),
          billing_document_is_cancelled: getBoolean(record, "billingDocumentIsCancelled"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "billing_document_headers",
    );

    ingestFolder(
      options.datasetDir,
      "billing_document_items",
      (record, sourceFile, sourceRow) => {
        statements.billingDocumentItems.run({
          billing_document: normalizeDocNumber(getString(record, "billingDocument")),
          billing_document_item: normalizeDocItem(getString(record, "billingDocumentItem")),
          material: getString(record, "material"),
          reference_sd_document: normalizeDocNumber(getString(record, "referenceSdDocument")),
          reference_sd_document_item: normalizeDocItem(getString(record, "referenceSdDocumentItem")),
          billing_quantity: getNumber(record, "billingQuantity"),
          billing_quantity_unit: getString(record, "billingQuantityUnit"),
          net_amount: getNumber(record, "netAmount"),
          transaction_currency: getString(record, "transactionCurrency"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "billing_document_items",
    );

    ingestFolder(
      options.datasetDir,
      "billing_document_cancellations",
      (record, sourceFile, sourceRow) => {
        statements.billingDocumentCancellations.run({
          billing_document: normalizeDocNumber(getString(record, "billingDocument")),
          cancelled_billing_document: normalizeDocNumber(getString(record, "cancelledBillingDocument")),
          accounting_document: normalizeDocNumber(getString(record, "accountingDocument")),
          sold_to_party: getString(record, "soldToParty"),
          billing_document_date: getString(record, "billingDocumentDate"),
          total_net_amount: getNumber(record, "totalNetAmount"),
          transaction_currency: getString(record, "transactionCurrency"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "billing_document_cancellations",
    );

    ingestFolder(
      options.datasetDir,
      "journal_entry_items_accounts_receivable",
      (record, sourceFile, sourceRow) => {
        statements.journalEntryItemsAr.run({
          company_code: getString(record, "companyCode"),
          fiscal_year: getString(record, "fiscalYear"),
          accounting_document: normalizeDocNumber(getString(record, "accountingDocument")),
          accounting_document_item: normalizeDocItem(getString(record, "accountingDocumentItem")),
          gl_account: getString(record, "glAccount"),
          reference_document: normalizeDocNumber(getString(record, "referenceDocument")),
          customer: getString(record, "customer"),
          posting_date: getString(record, "postingDate"),
          document_date: getString(record, "documentDate"),
          amount_in_transaction_currency: getNumber(record, "amountInTransactionCurrency"),
          transaction_currency: getString(record, "transactionCurrency"),
          clearing_accounting_document: normalizeDocNumber(getString(record, "clearingAccountingDocument")),
          clearing_date: getString(record, "clearingDate"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "journal_entry_items_ar",
    );

    ingestFolder(
      options.datasetDir,
      "payments_accounts_receivable",
      (record, sourceFile, sourceRow) => {
        statements.paymentsAr.run({
          company_code: getString(record, "companyCode"),
          fiscal_year: getString(record, "fiscalYear"),
          accounting_document: normalizeDocNumber(getString(record, "accountingDocument")),
          accounting_document_item: normalizeDocItem(getString(record, "accountingDocumentItem")),
          customer: getString(record, "customer"),
          clearing_accounting_document: normalizeDocNumber(getString(record, "clearingAccountingDocument")),
          clearing_date: getString(record, "clearingDate"),
          amount_in_transaction_currency: getNumber(record, "amountInTransactionCurrency"),
          transaction_currency: getString(record, "transactionCurrency"),
          posting_date: getString(record, "postingDate"),
          document_date: getString(record, "documentDate"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "payments_ar",
    );

    ingestFolder(
      options.datasetDir,
      "business_partners",
      (record, sourceFile, sourceRow) => {
        statements.businessPartners.run({
          customer: getString(record, "customer"),
          business_partner: getString(record, "businessPartner"),
          business_partner_name: getString(record, "businessPartnerName"),
          business_partner_full_name: getString(record, "businessPartnerFullName"),
          business_partner_category: getString(record, "businessPartnerCategory"),
          business_partner_grouping: getString(record, "businessPartnerGrouping"),
          blocked: getBoolean(record, "businessPartnerIsBlocked"),
          marked_for_archiving: getBoolean(record, "isMarkedForArchiving"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "business_partners",
    );

    ingestFolder(
      options.datasetDir,
      "business_partner_addresses",
      (record, sourceFile, sourceRow) => {
        statements.businessPartnerAddresses.run({
          business_partner: getString(record, "businessPartner"),
          address_id: getString(record, "addressId"),
          city_name: getString(record, "cityName"),
          country: getString(record, "country"),
          region: getString(record, "region"),
          postal_code: getString(record, "postalCode"),
          street_name: getString(record, "streetName"),
          validity_start_date: getString(record, "validityStartDate"),
          validity_end_date: getString(record, "validityEndDate"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "business_partner_addresses",
    );

    ingestFolder(
      options.datasetDir,
      "customer_sales_area_assignments",
      (record, sourceFile, sourceRow) => {
        statements.customerSalesAreaAssignments.run({
          customer: getString(record, "customer"),
          sales_organization: getString(record, "salesOrganization"),
          distribution_channel: getString(record, "distributionChannel"),
          division: getString(record, "division"),
          currency: getString(record, "currency"),
          customer_payment_terms: getString(record, "customerPaymentTerms"),
          incoterms_classification: getString(record, "incotermsClassification"),
          incoterms_location1: getString(record, "incotermsLocation1"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "customer_sales_area_assignments",
    );

    ingestFolder(
      options.datasetDir,
      "customer_company_assignments",
      (record, sourceFile, sourceRow) => {
        statements.customerCompanyAssignments.run({
          customer: getString(record, "customer"),
          company_code: getString(record, "companyCode"),
          reconciliation_account: getString(record, "reconciliationAccount"),
          customer_account_group: getString(record, "customerAccountGroup"),
          deletion_indicator: getBoolean(record, "deletionIndicator"),
          payment_blocking_reason: getString(record, "paymentBlockingReason"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "customer_company_assignments",
    );

    ingestFolder(
      options.datasetDir,
      "products",
      (record, sourceFile, sourceRow) => {
        statements.products.run({
          product: getString(record, "product"),
          product_type: getString(record, "productType"),
          product_old_id: getString(record, "productOldId"),
          product_group: getString(record, "productGroup"),
          base_unit: getString(record, "baseUnit"),
          division: getString(record, "division"),
          gross_weight: getNumber(record, "grossWeight"),
          net_weight: getNumber(record, "netWeight"),
          weight_unit: getString(record, "weightUnit"),
          is_marked_for_deletion: getBoolean(record, "isMarkedForDeletion"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "products",
    );

    ingestFolder(
      options.datasetDir,
      "product_descriptions",
      (record, sourceFile, sourceRow) => {
        statements.productDescriptions.run({
          product: getString(record, "product"),
          language: getString(record, "language"),
          product_description: getString(record, "productDescription"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "product_descriptions",
    );

    ingestFolder(
      options.datasetDir,
      "product_plants",
      (record, sourceFile, sourceRow) => {
        statements.productPlants.run({
          product: getString(record, "product"),
          plant: getString(record, "plant"),
          availability_check_type: getString(record, "availabilityCheckType"),
          profit_center: getString(record, "profitCenter"),
          mrp_type: getString(record, "mrpType"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "product_plants",
    );

    ingestFolder(
      options.datasetDir,
      "product_storage_locations",
      (record, sourceFile, sourceRow) => {
        statements.productStorageLocations.run({
          product: getString(record, "product"),
          plant: getString(record, "plant"),
          storage_location: getString(record, "storageLocation"),
          physical_inventory_block_ind: getString(record, "physicalInventoryBlockInd"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "product_storage_locations",
    );

    ingestFolder(
      options.datasetDir,
      "plants",
      (record, sourceFile, sourceRow) => {
        statements.plants.run({
          plant: getString(record, "plant"),
          plant_name: getString(record, "plantName"),
          valuation_area: getString(record, "valuationArea"),
          sales_organization: getString(record, "salesOrganization"),
          distribution_channel: getString(record, "distributionChannel"),
          division: getString(record, "division"),
          language: getString(record, "language"),
          is_marked_for_archiving: getBoolean(record, "isMarkedForArchiving"),
          source_file: sourceFile,
          source_row: sourceRow,
          raw_json: toRawJson(record),
        });
      },
      counters,
      "plants",
    );
  });

  tx();

  const finishedAt = new Date().toISOString();
  const totalInserted = Object.values(counters).reduce((sum, table) => sum + table.inserted, 0);
  const totalFailed = Object.values(counters).reduce((sum, table) => sum + table.failed, 0);

  return {
    datasetDir: options.datasetDir,
    startedAt,
    finishedAt,
    totalInserted,
    totalFailed,
    tables: counters,
  };
}
