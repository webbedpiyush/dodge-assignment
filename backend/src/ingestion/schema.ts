import Database from "better-sqlite3";

const TABLES = [
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

export function createIngestionSchema(db: Database.Database, reset: boolean): void {
  db.exec("PRAGMA journal_mode = WAL;");
  db.exec("PRAGMA foreign_keys = OFF;");

  if (reset) {
    for (const tableName of TABLES) {
      db.exec(`DROP TABLE IF EXISTS ${tableName};`);
    }
  }

  db.exec(`
    CREATE TABLE IF NOT EXISTS sales_order_headers (
      sales_order TEXT PRIMARY KEY,
      sold_to_party TEXT,
      sales_organization TEXT,
      distribution_channel TEXT,
      organization_division TEXT,
      total_net_amount REAL,
      transaction_currency TEXT,
      creation_date TEXT,
      requested_delivery_date TEXT,
      overall_delivery_status TEXT,
      overall_billing_status TEXT,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sales_order_items (
      sales_order TEXT NOT NULL,
      sales_order_item TEXT NOT NULL,
      material TEXT,
      requested_quantity REAL,
      requested_quantity_unit TEXT,
      net_amount REAL,
      transaction_currency TEXT,
      production_plant TEXT,
      storage_location TEXT,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL,
      PRIMARY KEY (sales_order, sales_order_item)
    );

    CREATE TABLE IF NOT EXISTS sales_order_schedule_lines (
      sales_order TEXT NOT NULL,
      sales_order_item TEXT NOT NULL,
      schedule_line TEXT NOT NULL,
      confirmed_delivery_date TEXT,
      order_quantity_unit TEXT,
      confirmed_quantity REAL,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL,
      PRIMARY KEY (sales_order, sales_order_item, schedule_line)
    );

    CREATE TABLE IF NOT EXISTS outbound_delivery_headers (
      delivery_document TEXT PRIMARY KEY,
      creation_date TEXT,
      shipping_point TEXT,
      overall_goods_movement_status TEXT,
      overall_picking_status TEXT,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS outbound_delivery_items (
      delivery_document TEXT NOT NULL,
      delivery_document_item TEXT NOT NULL,
      reference_sd_document TEXT,
      reference_sd_document_item TEXT,
      plant TEXT,
      storage_location TEXT,
      actual_delivery_quantity REAL,
      delivery_quantity_unit TEXT,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL,
      PRIMARY KEY (delivery_document, delivery_document_item)
    );

    CREATE TABLE IF NOT EXISTS billing_document_headers (
      billing_document TEXT PRIMARY KEY,
      billing_document_type TEXT,
      billing_document_date TEXT,
      company_code TEXT,
      fiscal_year TEXT,
      accounting_document TEXT,
      sold_to_party TEXT,
      total_net_amount REAL,
      transaction_currency TEXT,
      billing_document_is_cancelled INTEGER,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS billing_document_items (
      billing_document TEXT NOT NULL,
      billing_document_item TEXT NOT NULL,
      material TEXT,
      reference_sd_document TEXT,
      reference_sd_document_item TEXT,
      billing_quantity REAL,
      billing_quantity_unit TEXT,
      net_amount REAL,
      transaction_currency TEXT,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL,
      PRIMARY KEY (billing_document, billing_document_item)
    );

    CREATE TABLE IF NOT EXISTS billing_document_cancellations (
      billing_document TEXT PRIMARY KEY,
      cancelled_billing_document TEXT,
      accounting_document TEXT,
      sold_to_party TEXT,
      billing_document_date TEXT,
      total_net_amount REAL,
      transaction_currency TEXT,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS journal_entry_items_ar (
      company_code TEXT NOT NULL,
      fiscal_year TEXT NOT NULL,
      accounting_document TEXT NOT NULL,
      accounting_document_item TEXT NOT NULL,
      gl_account TEXT NOT NULL,
      reference_document TEXT,
      customer TEXT,
      posting_date TEXT,
      document_date TEXT,
      amount_in_transaction_currency REAL,
      transaction_currency TEXT,
      clearing_accounting_document TEXT,
      clearing_date TEXT,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL,
      PRIMARY KEY (
        company_code,
        fiscal_year,
        accounting_document,
        accounting_document_item,
        gl_account
      )
    );

    CREATE TABLE IF NOT EXISTS payments_ar (
      company_code TEXT NOT NULL,
      fiscal_year TEXT NOT NULL,
      accounting_document TEXT NOT NULL,
      accounting_document_item TEXT NOT NULL,
      customer TEXT NOT NULL,
      clearing_accounting_document TEXT,
      clearing_date TEXT,
      amount_in_transaction_currency REAL,
      transaction_currency TEXT,
      posting_date TEXT,
      document_date TEXT,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL,
      PRIMARY KEY (
        company_code,
        fiscal_year,
        accounting_document,
        accounting_document_item,
        customer
      )
    );

    CREATE TABLE IF NOT EXISTS business_partners (
      customer TEXT PRIMARY KEY,
      business_partner TEXT,
      business_partner_name TEXT,
      business_partner_full_name TEXT,
      business_partner_category TEXT,
      business_partner_grouping TEXT,
      blocked INTEGER,
      marked_for_archiving INTEGER,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS business_partner_addresses (
      business_partner TEXT NOT NULL,
      address_id TEXT NOT NULL,
      city_name TEXT,
      country TEXT,
      region TEXT,
      postal_code TEXT,
      street_name TEXT,
      validity_start_date TEXT,
      validity_end_date TEXT,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL,
      PRIMARY KEY (business_partner, address_id)
    );

    CREATE TABLE IF NOT EXISTS customer_sales_area_assignments (
      customer TEXT NOT NULL,
      sales_organization TEXT NOT NULL,
      distribution_channel TEXT NOT NULL,
      division TEXT NOT NULL,
      currency TEXT,
      customer_payment_terms TEXT,
      incoterms_classification TEXT,
      incoterms_location1 TEXT,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL,
      PRIMARY KEY (customer, sales_organization, distribution_channel, division)
    );

    CREATE TABLE IF NOT EXISTS customer_company_assignments (
      customer TEXT NOT NULL,
      company_code TEXT NOT NULL,
      reconciliation_account TEXT,
      customer_account_group TEXT,
      deletion_indicator INTEGER,
      payment_blocking_reason TEXT,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL,
      PRIMARY KEY (customer, company_code)
    );

    CREATE TABLE IF NOT EXISTS products (
      product TEXT PRIMARY KEY,
      product_type TEXT,
      product_old_id TEXT,
      product_group TEXT,
      base_unit TEXT,
      division TEXT,
      gross_weight REAL,
      net_weight REAL,
      weight_unit TEXT,
      is_marked_for_deletion INTEGER,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS product_descriptions (
      product TEXT NOT NULL,
      language TEXT NOT NULL,
      product_description TEXT,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL,
      PRIMARY KEY (product, language)
    );

    CREATE TABLE IF NOT EXISTS product_plants (
      product TEXT NOT NULL,
      plant TEXT NOT NULL,
      availability_check_type TEXT,
      profit_center TEXT,
      mrp_type TEXT,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL,
      PRIMARY KEY (product, plant)
    );

    CREATE TABLE IF NOT EXISTS product_storage_locations (
      product TEXT NOT NULL,
      plant TEXT NOT NULL,
      storage_location TEXT NOT NULL,
      physical_inventory_block_ind TEXT,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL,
      PRIMARY KEY (product, plant, storage_location)
    );

    CREATE TABLE IF NOT EXISTS plants (
      plant TEXT PRIMARY KEY,
      plant_name TEXT,
      valuation_area TEXT,
      sales_organization TEXT,
      distribution_channel TEXT,
      division TEXT,
      language TEXT,
      is_marked_for_archiving INTEGER,
      source_file TEXT,
      source_row INTEGER,
      raw_json TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_soi_material ON sales_order_items(material);
    CREATE INDEX IF NOT EXISTS idx_odi_ref_doc ON outbound_delivery_items(reference_sd_document, reference_sd_document_item);
    CREATE INDEX IF NOT EXISTS idx_bdi_ref_doc ON billing_document_items(reference_sd_document, reference_sd_document_item);
    CREATE INDEX IF NOT EXISTS idx_bdi_material ON billing_document_items(material);
    CREATE INDEX IF NOT EXISTS idx_bdh_accounting_doc ON billing_document_headers(accounting_document);
    CREATE INDEX IF NOT EXISTS idx_jei_reference_doc ON journal_entry_items_ar(reference_document);
    CREATE INDEX IF NOT EXISTS idx_jei_customer ON journal_entry_items_ar(customer);
    CREATE INDEX IF NOT EXISTS idx_pay_customer ON payments_ar(customer);
  `);
}
