import type Database from "better-sqlite3";
import type { GraphEdge, GraphNeighborhood, GraphNode } from "./types.js";

type NodeTypePrefix =
  | "SO"
  | "SOI"
  | "D"
  | "DI"
  | "B"
  | "BI"
  | "JE"
  | "PAY"
  | "C"
  | "P"
  | "PLANT"
  | "ADDR";

type ParsedNode =
  | { kind: "SalesOrder"; salesOrder: string }
  | { kind: "SalesOrderItem"; salesOrder: string; salesOrderItem: string }
  | { kind: "Delivery"; deliveryDocument: string }
  | { kind: "DeliveryItem"; deliveryDocument: string; deliveryDocumentItem: string }
  | { kind: "BillingDocument"; billingDocument: string }
  | { kind: "BillingItem"; billingDocument: string; billingDocumentItem: string }
  | { kind: "JournalEntry"; accountingDocument: string }
  | { kind: "Payment"; companyCode: string; fiscalYear: string; accountingDocument: string; accountingDocumentItem: string; customer: string }
  | { kind: "Customer"; customer: string }
  | { kind: "Product"; product: string }
  | { kind: "Plant"; plant: string }
  | { kind: "Address"; businessPartner: string; addressId: string };

function parseNodeId(nodeId: string): ParsedNode | null {
  const [prefix, ...parts] = nodeId.split(":");
  const p = prefix as NodeTypePrefix;

  switch (p) {
    case "SO":
      return parts[0] ? { kind: "SalesOrder", salesOrder: parts[0] } : null;
    case "SOI":
      return parts[0] && parts[1] ? { kind: "SalesOrderItem", salesOrder: parts[0], salesOrderItem: parts[1] } : null;
    case "D":
      return parts[0] ? { kind: "Delivery", deliveryDocument: parts[0] } : null;
    case "DI":
      return parts[0] && parts[1] ? { kind: "DeliveryItem", deliveryDocument: parts[0], deliveryDocumentItem: parts[1] } : null;
    case "B":
      return parts[0] ? { kind: "BillingDocument", billingDocument: parts[0] } : null;
    case "BI":
      return parts[0] && parts[1] ? { kind: "BillingItem", billingDocument: parts[0], billingDocumentItem: parts[1] } : null;
    case "JE":
      return parts[0] ? { kind: "JournalEntry", accountingDocument: parts[0] } : null;
    case "PAY":
      return parts[0] && parts[1] && parts[2] && parts[3] && parts[4]
        ? {
            kind: "Payment",
            companyCode: parts[0],
            fiscalYear: parts[1],
            accountingDocument: parts[2],
            accountingDocumentItem: parts[3],
            customer: parts[4],
          }
        : null;
    case "C":
      return parts[0] ? { kind: "Customer", customer: parts[0] } : null;
    case "P":
      return parts[0] ? { kind: "Product", product: parts[0] } : null;
    case "PLANT":
      return parts[0] ? { kind: "Plant", plant: parts[0] } : null;
    case "ADDR":
      return parts[0] && parts[1] ? { kind: "Address", businessPartner: parts[0], addressId: parts[1] } : null;
    default:
      return null;
  }
}

function upsertNode(collection: Map<string, GraphNode>, node: GraphNode | null): void {
  if (!node) return;
  collection.set(node.id, node);
}

function upsertEdge(collection: Map<string, GraphEdge>, edge: GraphEdge | null): void {
  if (!edge) return;
  collection.set(edge.id, edge);
}

function edgeId(source: string, target: string, relation: string): string {
  return `${source}|${relation}|${target}`;
}

export class GraphService {
  constructor(private readonly db: Database.Database) {}

  getNode(nodeId: string): GraphNode | null {
    const parsed = parseNodeId(nodeId);
    if (!parsed) return null;

    switch (parsed.kind) {
      case "SalesOrder":
        return this.getSalesOrderNode(parsed.salesOrder);
      case "SalesOrderItem":
        return this.getSalesOrderItemNode(parsed.salesOrder, parsed.salesOrderItem);
      case "Delivery":
        return this.getDeliveryNode(parsed.deliveryDocument);
      case "DeliveryItem":
        return this.getDeliveryItemNode(parsed.deliveryDocument, parsed.deliveryDocumentItem);
      case "BillingDocument":
        return this.getBillingDocumentNode(parsed.billingDocument);
      case "BillingItem":
        return this.getBillingItemNode(parsed.billingDocument, parsed.billingDocumentItem);
      case "JournalEntry":
        return this.getJournalEntryNode(parsed.accountingDocument);
      case "Payment":
        return this.getPaymentNode(
          parsed.companyCode,
          parsed.fiscalYear,
          parsed.accountingDocument,
          parsed.accountingDocumentItem,
          parsed.customer,
        );
      case "Customer":
        return this.getCustomerNode(parsed.customer);
      case "Product":
        return this.getProductNode(parsed.product);
      case "Plant":
        return this.getPlantNode(parsed.plant);
      case "Address":
        return this.getAddressNode(parsed.businessPartner, parsed.addressId);
      default:
        return null;
    }
  }

  getSeedNodes(limit = 40): GraphNode[] {
    const nodes: GraphNode[] = [];
    const productLimit = Math.max(1, Math.floor(limit / 2));
    const customerLimit = Math.max(1, limit - productLimit);
    const billedProducts = this.db
      .prepare(
        `
        SELECT bdi.material, COUNT(*) as c
        FROM billing_document_items bdi
        WHERE bdi.material IS NOT NULL
        GROUP BY bdi.material
        ORDER BY c DESC
        LIMIT ?
      `,
      )
      .all(productLimit) as Array<{ material: string }>;

    for (const row of billedProducts) {
      const node = this.getProductNode(row.material);
      if (node) nodes.push(node);
    }

    const customers = this.db
      .prepare(
        `
        SELECT customer
        FROM business_partners
        WHERE customer IS NOT NULL
        LIMIT ?
      `,
      )
      .all(Math.max(1, Math.min(customerLimit, limit - nodes.length))) as Array<{ customer: string }>;

    for (const row of customers) {
      const node = this.getCustomerNode(row.customer);
      if (node) nodes.push(node);
    }

    return nodes;
  }

  getNeighborhood(nodeId: string, limit = 50): GraphNeighborhood {
    const center = this.getNode(nodeId);
    if (!center) {
      return { center: null, nodes: [], edges: [] };
    }

    const nodes = new Map<string, GraphNode>();
    const edges = new Map<string, GraphEdge>();
    upsertNode(nodes, center);

    const parsed = parseNodeId(nodeId);
    if (!parsed) {
      return { center, nodes: [center], edges: [] };
    }

    switch (parsed.kind) {
      case "SalesOrder": {
        const itemRows = this.db
          .prepare(
            `
            SELECT sales_order_item
            FROM sales_order_items
            WHERE sales_order = ?
            ORDER BY sales_order_item
            LIMIT ?
          `,
          )
          .all(parsed.salesOrder, limit) as Array<{ sales_order_item: string }>;

        for (const row of itemRows) {
          const itemNode = this.getSalesOrderItemNode(parsed.salesOrder, row.sales_order_item);
          upsertNode(nodes, itemNode);
          upsertEdge(
            edges,
            itemNode
              ? {
                  id: edgeId(center.id, itemNode.id, "HAS_ITEM"),
                  source: center.id,
                  target: itemNode.id,
                  relation: "HAS_ITEM",
                }
              : null,
          );
        }

        const so = this.db
          .prepare("SELECT sold_to_party FROM sales_order_headers WHERE sales_order = ?")
          .get(parsed.salesOrder) as { sold_to_party: string | null } | undefined;
        if (so?.sold_to_party) {
          const customerNode = this.getCustomerNode(so.sold_to_party);
          upsertNode(nodes, customerNode);
          upsertEdge(
            edges,
            customerNode
              ? {
                  id: edgeId(customerNode.id, center.id, "PLACED"),
                  source: customerNode.id,
                  target: center.id,
                  relation: "PLACED",
                }
              : null,
          );
        }
        break;
      }
      case "SalesOrderItem": {
        const soNode = this.getSalesOrderNode(parsed.salesOrder);
        upsertNode(nodes, soNode);
        upsertEdge(
          edges,
          soNode
            ? {
                id: edgeId(soNode.id, center.id, "HAS_ITEM"),
                source: soNode.id,
                target: center.id,
                relation: "HAS_ITEM",
              }
            : null,
        );

        const soi = this.db
          .prepare(
            `
            SELECT material
            FROM sales_order_items
            WHERE sales_order = ? AND sales_order_item = ?
          `,
          )
          .get(parsed.salesOrder, parsed.salesOrderItem) as { material: string | null } | undefined;

        if (soi?.material) {
          const productNode = this.getProductNode(soi.material);
          upsertNode(nodes, productNode);
          upsertEdge(
            edges,
            productNode
              ? {
                  id: edgeId(center.id, productNode.id, "FOR_PRODUCT"),
                  source: center.id,
                  target: productNode.id,
                  relation: "FOR_PRODUCT",
                }
              : null,
          );
        }

        const deliveryRows = this.db
          .prepare(
            `
            SELECT delivery_document, delivery_document_item
            FROM outbound_delivery_items
            WHERE reference_sd_document = ? AND reference_sd_document_item = ?
            LIMIT ?
          `,
          )
          .all(parsed.salesOrder, parsed.salesOrderItem, limit) as Array<{
          delivery_document: string;
          delivery_document_item: string;
        }>;

        for (const row of deliveryRows) {
          const diNode = this.getDeliveryItemNode(row.delivery_document, row.delivery_document_item);
          upsertNode(nodes, diNode);
          upsertEdge(
            edges,
            diNode
              ? {
                  id: edgeId(center.id, diNode.id, "FULFILLED_BY"),
                  source: center.id,
                  target: diNode.id,
                  relation: "FULFILLED_BY",
                }
              : null,
          );
        }
        break;
      }
      case "Delivery": {
        const itemRows = this.db
          .prepare(
            `
            SELECT delivery_document_item
            FROM outbound_delivery_items
            WHERE delivery_document = ?
            LIMIT ?
          `,
          )
          .all(parsed.deliveryDocument, limit) as Array<{ delivery_document_item: string }>;

        for (const row of itemRows) {
          const itemNode = this.getDeliveryItemNode(parsed.deliveryDocument, row.delivery_document_item);
          upsertNode(nodes, itemNode);
          upsertEdge(
            edges,
            itemNode
              ? {
                  id: edgeId(center.id, itemNode.id, "HAS_ITEM"),
                  source: center.id,
                  target: itemNode.id,
                  relation: "HAS_ITEM",
                }
              : null,
          );
        }
        break;
      }
      case "DeliveryItem": {
        const dNode = this.getDeliveryNode(parsed.deliveryDocument);
        upsertNode(nodes, dNode);
        upsertEdge(
          edges,
          dNode
            ? {
                id: edgeId(dNode.id, center.id, "HAS_ITEM"),
                source: dNode.id,
                target: center.id,
                relation: "HAS_ITEM",
              }
            : null,
        );

        const di = this.db
          .prepare(
            `
            SELECT reference_sd_document, reference_sd_document_item, plant
            FROM outbound_delivery_items
            WHERE delivery_document = ? AND delivery_document_item = ?
          `,
          )
          .get(parsed.deliveryDocument, parsed.deliveryDocumentItem) as
          | { reference_sd_document: string | null; reference_sd_document_item: string | null; plant: string | null }
          | undefined;

        if (di?.reference_sd_document && di.reference_sd_document_item) {
          const soiNode = this.getSalesOrderItemNode(di.reference_sd_document, di.reference_sd_document_item);
          upsertNode(nodes, soiNode);
          upsertEdge(
            edges,
            soiNode
              ? {
                  id: edgeId(soiNode.id, center.id, "FULFILLED_BY"),
                  source: soiNode.id,
                  target: center.id,
                  relation: "FULFILLED_BY",
                }
              : null,
          );
        }

        if (di?.plant) {
          const plantNode = this.getPlantNode(di.plant);
          upsertNode(nodes, plantNode);
          upsertEdge(
            edges,
            plantNode
              ? {
                  id: edgeId(center.id, plantNode.id, "SHIPPED_FROM"),
                  source: center.id,
                  target: plantNode.id,
                  relation: "SHIPPED_FROM",
                }
              : null,
          );
        }

        const billedRows = this.db
          .prepare(
            `
            SELECT billing_document, billing_document_item
            FROM billing_document_items
            WHERE reference_sd_document = ? AND reference_sd_document_item = ?
            LIMIT ?
          `,
          )
          .all(parsed.deliveryDocument, parsed.deliveryDocumentItem, limit) as Array<{
          billing_document: string;
          billing_document_item: string;
        }>;

        for (const row of billedRows) {
          const biNode = this.getBillingItemNode(row.billing_document, row.billing_document_item);
          upsertNode(nodes, biNode);
          upsertEdge(
            edges,
            biNode
              ? {
                  id: edgeId(center.id, biNode.id, "BILLED_AS"),
                  source: center.id,
                  target: biNode.id,
                  relation: "BILLED_AS",
                }
              : null,
          );
        }
        break;
      }
      case "BillingDocument": {
        const itemRows = this.db
          .prepare(
            `
            SELECT billing_document_item
            FROM billing_document_items
            WHERE billing_document = ?
            LIMIT ?
          `,
          )
          .all(parsed.billingDocument, limit) as Array<{ billing_document_item: string }>;

        for (const row of itemRows) {
          const itemNode = this.getBillingItemNode(parsed.billingDocument, row.billing_document_item);
          upsertNode(nodes, itemNode);
          upsertEdge(
            edges,
            itemNode
              ? {
                  id: edgeId(center.id, itemNode.id, "HAS_ITEM"),
                  source: center.id,
                  target: itemNode.id,
                  relation: "HAS_ITEM",
                }
              : null,
          );
        }

        const bdh = this.db
          .prepare(
            `
            SELECT accounting_document, sold_to_party
            FROM billing_document_headers
            WHERE billing_document = ?
          `,
          )
          .get(parsed.billingDocument) as { accounting_document: string | null; sold_to_party: string | null } | undefined;

        if (bdh?.accounting_document) {
          const jeNode = this.getJournalEntryNode(bdh.accounting_document);
          upsertNode(nodes, jeNode);
          upsertEdge(
            edges,
            jeNode
              ? {
                  id: edgeId(center.id, jeNode.id, "POSTED_TO"),
                  source: center.id,
                  target: jeNode.id,
                  relation: "POSTED_TO",
                }
              : null,
          );
        }

        if (bdh?.sold_to_party) {
          const customerNode = this.getCustomerNode(bdh.sold_to_party);
          upsertNode(nodes, customerNode);
          upsertEdge(
            edges,
            customerNode
              ? {
                  id: edgeId(customerNode.id, center.id, "INVOICED"),
                  source: customerNode.id,
                  target: center.id,
                  relation: "INVOICED",
                }
              : null,
          );
        }
        break;
      }
      case "BillingItem": {
        const bNode = this.getBillingDocumentNode(parsed.billingDocument);
        upsertNode(nodes, bNode);
        upsertEdge(
          edges,
          bNode
            ? {
                id: edgeId(bNode.id, center.id, "HAS_ITEM"),
                source: bNode.id,
                target: center.id,
                relation: "HAS_ITEM",
              }
            : null,
        );

        const bi = this.db
          .prepare(
            `
            SELECT material, reference_sd_document, reference_sd_document_item
            FROM billing_document_items
            WHERE billing_document = ? AND billing_document_item = ?
          `,
          )
          .get(parsed.billingDocument, parsed.billingDocumentItem) as
          | { material: string | null; reference_sd_document: string | null; reference_sd_document_item: string | null }
          | undefined;

        if (bi?.material) {
          const productNode = this.getProductNode(bi.material);
          upsertNode(nodes, productNode);
          upsertEdge(
            edges,
            productNode
              ? {
                  id: edgeId(center.id, productNode.id, "FOR_PRODUCT"),
                  source: center.id,
                  target: productNode.id,
                  relation: "FOR_PRODUCT",
                }
              : null,
          );
        }

        if (bi?.reference_sd_document && bi.reference_sd_document_item) {
          const diNode = this.getDeliveryItemNode(bi.reference_sd_document, bi.reference_sd_document_item);
          upsertNode(nodes, diNode);
          upsertEdge(
            edges,
            diNode
              ? {
                  id: edgeId(diNode.id, center.id, "BILLED_AS"),
                  source: diNode.id,
                  target: center.id,
                  relation: "BILLED_AS",
                }
              : null,
          );
        }
        break;
      }
      case "JournalEntry": {
        const linkedBilling = this.db
          .prepare(
            `
            SELECT billing_document
            FROM billing_document_headers
            WHERE accounting_document = ?
            LIMIT ?
          `,
          )
          .all(parsed.accountingDocument, limit) as Array<{ billing_document: string }>;

        for (const row of linkedBilling) {
          const billingNode = this.getBillingDocumentNode(row.billing_document);
          upsertNode(nodes, billingNode);
          upsertEdge(
            edges,
            billingNode
              ? {
                  id: edgeId(billingNode.id, center.id, "POSTED_TO"),
                  source: billingNode.id,
                  target: center.id,
                  relation: "POSTED_TO",
                }
              : null,
          );
        }

        const paymentRows = this.db
          .prepare(
            `
            SELECT company_code, fiscal_year, accounting_document, accounting_document_item, customer
            FROM payments_ar
            WHERE accounting_document = ?
            LIMIT ?
          `,
          )
          .all(parsed.accountingDocument, limit) as Array<{
          company_code: string;
          fiscal_year: string;
          accounting_document: string;
          accounting_document_item: string;
          customer: string;
        }>;

        for (const row of paymentRows) {
          const paymentNode = this.getPaymentNode(
            row.company_code,
            row.fiscal_year,
            row.accounting_document,
            row.accounting_document_item,
            row.customer,
          );
          upsertNode(nodes, paymentNode);
          upsertEdge(
            edges,
            paymentNode
              ? {
                  id: edgeId(center.id, paymentNode.id, "CLEARED_BY_PAYMENT"),
                  source: center.id,
                  target: paymentNode.id,
                  relation: "CLEARED_BY_PAYMENT",
                }
              : null,
          );
        }
        break;
      }
      case "Payment": {
        const jeNode = this.getJournalEntryNode(parsed.accountingDocument);
        upsertNode(nodes, jeNode);
        upsertEdge(
          edges,
          jeNode
            ? {
                id: edgeId(jeNode.id, center.id, "CLEARED_BY_PAYMENT"),
                source: jeNode.id,
                target: center.id,
                relation: "CLEARED_BY_PAYMENT",
              }
            : null,
        );

        const customerNode = this.getCustomerNode(parsed.customer);
        upsertNode(nodes, customerNode);
        upsertEdge(
          edges,
          customerNode
            ? {
                id: edgeId(customerNode.id, center.id, "PAID"),
                source: customerNode.id,
                target: center.id,
                relation: "PAID",
              }
            : null,
        );
        break;
      }
      case "Customer": {
        const orderRows = this.db
          .prepare(
            `
            SELECT sales_order
            FROM sales_order_headers
            WHERE sold_to_party = ?
            LIMIT ?
          `,
          )
          .all(parsed.customer, limit) as Array<{ sales_order: string }>;
        for (const row of orderRows) {
          const soNode = this.getSalesOrderNode(row.sales_order);
          upsertNode(nodes, soNode);
          upsertEdge(
            edges,
            soNode
              ? {
                  id: edgeId(center.id, soNode.id, "PLACED"),
                  source: center.id,
                  target: soNode.id,
                  relation: "PLACED",
                }
              : null,
          );
        }

        const addressRows = this.db
          .prepare(
            `
            SELECT address_id
            FROM business_partner_addresses
            WHERE business_partner = ?
            LIMIT ?
          `,
          )
          .all(parsed.customer, limit) as Array<{ address_id: string }>;
        for (const row of addressRows) {
          const addrNode = this.getAddressNode(parsed.customer, row.address_id);
          upsertNode(nodes, addrNode);
          upsertEdge(
            edges,
            addrNode
              ? {
                  id: edgeId(center.id, addrNode.id, "HAS_ADDRESS"),
                  source: center.id,
                  target: addrNode.id,
                  relation: "HAS_ADDRESS",
                }
              : null,
          );
        }
        break;
      }
      case "Product": {
        const soRows = this.db
          .prepare(
            `
            SELECT sales_order, sales_order_item
            FROM sales_order_items
            WHERE material = ?
            LIMIT ?
          `,
          )
          .all(parsed.product, limit) as Array<{ sales_order: string; sales_order_item: string }>;
        for (const row of soRows) {
          const soiNode = this.getSalesOrderItemNode(row.sales_order, row.sales_order_item);
          upsertNode(nodes, soiNode);
          upsertEdge(
            edges,
            soiNode
              ? {
                  id: edgeId(soiNode.id, center.id, "FOR_PRODUCT"),
                  source: soiNode.id,
                  target: center.id,
                  relation: "FOR_PRODUCT",
                }
              : null,
          );
        }

        const biRows = this.db
          .prepare(
            `
            SELECT billing_document, billing_document_item
            FROM billing_document_items
            WHERE material = ?
            LIMIT ?
          `,
          )
          .all(parsed.product, limit) as Array<{ billing_document: string; billing_document_item: string }>;
        for (const row of biRows) {
          const biNode = this.getBillingItemNode(row.billing_document, row.billing_document_item);
          upsertNode(nodes, biNode);
          upsertEdge(
            edges,
            biNode
              ? {
                  id: edgeId(biNode.id, center.id, "FOR_PRODUCT"),
                  source: biNode.id,
                  target: center.id,
                  relation: "FOR_PRODUCT",
                }
              : null,
          );
        }
        break;
      }
      case "Plant": {
        const diRows = this.db
          .prepare(
            `
            SELECT delivery_document, delivery_document_item
            FROM outbound_delivery_items
            WHERE plant = ?
            LIMIT ?
          `,
          )
          .all(parsed.plant, limit) as Array<{ delivery_document: string; delivery_document_item: string }>;
        for (const row of diRows) {
          const diNode = this.getDeliveryItemNode(row.delivery_document, row.delivery_document_item);
          upsertNode(nodes, diNode);
          upsertEdge(
            edges,
            diNode
              ? {
                  id: edgeId(diNode.id, center.id, "SHIPPED_FROM"),
                  source: diNode.id,
                  target: center.id,
                  relation: "SHIPPED_FROM",
                }
              : null,
          );
        }
        break;
      }
      case "Address": {
        const customerNode = this.getCustomerNode(parsed.businessPartner);
        upsertNode(nodes, customerNode);
        upsertEdge(
          edges,
          customerNode
            ? {
                id: edgeId(customerNode.id, center.id, "HAS_ADDRESS"),
                source: customerNode.id,
                target: center.id,
                relation: "HAS_ADDRESS",
              }
            : null,
        );
        break;
      }
      default:
        break;
    }

    return {
      center,
      nodes: Array.from(nodes.values()),
      edges: Array.from(edges.values()),
    };
  }

  private getSalesOrderNode(salesOrder: string): GraphNode | null {
    const row = this.db
      .prepare(
        `
        SELECT sales_order, sold_to_party, total_net_amount, transaction_currency, requested_delivery_date
        FROM sales_order_headers
        WHERE sales_order = ?
      `,
      )
      .get(salesOrder) as
      | {
          sales_order: string;
          sold_to_party: string | null;
          total_net_amount: number | null;
          transaction_currency: string | null;
          requested_delivery_date: string | null;
        }
      | undefined;
    if (!row) return null;
    return {
      id: `SO:${row.sales_order}`,
      entityType: "SalesOrder",
      label: `Sales Order ${row.sales_order}`,
      metadata: {
        salesOrder: row.sales_order,
        soldToParty: row.sold_to_party,
        totalNetAmount: row.total_net_amount,
        transactionCurrency: row.transaction_currency,
        requestedDeliveryDate: row.requested_delivery_date,
      },
    };
  }

  private getSalesOrderItemNode(salesOrder: string, salesOrderItem: string): GraphNode | null {
    const row = this.db
      .prepare(
        `
        SELECT sales_order, sales_order_item, material, requested_quantity, requested_quantity_unit, net_amount
        FROM sales_order_items
        WHERE sales_order = ? AND sales_order_item = ?
      `,
      )
      .get(salesOrder, salesOrderItem) as
      | {
          sales_order: string;
          sales_order_item: string;
          material: string | null;
          requested_quantity: number | null;
          requested_quantity_unit: string | null;
          net_amount: number | null;
        }
      | undefined;
    if (!row) return null;
    return {
      id: `SOI:${row.sales_order}:${row.sales_order_item}`,
      entityType: "SalesOrderItem",
      label: `SO Item ${row.sales_order}-${row.sales_order_item}`,
      metadata: {
        salesOrder: row.sales_order,
        salesOrderItem: row.sales_order_item,
        material: row.material,
        requestedQuantity: row.requested_quantity,
        requestedQuantityUnit: row.requested_quantity_unit,
        netAmount: row.net_amount,
      },
    };
  }

  private getDeliveryNode(deliveryDocument: string): GraphNode | null {
    const row = this.db
      .prepare(
        `
        SELECT delivery_document, creation_date, shipping_point, overall_goods_movement_status, overall_picking_status
        FROM outbound_delivery_headers
        WHERE delivery_document = ?
      `,
      )
      .get(deliveryDocument) as
      | {
          delivery_document: string;
          creation_date: string | null;
          shipping_point: string | null;
          overall_goods_movement_status: string | null;
          overall_picking_status: string | null;
        }
      | undefined;
    if (!row) return null;
    return {
      id: `D:${row.delivery_document}`,
      entityType: "Delivery",
      label: `Delivery ${row.delivery_document}`,
      metadata: {
        deliveryDocument: row.delivery_document,
        creationDate: row.creation_date,
        shippingPoint: row.shipping_point,
        overallGoodsMovementStatus: row.overall_goods_movement_status,
        overallPickingStatus: row.overall_picking_status,
      },
    };
  }

  private getDeliveryItemNode(deliveryDocument: string, deliveryDocumentItem: string): GraphNode | null {
    const row = this.db
      .prepare(
        `
        SELECT delivery_document, delivery_document_item, reference_sd_document, reference_sd_document_item,
               plant, storage_location, actual_delivery_quantity, delivery_quantity_unit
        FROM outbound_delivery_items
        WHERE delivery_document = ? AND delivery_document_item = ?
      `,
      )
      .get(deliveryDocument, deliveryDocumentItem) as
      | {
          delivery_document: string;
          delivery_document_item: string;
          reference_sd_document: string | null;
          reference_sd_document_item: string | null;
          plant: string | null;
          storage_location: string | null;
          actual_delivery_quantity: number | null;
          delivery_quantity_unit: string | null;
        }
      | undefined;
    if (!row) return null;
    return {
      id: `DI:${row.delivery_document}:${row.delivery_document_item}`,
      entityType: "DeliveryItem",
      label: `Delivery Item ${row.delivery_document}-${row.delivery_document_item}`,
      metadata: {
        deliveryDocument: row.delivery_document,
        deliveryDocumentItem: row.delivery_document_item,
        referenceSalesOrder: row.reference_sd_document,
        referenceSalesOrderItem: row.reference_sd_document_item,
        plant: row.plant,
        storageLocation: row.storage_location,
        actualDeliveryQuantity: row.actual_delivery_quantity,
        deliveryQuantityUnit: row.delivery_quantity_unit,
      },
    };
  }

  private getBillingDocumentNode(billingDocument: string): GraphNode | null {
    const row = this.db
      .prepare(
        `
        SELECT billing_document, billing_document_date, sold_to_party, total_net_amount, transaction_currency,
               accounting_document, billing_document_is_cancelled
        FROM billing_document_headers
        WHERE billing_document = ?
      `,
      )
      .get(billingDocument) as
      | {
          billing_document: string;
          billing_document_date: string | null;
          sold_to_party: string | null;
          total_net_amount: number | null;
          transaction_currency: string | null;
          accounting_document: string | null;
          billing_document_is_cancelled: number | null;
        }
      | undefined;
    if (!row) return null;
    return {
      id: `B:${row.billing_document}`,
      entityType: "BillingDocument",
      label: `Billing ${row.billing_document}`,
      metadata: {
        billingDocument: row.billing_document,
        billingDocumentDate: row.billing_document_date,
        soldToParty: row.sold_to_party,
        totalNetAmount: row.total_net_amount,
        transactionCurrency: row.transaction_currency,
        accountingDocument: row.accounting_document,
        isCancelled: row.billing_document_is_cancelled === 1,
      },
    };
  }

  private getBillingItemNode(billingDocument: string, billingDocumentItem: string): GraphNode | null {
    const row = this.db
      .prepare(
        `
        SELECT billing_document, billing_document_item, material, reference_sd_document, reference_sd_document_item,
               billing_quantity, billing_quantity_unit, net_amount, transaction_currency
        FROM billing_document_items
        WHERE billing_document = ? AND billing_document_item = ?
      `,
      )
      .get(billingDocument, billingDocumentItem) as
      | {
          billing_document: string;
          billing_document_item: string;
          material: string | null;
          reference_sd_document: string | null;
          reference_sd_document_item: string | null;
          billing_quantity: number | null;
          billing_quantity_unit: string | null;
          net_amount: number | null;
          transaction_currency: string | null;
        }
      | undefined;
    if (!row) return null;
    return {
      id: `BI:${row.billing_document}:${row.billing_document_item}`,
      entityType: "BillingItem",
      label: `Billing Item ${row.billing_document}-${row.billing_document_item}`,
      metadata: {
        billingDocument: row.billing_document,
        billingDocumentItem: row.billing_document_item,
        material: row.material,
        referenceDeliveryDocument: row.reference_sd_document,
        referenceDeliveryDocumentItem: row.reference_sd_document_item,
        billingQuantity: row.billing_quantity,
        billingQuantityUnit: row.billing_quantity_unit,
        netAmount: row.net_amount,
        transactionCurrency: row.transaction_currency,
      },
    };
  }

  private getJournalEntryNode(accountingDocument: string): GraphNode | null {
    const row = this.db
      .prepare(
        `
        SELECT accounting_document, company_code, fiscal_year, posting_date, customer,
               amount_in_transaction_currency, transaction_currency
        FROM journal_entry_items_ar
        WHERE accounting_document = ?
        LIMIT 1
      `,
      )
      .get(accountingDocument) as
      | {
          accounting_document: string;
          company_code: string;
          fiscal_year: string;
          posting_date: string | null;
          customer: string | null;
          amount_in_transaction_currency: number | null;
          transaction_currency: string | null;
        }
      | undefined;
    if (!row) return null;
    return {
      id: `JE:${row.accounting_document}`,
      entityType: "JournalEntry",
      label: `Journal Entry ${row.accounting_document}`,
      metadata: {
        accountingDocument: row.accounting_document,
        companyCode: row.company_code,
        fiscalYear: row.fiscal_year,
        postingDate: row.posting_date,
        customer: row.customer,
        amountInTransactionCurrency: row.amount_in_transaction_currency,
        transactionCurrency: row.transaction_currency,
      },
    };
  }

  private getPaymentNode(
    companyCode: string,
    fiscalYear: string,
    accountingDocument: string,
    accountingDocumentItem: string,
    customer: string,
  ): GraphNode | null {
    const row = this.db
      .prepare(
        `
        SELECT company_code, fiscal_year, accounting_document, accounting_document_item, customer,
               amount_in_transaction_currency, transaction_currency, clearing_accounting_document, clearing_date
        FROM payments_ar
        WHERE company_code = ? AND fiscal_year = ? AND accounting_document = ? AND accounting_document_item = ? AND customer = ?
      `,
      )
      .get(companyCode, fiscalYear, accountingDocument, accountingDocumentItem, customer) as
      | {
          company_code: string;
          fiscal_year: string;
          accounting_document: string;
          accounting_document_item: string;
          customer: string;
          amount_in_transaction_currency: number | null;
          transaction_currency: string | null;
          clearing_accounting_document: string | null;
          clearing_date: string | null;
        }
      | undefined;
    if (!row) return null;
    return {
      id: `PAY:${row.company_code}:${row.fiscal_year}:${row.accounting_document}:${row.accounting_document_item}:${row.customer}`,
      entityType: "Payment",
      label: `Payment ${row.accounting_document}`,
      metadata: {
        companyCode: row.company_code,
        fiscalYear: row.fiscal_year,
        accountingDocument: row.accounting_document,
        accountingDocumentItem: row.accounting_document_item,
        customer: row.customer,
        amountInTransactionCurrency: row.amount_in_transaction_currency,
        transactionCurrency: row.transaction_currency,
        clearingAccountingDocument: row.clearing_accounting_document,
        clearingDate: row.clearing_date,
      },
    };
  }

  private getCustomerNode(customer: string): GraphNode | null {
    const row = this.db
      .prepare(
        `
        SELECT customer, business_partner_name, business_partner_full_name, blocked, marked_for_archiving
        FROM business_partners
        WHERE customer = ?
      `,
      )
      .get(customer) as
      | {
          customer: string;
          business_partner_name: string | null;
          business_partner_full_name: string | null;
          blocked: number | null;
          marked_for_archiving: number | null;
        }
      | undefined;
    if (!row) return null;
    return {
      id: `C:${row.customer}`,
      entityType: "Customer",
      label: row.business_partner_name ?? `Customer ${row.customer}`,
      metadata: {
        customer: row.customer,
        businessPartnerName: row.business_partner_name,
        businessPartnerFullName: row.business_partner_full_name,
        blocked: row.blocked === 1,
        markedForArchiving: row.marked_for_archiving === 1,
      },
    };
  }

  private getProductNode(product: string): GraphNode | null {
    const productRow = this.db
      .prepare(
        `
        SELECT p.product, p.product_type, p.product_group, p.product_old_id, pd.product_description
        FROM products p
        LEFT JOIN product_descriptions pd ON pd.product = p.product AND pd.language = 'EN'
        WHERE p.product = ?
      `,
      )
      .get(product) as
      | {
          product: string;
          product_type: string | null;
          product_group: string | null;
          product_old_id: string | null;
          product_description: string | null;
        }
      | undefined;
    if (!productRow) return null;
    return {
      id: `P:${productRow.product}`,
      entityType: "Product",
      label: productRow.product_description ?? `Product ${productRow.product}`,
      metadata: {
        product: productRow.product,
        productType: productRow.product_type,
        productGroup: productRow.product_group,
        productOldId: productRow.product_old_id,
        productDescription: productRow.product_description,
      },
    };
  }

  private getPlantNode(plant: string): GraphNode | null {
    const row = this.db
      .prepare(
        `
        SELECT plant, plant_name, sales_organization, distribution_channel, division
        FROM plants
        WHERE plant = ?
      `,
      )
      .get(plant) as
      | {
          plant: string;
          plant_name: string | null;
          sales_organization: string | null;
          distribution_channel: string | null;
          division: string | null;
        }
      | undefined;
    if (!row) return null;
    return {
      id: `PLANT:${row.plant}`,
      entityType: "Plant",
      label: row.plant_name ?? `Plant ${row.plant}`,
      metadata: {
        plant: row.plant,
        plantName: row.plant_name,
        salesOrganization: row.sales_organization,
        distributionChannel: row.distribution_channel,
        division: row.division,
      },
    };
  }

  private getAddressNode(businessPartner: string, addressId: string): GraphNode | null {
    const row = this.db
      .prepare(
        `
        SELECT business_partner, address_id, city_name, country, region, postal_code, street_name
        FROM business_partner_addresses
        WHERE business_partner = ? AND address_id = ?
      `,
      )
      .get(businessPartner, addressId) as
      | {
          business_partner: string;
          address_id: string;
          city_name: string | null;
          country: string | null;
          region: string | null;
          postal_code: string | null;
          street_name: string | null;
        }
      | undefined;
    if (!row) return null;
    return {
      id: `ADDR:${row.business_partner}:${row.address_id}`,
      entityType: "Address",
      label: `Address ${row.address_id}`,
      metadata: {
        businessPartner: row.business_partner,
        addressId: row.address_id,
        cityName: row.city_name,
        country: row.country,
        region: row.region,
        postalCode: row.postal_code,
        streetName: row.street_name,
      },
    };
  }
}
