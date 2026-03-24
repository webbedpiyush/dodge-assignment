export type EntityType =
  | "SalesOrder"
  | "SalesOrderItem"
  | "Delivery"
  | "DeliveryItem"
  | "BillingDocument"
  | "BillingItem"
  | "JournalEntry"
  | "Payment"
  | "Customer"
  | "Product"
  | "Plant"
  | "Address";

export type GraphNode = {
  id: string;
  entityType: EntityType;
  label: string;
  metadata: Record<string, unknown>;
};

export type GraphEdge = {
  id: string;
  source: string;
  target: string;
  relation: string;
};

export type GraphNeighborhood = {
  center: GraphNode | null;
  nodes: GraphNode[];
  edges: GraphEdge[];
};
