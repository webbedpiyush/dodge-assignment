"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  type Edge,
  type Node,
  type NodeMouseHandler,
  useEdgesState,
  useNodesState,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { appConfig } from "@/lib/config";

type ApiNode = {
  id: string;
  entityType: string;
  label: string;
  metadata: Record<string, unknown>;
};

type ApiEdge = {
  id: string;
  source: string;
  target: string;
  relation: string;
};

type GraphShellProps = {
  highlightedNodeIds?: string[];
};

const colorByEntity: Record<string, { bg: string; border: string }> = {
  SalesOrder: { bg: "#dbeafe", border: "#60a5fa" },
  SalesOrderItem: { bg: "#e0e7ff", border: "#818cf8" },
  Delivery: { bg: "#e0f2fe", border: "#38bdf8" },
  DeliveryItem: { bg: "#cffafe", border: "#06b6d4" },
  BillingDocument: { bg: "#fef3c7", border: "#f59e0b" },
  BillingItem: { bg: "#fde68a", border: "#f59e0b" },
  JournalEntry: { bg: "#ede9fe", border: "#8b5cf6" },
  Payment: { bg: "#dcfce7", border: "#22c55e" },
  Customer: { bg: "#fee2e2", border: "#ef4444" },
  Product: { bg: "#fae8ff", border: "#d946ef" },
  Plant: { bg: "#ccfbf1", border: "#14b8a6" },
  Address: { bg: "#f3f4f6", border: "#9ca3af" },
};

function toFlowNode(node: ApiNode, index: number): Node {
  const palette = colorByEntity[node.entityType] ?? { bg: "#f3f4f6", border: "#9ca3af" };
  return {
    id: node.id,
    position: {
      x: 120 + (index % 6) * 220,
      y: 160 + Math.floor(index / 6) * 140,
    },
    data: {
      label: node.label,
      entityType: node.entityType,
    },
    style: {
      background: palette.bg,
      border: `1px solid ${palette.border}`,
      borderRadius: 10,
      fontSize: 12,
      width: 190,
    },
  };
}

function toFlowEdge(edge: ApiEdge): Edge {
  return {
    id: edge.id,
    source: edge.source,
    target: edge.target,
    label: edge.relation,
    style: { stroke: "#93c5fd", strokeWidth: 1.5 },
    labelStyle: { fill: "#6b7280", fontSize: 10 },
  };
}

function withHighlightStyle(node: Node, highlighted: Set<string>): Node {
  const entityType = String((node.data as { entityType?: string } | undefined)?.entityType ?? "");
  const palette = colorByEntity[entityType] ?? { bg: "#f3f4f6", border: "#9ca3af" };
  const isHighlighted = highlighted.has(node.id);
  return {
    ...node,
    style: {
      ...node.style,
      background: palette.bg,
      border: isHighlighted ? "2px solid #111827" : `1px solid ${palette.border}`,
      boxShadow: isHighlighted ? "0 0 0 3px rgba(17, 24, 39, 0.15)" : "none",
    },
  };
}

export function GraphShell({ highlightedNodeIds = [] }: GraphShellProps) {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [selectedNode, setSelectedNode] = useState<ApiNode | null>(null);
  const [loading, setLoading] = useState(true);

  const nodeById = useMemo(() => new Map(nodes.map((node) => [node.id, node])), [nodes]);

  const loadSeed = useCallback(async () => {
    setLoading(true);
    try {
      const response = await fetch(`${appConfig.apiBaseUrl}/graph/seed?limit=24`);
      const payload = (await response.json()) as { nodes: ApiNode[]; ok: boolean };
      if (!payload.ok) return;
      const highlightedSet = new Set(highlightedNodeIds);
      setNodes(payload.nodes.map((node, index) => withHighlightStyle(toFlowNode(node, index), highlightedSet)));
    } finally {
      setLoading(false);
    }
  }, [highlightedNodeIds, setNodes]);

  useEffect(() => {
    loadSeed();
  }, [loadSeed]);

  useEffect(() => {
    const highlightedSet = new Set(highlightedNodeIds);
    setNodes((current) => current.map((node) => withHighlightStyle(node, highlightedSet)));
  }, [highlightedNodeIds, setNodes]);

  useEffect(() => {
    async function ensureHighlightedNodesLoaded() {
      if (!highlightedNodeIds.length) return;

      const missing = highlightedNodeIds.filter((id) => !nodeById.has(id)).slice(0, 10);
      if (!missing.length) return;

      const loadedNodes: ApiNode[] = [];
      for (const id of missing) {
        const response = await fetch(`${appConfig.apiBaseUrl}/graph/node/${encodeURIComponent(id)}`);
        if (!response.ok) continue;
        const payload = (await response.json()) as { ok: boolean; node?: ApiNode };
        if (payload.ok && payload.node) loadedNodes.push(payload.node);
      }

      if (loadedNodes.length) {
        setNodes((current) => {
          const existing = new Set(current.map((n) => n.id));
          const highlightedSet = new Set(highlightedNodeIds);
          const next = [...current];
          for (const apiNode of loadedNodes) {
            if (existing.has(apiNode.id)) continue;
            next.push(withHighlightStyle(toFlowNode(apiNode, next.length), highlightedSet));
          }
          return next;
        });
      }
    }

    void ensureHighlightedNodesLoaded();
  }, [highlightedNodeIds, nodeById, setNodes]);

  const onNodeClick: NodeMouseHandler = useCallback(
    async (_evt, node) => {
      const nodeRes = await fetch(`${appConfig.apiBaseUrl}/graph/node/${encodeURIComponent(node.id)}`);
      const nodePayload = (await nodeRes.json()) as { ok: boolean; node?: ApiNode };
      if (nodePayload.ok && nodePayload.node) {
        setSelectedNode(nodePayload.node);
      }

      const response = await fetch(
        `${appConfig.apiBaseUrl}/graph/neighbors/${encodeURIComponent(node.id)}?limit=35`,
      );
      const payload = (await response.json()) as {
        ok: boolean;
        nodes: ApiNode[];
        edges: ApiEdge[];
      };
      if (!payload.ok) return;

      setNodes((current) => {
        const existing = new Map(current.map((n) => [n.id, n]));
        const highlightedSet = new Set(highlightedNodeIds);
        const next = [...current];
        for (const apiNode of payload.nodes) {
          if (existing.has(apiNode.id)) continue;
          next.push(withHighlightStyle(toFlowNode(apiNode, next.length), highlightedSet));
        }
        return next;
      });

      setEdges((current) => {
        const existing = new Set(current.map((e) => e.id));
        const next = [...current];
        for (const apiEdge of payload.edges) {
          if (existing.has(apiEdge.id)) continue;
          const sourceExists = nodeById.has(apiEdge.source) || payload.nodes.some((n) => n.id === apiEdge.source);
          const targetExists = nodeById.has(apiEdge.target) || payload.nodes.some((n) => n.id === apiEdge.target);
          if (!sourceExists || !targetExists) continue;
          next.push(toFlowEdge(apiEdge));
        }
        return next;
      });
    },
    [highlightedNodeIds, nodeById, setEdges, setNodes],
  );

  return (
    <div className="relative h-full w-full rounded-lg border bg-white">
      <div className="absolute left-4 top-4 z-10 flex gap-2">
        <button className="rounded-md border bg-white px-3 py-1.5 text-sm">Minimize</button>
        <button className="rounded-md bg-black px-3 py-1.5 text-sm text-white">Hide Granular Overlay</button>
      </div>

      {selectedNode ? (
        <div className="absolute left-1/2 top-24 z-10 w-80 -translate-x-1/2 rounded-xl border bg-white/95 p-4 shadow-sm">
          <p className="text-sm font-semibold">{selectedNode.label}</p>
          <p className="mt-1 text-xs text-gray-600">Entity: {selectedNode.entityType}</p>
          <div className="mt-2 space-y-1 text-xs">
            {Object.entries(selectedNode.metadata)
              .slice(0, 6)
              .map(([key, value]) => (
                <p key={key}>
                  <span className="font-medium">{key}:</span> {String(value ?? "-")}
                </p>
              ))}
          </div>
        </div>
      ) : null}

      {loading ? (
        <div className="absolute left-1/2 top-20 z-10 -translate-x-1/2 rounded bg-white px-3 py-1 text-xs shadow">
          Loading seed graph...
        </div>
      ) : null}

      <ReactFlow
        fitView
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={onNodeClick}
      >
        <Background gap={18} size={1} />
        <Controls />
        <MiniMap />
      </ReactFlow>
    </div>
  );
}
