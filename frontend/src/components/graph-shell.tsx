"use client";

import { Background, Controls, MiniMap, ReactFlow } from "@xyflow/react";
import "@xyflow/react/dist/style.css";

const nodes = [
  {
    id: "sales-order",
    position: { x: 120, y: 160 },
    data: { label: "Sales Order 740506" },
    style: { background: "#dbeafe", border: "1px solid #60a5fa" },
  },
  {
    id: "delivery",
    position: { x: 360, y: 90 },
    data: { label: "Delivery 80738076" },
    style: { background: "#e0f2fe", border: "1px solid #38bdf8" },
  },
  {
    id: "billing",
    position: { x: 360, y: 250 },
    data: { label: "Billing 90504298" },
    style: { background: "#fef3c7", border: "1px solid #f59e0b" },
  },
];

const edges = [
  { id: "e1", source: "sales-order", target: "delivery", animated: true },
  { id: "e2", source: "delivery", target: "billing" },
];

export function GraphShell() {
  return (
    <div className="relative h-full w-full rounded-lg border bg-white">
      <div className="absolute left-4 top-4 z-10 flex gap-2">
        <button className="rounded-md border bg-white px-3 py-1.5 text-sm">Minimize</button>
        <button className="rounded-md bg-black px-3 py-1.5 text-sm text-white">Hide Granular Overlay</button>
      </div>

      <div className="absolute left-1/2 top-24 z-10 w-72 -translate-x-1/2 rounded-xl border bg-white/95 p-4 shadow-sm">
        <p className="text-sm font-semibold">Journal Entry</p>
        <p className="mt-1 text-xs text-gray-600">Entity: JournalEntry</p>
        <p className="mt-2 text-xs">AccountingDocument: 9400635958</p>
        <p className="text-xs">ReferenceDocument: 91150187</p>
        <p className="text-xs">Connections: 2</p>
      </div>

      <ReactFlow fitView defaultNodes={nodes} defaultEdges={edges}>
        <Background gap={18} size={1} />
        <Controls />
        <MiniMap />
      </ReactFlow>
    </div>
  );
}
