"use client";

import { type KeyboardEvent, useMemo, useState } from "react";
import { appConfig } from "@/lib/config";
import { deriveGraphNodeIdsFromRows } from "@/lib/query-highlights";

type QueryRow = Record<string, string | number | boolean | null>;

type QueryResponse = {
  ok: boolean;
  guardrailTriggered: boolean;
  message: string;
  intent?: string;
  sql?: string;
  rowCount?: number;
  rowsPreview?: QueryRow[];
  confidence?: number;
  source?: "gemini" | "fallback";
};

type ChatMessage =
  | { role: "assistant"; text: string }
  | {
      role: "result";
      prompt: string;
      payload: QueryResponse;
    };

type ChatShellProps = {
  onHighlightNodes: (nodeIds: string[]) => void;
};

export function ChatShell({ onHighlightNodes }: ChatShellProps) {
  const [prompt, setPrompt] = useState("");
  const [loading, setLoading] = useState(false);
  const [messages, setMessages] = useState<ChatMessage[]>([
    {
      role: "assistant",
      text: "Hi! I can help you analyze the Order to Cash process.",
    },
  ]);

  const canSend = useMemo(() => prompt.trim().length > 0 && !loading, [prompt, loading]);

  async function sendPrompt() {
    const currentPrompt = prompt.trim();
    if (!currentPrompt || loading) return;

    setLoading(true);
    setPrompt("");
    try {
      const response = await fetch(`${appConfig.apiBaseUrl}/query`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ prompt: currentPrompt }),
      });
      const payload = (await response.json()) as QueryResponse;
      setMessages((prev) => [...prev, { role: "result", prompt: currentPrompt, payload }]);

      const rows = payload.rowsPreview ?? [];
      const highlighted = deriveGraphNodeIdsFromRows(rows);
      onHighlightNodes(highlighted);
    } catch (error) {
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          text: `Request failed: ${error instanceof Error ? error.message : "Unknown error"}`,
        },
      ]);
      onHighlightNodes([]);
    } finally {
      setLoading(false);
    }
  }

  function onInputKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === "Enter") {
      event.preventDefault();
      void sendPrompt();
    }
  }

  return (
    <aside className="flex h-full w-full flex-col rounded-lg border bg-white">
      <div className="border-b p-4">
        <p className="text-sm font-semibold">Chat with Graph</p>
        <p className="text-xs text-gray-500">Order to Cash</p>
      </div>

      <div className="space-y-3 overflow-y-auto p-4">
        {messages.map((message, idx) => {
          if (message.role === "assistant") {
            return (
              <div key={`${message.role}-${idx}`}>
                <p className="text-sm font-semibold">Graph Agent</p>
                <p className="mt-1 text-sm text-gray-700">{message.text}</p>
              </div>
            );
          }

          return (
            <div key={`${message.role}-${idx}`} className="space-y-2">
              <div className="rounded-md bg-gray-900 p-2 text-xs text-white">{message.prompt}</div>
              <div className="rounded-md border bg-gray-50 p-2 text-xs">
                <p className="font-medium">Answer</p>
                <p className="mt-1 text-gray-700">{message.payload.message}</p>
                {message.payload.sql ? (
                  <details className="mt-2">
                    <summary className="cursor-pointer text-[11px] font-medium text-gray-600">
                      SQL + Preview ({message.payload.rowCount ?? 0} rows)
                    </summary>
                    <pre className="mt-1 max-h-36 overflow-auto rounded bg-white p-2 text-[10px]">
                      {message.payload.sql}
                    </pre>
                    {message.payload.rowsPreview?.length ? (
                      <pre className="mt-1 max-h-36 overflow-auto rounded bg-white p-2 text-[10px]">
                        {JSON.stringify(message.payload.rowsPreview.slice(0, 5), null, 2)}
                      </pre>
                    ) : null}
                  </details>
                ) : null}
                <p className="mt-2 text-[10px] text-gray-500">
                  Source: {message.payload.source ?? "-"} | Confidence:{" "}
                  {typeof message.payload.confidence === "number"
                    ? message.payload.confidence.toFixed(2)
                    : "-"}
                </p>
              </div>
            </div>
          );
        })}
      </div>

      <div className="mt-auto border-t p-3">
        <div className="rounded-md border p-2">
          <p className="text-xs text-gray-500">Analyze anything</p>
          <div className="mt-2 flex items-center gap-2">
            <input
              className="h-9 w-full rounded border px-2 text-sm outline-none"
              placeholder="Find orders with missing billing"
              value={prompt}
              onChange={(event) => setPrompt(event.target.value)}
              onKeyDown={onInputKeyDown}
              disabled={loading}
            />
            <button
              className="rounded bg-gray-700 px-3 py-2 text-xs text-white disabled:cursor-not-allowed disabled:bg-gray-400"
              disabled={!canSend}
              onClick={() => void sendPrompt()}
            >
              {loading ? "..." : "Send"}
            </button>
          </div>
        </div>
      </div>
    </aside>
  );
}
