export function ChatShell() {
  return (
    <aside className="flex h-full w-full flex-col rounded-lg border bg-white">
      <div className="border-b p-4">
        <p className="text-sm font-semibold">Chat with Graph</p>
        <p className="text-xs text-gray-500">Order to Cash</p>
      </div>

      <div className="space-y-4 p-4">
        <div>
          <p className="text-sm font-semibold">Graph Agent</p>
          <p className="mt-1 text-sm text-gray-700">
            Hi! I can help you analyze the Order to Cash process.
          </p>
        </div>
      </div>

      <div className="mt-auto border-t p-3">
        <div className="rounded-md border p-2">
          <p className="text-xs text-gray-500">Analyze anything</p>
          <div className="mt-2 flex items-center gap-2">
            <input
              className="h-9 w-full rounded border px-2 text-sm outline-none"
              placeholder="Find orders with missing billing"
              disabled
            />
            <button className="rounded bg-gray-700 px-3 py-2 text-xs text-white" disabled>
              Send
            </button>
          </div>
        </div>
      </div>
    </aside>
  );
}
