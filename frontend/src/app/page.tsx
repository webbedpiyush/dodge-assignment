import { ChatShell } from "@/components/chat-shell";
import { GraphShell } from "@/components/graph-shell";

export default function HomePage() {
  return (
    <main className="h-screen w-screen p-4">
      <div className="mb-3 text-sm text-gray-500">
        <span>Mapping</span> / <span className="font-semibold text-gray-800">Order to Cash</span>
      </div>
      <div className="grid h-[calc(100%-2rem)] grid-cols-[1fr_340px] gap-4">
        <GraphShell />
        <ChatShell />
      </div>
    </main>
  );
}
