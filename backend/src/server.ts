import cors from "cors";
import express from "express";
import { env } from "./config.js";
import { getSqlite } from "./db/client.js";
import { GraphService } from "./graph/service.js";
import { ingestDataset } from "./ingestion/ingest.js";

const app = express();
const db = getSqlite();
const graphService = new GraphService(db);

app.use(
  cors({
    origin: env.FRONTEND_ORIGIN,
  }),
);
app.use(express.json({ limit: "1mb" }));

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "o2c-backend",
    env: env.NODE_ENV,
  });
});

app.get("/api/milestone-status", (_req, res) => {
  res.json({
    milestone: 3,
    status: "completed",
    nextMilestone: 4,
    stack: {
      backend: "express-typescript",
      db: "sqlite-better-sqlite3",
      query: "kysely",
      llm: env.GEMINI_MODEL,
    },
  });
});

app.post("/ingest", (req, res) => {
  const reset = req.body?.reset ?? true;
  const datasetDir = req.body?.datasetDir ?? env.DATASET_DIR;

  try {
    const summary = ingestDataset(db, { datasetDir, reset: Boolean(reset) });
    res.json({
      ok: true,
      summary,
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: "Failed to ingest dataset",
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

app.get("/graph/seed", (req, res) => {
  const limit = Number(req.query.limit ?? 30);
  const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.min(200, Math.floor(limit))) : 30;
  const nodes = graphService.getSeedNodes(safeLimit);

  res.json({
    ok: true,
    nodes,
  });
});

app.get("/graph/node/:nodeId", (req, res) => {
  const node = graphService.getNode(req.params.nodeId);
  if (!node) {
    res.status(404).json({
      ok: false,
      message: "Node not found",
    });
    return;
  }

  res.json({
    ok: true,
    node,
  });
});

app.get("/graph/neighbors/:nodeId", (req, res) => {
  const limit = Number(req.query.limit ?? 50);
  const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.min(200, Math.floor(limit))) : 50;
  const neighborhood = graphService.getNeighborhood(req.params.nodeId, safeLimit);
  if (!neighborhood.center) {
    res.status(404).json({
      ok: false,
      message: "Node not found",
    });
    return;
  }

  res.json({
    ok: true,
    ...neighborhood,
  });
});

app.listen(env.PORT, () => {
  // Intentional concise startup log for local verification.
  console.log(`Backend listening on http://localhost:${env.PORT}`);
});
