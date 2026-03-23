import cors from "cors";
import express from "express";
import { env } from "./config.js";
import { getSqlite } from "./db/client.js";
import { ingestDataset } from "./ingestion/ingest.js";

const app = express();
const db = getSqlite();

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
    milestone: 2,
    status: "completed",
    nextMilestone: 3,
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

app.listen(env.PORT, () => {
  // Intentional concise startup log for local verification.
  console.log(`Backend listening on http://localhost:${env.PORT}`);
});
