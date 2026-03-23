import cors from "cors";
import express from "express";
import { env } from "./config.js";

const app = express();

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
    milestone: 1,
    status: "in-progress",
    stack: {
      backend: "express-typescript",
      db: "sqlite-better-sqlite3",
      query: "kysely",
      llm: env.GEMINI_MODEL,
    },
  });
});

app.listen(env.PORT, () => {
  // Intentional concise startup log for local verification.
  console.log(`Backend listening on http://localhost:${env.PORT}`);
});
