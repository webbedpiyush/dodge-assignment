import { env } from "../config.js";
import { getSqlite } from "../db/client.js";
import { ingestDataset } from "../ingestion/ingest.js";

const db = getSqlite();

const summary = ingestDataset(db, {
  datasetDir: env.DATASET_DIR,
  reset: true,
});

console.log(JSON.stringify(summary, null, 2));
