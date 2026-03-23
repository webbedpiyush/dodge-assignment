import dotenv from "dotenv";
import path from "node:path";
import { z } from "zod";

dotenv.config();

const schema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  PORT: z.coerce.number().default(4000),
  FRONTEND_ORIGIN: z.string().default("http://localhost:3000"),
  SQLITE_PATH: z.string().default(path.resolve(process.cwd(), "../data/o2c.sqlite")),
  DATASET_DIR: z.string().default(path.resolve(process.cwd(), "../sap-o2c-data")),
  GEMINI_API_KEY: z.string().optional(),
  GEMINI_MODEL: z.string().default("gemini-2.5-flash"),
});

export const env = schema.parse(process.env);
