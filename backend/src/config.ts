import dotenv from "dotenv";
import fs from "node:fs";
import path from "node:path";
import { z } from "zod";

const dotenvCandidates = [
  path.resolve(process.cwd(), ".env"),
  path.resolve(process.cwd(), "../.env"),
];
const dotenvPath = dotenvCandidates.find((candidate) => fs.existsSync(candidate));
dotenv.config(dotenvPath ? { path: dotenvPath } : undefined);
const dotenvBaseDir = dotenvPath ? path.dirname(dotenvPath) : process.cwd();

const schema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  PORT: z.coerce.number().default(4000),
  FRONTEND_ORIGIN: z.string().default("http://localhost:3000"),
  SQLITE_PATH: z.string().default(path.resolve(dotenvBaseDir, "data/o2c.sqlite")),
  DATASET_DIR: z.string().default(path.resolve(dotenvBaseDir, "sap-o2c-data")),
  GEMINI_API_KEY: z.string().optional(),
  GEMINI_MODEL: z.string().default("gemini-2.5-flash"),
});

const parsed = schema.parse(process.env);

function resolveMaybeRelativePath(inputPath: string): string {
  if (path.isAbsolute(inputPath)) {
    return inputPath;
  }
  return path.resolve(dotenvBaseDir, inputPath);
}

export const env = {
  ...parsed,
  SQLITE_PATH: resolveMaybeRelativePath(parsed.SQLITE_PATH),
  DATASET_DIR: resolveMaybeRelativePath(parsed.DATASET_DIR),
};
