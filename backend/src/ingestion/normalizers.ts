type MaybeRecord = Record<string, unknown>;

export function getString(record: MaybeRecord, key: string): string | null {
  const value = record[key];
  if (value === undefined || value === null) {
    return null;
  }

  const text = String(value).trim();
  return text.length > 0 ? text : null;
}

export function getNumber(record: MaybeRecord, key: string): number | null {
  const text = getString(record, key);
  if (!text) {
    return null;
  }
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
}

export function getBoolean(record: MaybeRecord, key: string): number | null {
  const value = record[key];
  if (value === undefined || value === null) {
    return null;
  }
  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }
  const text = String(value).trim().toLowerCase();
  if (text === "true") {
    return 1;
  }
  if (text === "false") {
    return 0;
  }
  return null;
}

export function normalizeDocItem(value: string | null): string | null {
  if (!value) {
    return null;
  }
  const compact = value.trim();
  if (/^\d+$/.test(compact)) {
    return compact.padStart(6, "0");
  }
  return compact;
}

export function normalizeDocNumber(value: string | null): string | null {
  if (!value) {
    return null;
  }
  return value.trim();
}

export function toRawJson(record: MaybeRecord): string {
  return JSON.stringify(record);
}
