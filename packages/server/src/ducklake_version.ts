// DuckLake catalog-format version contract.
//
// A DuckLake catalog records the on-disk format version it was written at. The
// DuckLake extension bundled with a given DuckDB engine attaches only a bounded
// RANGE of catalog formats without migration; a catalog outside that range
// fails deep inside DuckDB with an opaque error. This module derives the
// supported range purely from the pinned DuckDB engine version, so it never has
// to enumerate an allow-list of "known good" catalog versions (which drifts
// silently on an engine bump and is easy to forget to update). Both the runtime
// attach preflight (`service/connection.ts`) and the CI version-contract check
// (`scripts/validate-ducklake-catalog-range.ts`) consume this one source.
//
// Lower bound: fixed at "1.0". The 1.x DuckLake line does not attach the older
// 0.x formats without AUTOMATIC_MIGRATION, and the Publisher attach paths never
// pass that flag, so 1.0 is the floor. Upper bound: the maximum format the
// engine's bundled extension writes/attaches, looked up in the migration matrix
// below and keyed by engine (major, minor).

/** A parsed DuckLake catalog format version, e.g. `1.0` or `0.3-dev1`. */
export interface CatalogFormat {
   major: number;
   minor: number;
   /** e.g. `dev1` in `0.3-dev1`; a pre-release sorts BELOW the same release. */
   prerelease?: string;
}

/** The inclusive `[min, max]` catalog-format range for a given engine. */
export interface CatalogFormatRange {
   min: string;
   max: string;
   /** The engine minor line the range was derived from (for error messages). */
   engineVersion: string;
}

/** Fixed lower bound of the supported range (see file header). */
export const MIN_CATALOG_FORMAT = "1.0";

/**
 * Migration matrix: DuckDB engine minor line -> the maximum DuckLake catalog
 * format its bundled DuckLake extension attaches WITHOUT migration. This is NOT
 * an allow-list of catalog versions -- it is the single fact, per engine minor
 * line, from which the `[MIN_CATALOG_FORMAT, maxFormat]` range is derived.
 *
 * Verified against the baked extension: DuckDB 1.5.x's DuckLake extension writes
 * and attaches the "1.0" catalog format (the older 0.x formats require
 * AUTOMATIC_MIGRATION, which Publisher never passes). When a Malloy bump moves
 * `@duckdb/node-api` to a new minor line, add its row here after verifying the
 * extension's max attach-without-migration format -- the CI version-contract
 * check FAILS THE BUILD until a row covers the pinned engine, so this never
 * drifts silently.
 */
export const ENGINE_MAX_FORMAT: ReadonlyArray<{
   engine: { major: number; minor: number };
   maxFormat: string;
}> = [{ engine: { major: 1, minor: 5 }, maxFormat: "1.0" }];

const CATALOG_FORMAT_RE = /^(\d+)\.(\d+)(?:-([0-9A-Za-z.]+))?$/;

/**
 * Parse a catalog format string into its components, or `null` if it is not a
 * valid `<major>.<minor>[-<prerelease>]` format. A three-part version like
 * `1.0.0` is intentionally rejected: catalog formats are two-part, and a
 * three-part string is a malformed input, not a newer format.
 */
export function parseCatalogFormat(raw: string): CatalogFormat | null {
   if (typeof raw !== "string") return null;
   const m = CATALOG_FORMAT_RE.exec(raw);
   if (!m) return null;
   const format: CatalogFormat = { major: Number(m[1]), minor: Number(m[2]) };
   if (m[3] !== undefined) format.prerelease = m[3];
   return format;
}

/**
 * Compare two parsed catalog formats: -1 if `a < b`, 0 if equal, 1 if `a > b`.
 * A release sorts ABOVE its own pre-releases (`1.0` > `1.0-dev1`), matching
 * semver ordering.
 */
export function compareCatalogFormat(
   a: CatalogFormat,
   b: CatalogFormat,
): number {
   if (a.major !== b.major) return a.major < b.major ? -1 : 1;
   if (a.minor !== b.minor) return a.minor < b.minor ? -1 : 1;
   if (a.prerelease === b.prerelease) return 0;
   if (a.prerelease === undefined) return 1; // release > pre-release
   if (b.prerelease === undefined) return -1;
   return a.prerelease < b.prerelease ? -1 : 1;
}

/**
 * Extract the `{ major, minor }` line from a DuckDB engine version string
 * (`1.5.3`, `1.5.3-r.2`, `v1.5.3` all parse to `{1, 5}`), or `null` if none is
 * present.
 */
export function parseEngineMajorMinor(
   raw: string,
): { major: number; minor: number } | null {
   const m = /(\d+)\.(\d+)\.(\d+)/.exec(String(raw));
   if (!m) return null;
   return { major: Number(m[1]), minor: Number(m[2]) };
}

/**
 * The supported catalog-format range for a DuckDB engine version, or `null`
 * when the engine's minor line has no row in {@link ENGINE_MAX_FORMAT}. A
 * `null` return is the DRIFT signal: the CI version-contract check fails the
 * build on it, and the runtime preflight treats it as "unknown, skip" (the
 * preflight is non-load-bearing). The match is by `(major, minor)` so any patch
 * release of a known minor line resolves.
 */
export function catalogFormatRangeForEngine(
   engineVersion: string,
): CatalogFormatRange | null {
   const e = parseEngineMajorMinor(engineVersion);
   if (!e) return null;
   const entry = ENGINE_MAX_FORMAT.find(
      (r) => r.engine.major === e.major && r.engine.minor === e.minor,
   );
   if (!entry) return null;
   return {
      min: MIN_CATALOG_FORMAT,
      max: entry.maxFormat,
      engineVersion: `${e.major}.${e.minor}`,
   };
}

/** Whether `format` falls within the inclusive range (invalid input -> false). */
export function isCatalogFormatInRange(
   format: string,
   range: CatalogFormatRange,
): boolean {
   const f = parseCatalogFormat(format);
   const min = parseCatalogFormat(range.min);
   const max = parseCatalogFormat(range.max);
   if (!f || !min || !max) return false;
   return (
      compareCatalogFormat(f, min) >= 0 && compareCatalogFormat(f, max) <= 0
   );
}

/**
 * Whether a catalog `format` is supported by the DuckLake extension bundled
 * with `engineVersion`. Returns `false` for an unknown engine (no matrix row)
 * or a malformed format.
 */
export function isCatalogFormatSupportedByEngine(
   format: string,
   engineVersion: string,
): boolean {
   const range = catalogFormatRangeForEngine(engineVersion);
   return range ? isCatalogFormatInRange(format, range) : false;
}
