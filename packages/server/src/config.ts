// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { components } from "./api";
import {
   API_PREFIX,
   DEFAULT_MAX_CONCURRENT_QUERIES,
   DEFAULT_MAX_QUERY_ROWS,
   DEFAULT_MAX_RESPONSE_BYTES,
   DEFAULT_POSTGRES_POOL_MAX,
   DEFAULT_QUERY_ROW_LIMIT,
   DEFAULT_QUERY_TIMEOUT_MS,
   PUBLISHER_CONFIG_NAME,
} from "./constants";
import { logger } from "./logger";
import { defaultOpenAiBaseUrl } from "./providers/openai_compatible";
import type {
   EmbeddingSettings,
   LlmSettings,
   ProviderName,
} from "./providers/types";
import {
   DEFAULT_SEMANTIC_INDEX_MAX_ENTITIES,
   parseRetrievalConfig,
   type RetrievalConfig,
   type RetrievalEmbeddingConfig,
   type RetrievalLlmConfig,
} from "./retrieval_config";

// Re-exported: callers import these from here.
export {
   DEFAULT_SEMANTIC_INDEX_MAX_ENTITIES,
   parseRetrievalConfig,
   type RetrievalConfig,
};
import { accessSync, constants as fsConstants, mkdirSync } from "node:fs";

/**
 * The sample config shipped inside the package, so a zero-argument
 * `npx @malloy-publisher/server` boots. `build.ts` copies it next to the
 * running module; in a checkout it lives beside this file.
 */
const BUNDLED_DEFAULT_CONFIG_PATH = path.join(
   path.dirname(fileURLToPath(import.meta.url)),
   "default-publisher.config.json",
);

/**
 * Decide which `publisher.config.json` to read.
 *
 * Precedence:
 *   1. `--config <path>` (surfaced via `process.env.PUBLISHER_CONFIG_PATH`)
 *   2. `<serverRoot>/publisher.config.json`
 *   3. The bundled default, ONLY when `PUBLISHER_USE_BUNDLED_DEFAULT` is
 *      "true" (set for a zero-flag CLI run), so programmatic callers get no
 *      surprise filesystem fallback.
 *
 * Returns `null` if step 1 names a missing file: an explicit mistake to
 * surface, not to fall back from.
 */
function resolvePublisherConfigPath(serverRoot: string): {
   path: string;
   isBundledDefault: boolean;
} | null {
   const explicitPath = process.env.PUBLISHER_CONFIG_PATH;
   if (explicitPath && explicitPath.length > 0) {
      if (!fs.existsSync(explicitPath)) {
         return null;
      }
      return { path: explicitPath, isBundledDefault: false };
   }

   const serverRootPath = path.join(serverRoot, PUBLISHER_CONFIG_NAME);
   if (fs.existsSync(serverRootPath)) {
      return { path: serverRootPath, isBundledDefault: false };
   }

   if (
      process.env.PUBLISHER_USE_BUNDLED_DEFAULT === "true" &&
      fs.existsSync(BUNDLED_DEFAULT_CONFIG_PATH)
   ) {
      return { path: BUNDLED_DEFAULT_CONFIG_PATH, isBundledDefault: true };
   }

   return null;
}

// Home paths are POSIX form only: `~/` expands, while bare `~`, `~user/…`,
// and Windows-style `~\` are not local paths and are rejected downstream.
type FilesystemPath =
   | `./${string}`
   | `../${string}`
   | `/${string}`
   | `~/${string}`;
type GcsPath = `gs://${string}`;
type ApiConnection = components["schemas"]["Connection"];
export type Theme = components["schemas"]["Theme"];

/**
 * Palette colour keys that have separate light/dark variants. Hand-copied
 * from `packages/sdk/src/theme/keys.ts` because the server and SDK share only
 * api-doc.yaml; `theme_key_parity.spec.ts` fails if the copies drift.
 */
export const PER_MODE_COLOR_KEYS = [
   "background",
   "tableHeader",
   "tableHeaderBackground",
   "tableBody",
   "tile",
   "tileTitle",
   "mapColor",
   "border",
   "cardBorder",
   "axis",
   "gridline",
   "chartText",
   "value",
] as const;

export type Package = {
   name: string;
   location: FilesystemPath | GcsPath;
};

export type Connection = {
   name: string;
   type: string;
};

export type Environment = {
   name: string;
   theme?: Theme;
   packages: Package[];
   connections?: Connection[];
   storageDestinations?: Connection[];
};

/** The `mcp` block of publisher.config.json: settings for the MCP server. */
export type McpConfig = {
   /**
    * Offer `includeHiddenFilesAndSources` on MCP `execute_query`. Default false.
    * See {@link isMcpIncludeHiddenFilesAndSources}.
    */
   includeHiddenFilesAndSources?: boolean;
};

export type PublisherConfig = {
   frozenConfig: boolean;
   theme?: Theme;
   mcp?: McpConfig;
   retrieval?: RetrievalConfig;
   versionPromotion?: VersionPromotionMode;
   environments: Environment[];
};

/**
 * `PUBLISHER_PACKAGE_VERSIONING`: `on` or `off` (the default). A TRANSITION
 * flag, read from the environment only, and checked in one place: the publish
 * from a location. Off, that publish installs the package in place as before
 * versions; on, it publishes the version its publisher.json declares and
 * refuses a tree that declares none. #1381 removes the flag and the off branch
 * together, once every deployment runs with it on.
 */
export type PackageVersioningMode = "off" | "on";

const PACKAGE_VERSIONING_MODES: readonly PackageVersioningMode[] = [
   "off",
   "on",
];

/**
 * Who moves a package's `latest` version.
 *
 *  - `on-publish` (default): a publish makes the new version `latest` unless a
 *    higher version already is.
 *  - `explicit`: a publish never moves it; only `PUT .../packages/{name}/latest`
 *    does. For an orchestrator that decides when a version is ready to serve.
 */
export type VersionPromotionMode = "on-publish" | "explicit";

const VERSION_PROMOTION_MODES: readonly VersionPromotionMode[] = [
   "on-publish",
   "explicit",
];

export type ProcessedEnvironment = {
   name: string;
   theme?: Theme;
   packages: Package[];
   connections: ApiConnection[];
   /**
    * Unfiltered: every path onto an Environment, config or request body,
    * validates destinations in `processStorageDestinations`.
    */
   storageDestinations: ApiConnection[];
};

export type ProcessedPublisherConfig = {
   frozenConfig: boolean;
   theme?: Theme;
   environments: ProcessedEnvironment[];
};

/**
 * Tunables for {@link PackageMemoryGovernor}, parsed by
 * {@link getMemoryGovernorConfig}. The governor is admission control only: it
 * toggles back-pressure on an RSS hysteresis band and never evicts or
 * interrupts loaded packages.
 */
export interface MemoryGovernorConfig {
   /** Hard ceiling for process RSS in bytes (the OOM-relevant figure). */
   maxMemoryBytes: number;
   /** Fraction of `maxMemoryBytes` at which the governor activates back-pressure (new package loads start returning HTTP 503). Must be in (0, 1) and strictly greater than `lowWaterFraction`. */
   highWaterFraction: number;
   /** Fraction of `maxMemoryBytes` at which the governor clears back-pressure (new package loads admitted again). Must be in (0, 1) and strictly less than `highWaterFraction`; the gap is the hysteresis band that prevents flap. */
   lowWaterFraction: number;
   /** Polling cadence for the RSS sampler, in milliseconds. */
   checkIntervalMs: number;
   /** When false, the governor samples and emits metrics but never rejects: a monitoring-only rollout. */
   backpressureEnabled: boolean;
}

const DEFAULT_HIGH_WATER_FRACTION = 0.8;
const DEFAULT_LOW_WATER_FRACTION = 0.7;
const DEFAULT_CHECK_INTERVAL_MS = 5_000;
const MIN_CHECK_INTERVAL_MS = 100;

function parseIntEnv(name: string): number | undefined {
   const raw = process.env[name];
   if (raw === undefined || raw.trim() === "") return undefined;
   const value = Number.parseInt(raw, 10);
   if (!Number.isFinite(value) || String(value) !== raw.trim()) {
      throw new Error(
         `Invalid value for ${name}: expected a base-10 integer, got "${raw}"`,
      );
   }
   return value;
}

function parseFloatEnv(name: string, example: string): number | undefined {
   const raw = process.env[name];
   if (raw === undefined || raw.trim() === "") return undefined;
   const trimmed = raw.trim();
   // Match the whole string: parseFloat reads "0.5abc" as 0.5. A round-trip
   // check like parseIntEnv's would reject "0.50", ".5" and "1e-3".
   const invalid = () =>
      new Error(
         `Invalid value for ${name}: expected a finite number, got "${raw}". ` +
            `Fix: ${name}=${example}`,
      );
   if (!/^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/.test(trimmed)) {
      throw invalid();
   }
   const value = Number.parseFloat(trimmed);
   if (!Number.isFinite(value)) {
      throw invalid();
   }
   return value;
}

export function parseBoolEnv(name: string): boolean | undefined {
   return parseBoolString(process.env[name], name);
}

/** A boolean as a variable spells it: `1/true/yes/on`, `0/false/no/off`, any case; blank is unset. */
function parseBoolString(
   raw: string | undefined,
   name: string,
): boolean | undefined {
   if (raw === undefined || raw.trim() === "") return undefined;
   const normalised = raw.trim().toLowerCase();
   if (["1", "true", "yes", "on"].includes(normalised)) return true;
   if (["0", "false", "no", "off"].includes(normalised)) return false;
   throw new Error(
      `Invalid value for ${name}: expected a boolean (true/false), got "${raw}"`,
   );
}

/**
 * Memory-governor settings from the environment, or `null` when
 * `PUBLISHER_MAX_MEMORY_BYTES` is unset or `0`. Throws on malformed input so
 * a manifest typo fails loudly instead of silently disabling the cap.
 */
export const getMemoryGovernorConfig = (): MemoryGovernorConfig | null => {
   const maxMemoryBytes = parseIntEnv("PUBLISHER_MAX_MEMORY_BYTES");
   if (maxMemoryBytes === undefined || maxMemoryBytes === 0) {
      return null;
   }
   if (maxMemoryBytes < 0) {
      throw new Error(
         `PUBLISHER_MAX_MEMORY_BYTES must be a positive integer (got ${maxMemoryBytes})`,
      );
   }

   const highWaterFraction =
      parseFloatEnv("PUBLISHER_MEMORY_HIGH_WATER_FRACTION", "0.8") ??
      DEFAULT_HIGH_WATER_FRACTION;
   const lowWaterFraction =
      parseFloatEnv("PUBLISHER_MEMORY_LOW_WATER_FRACTION", "0.7") ??
      DEFAULT_LOW_WATER_FRACTION;
   const checkIntervalMs =
      parseIntEnv("PUBLISHER_MEMORY_CHECK_INTERVAL_MS") ??
      DEFAULT_CHECK_INTERVAL_MS;
   const backpressureEnabled =
      parseBoolEnv("PUBLISHER_MEMORY_BACKPRESSURE") ?? true;

   if (highWaterFraction <= 0 || highWaterFraction >= 1) {
      throw new Error(
         `PUBLISHER_MEMORY_HIGH_WATER_FRACTION must be in (0, 1) (got ${highWaterFraction})`,
      );
   }
   if (lowWaterFraction <= 0 || lowWaterFraction >= 1) {
      throw new Error(
         `PUBLISHER_MEMORY_LOW_WATER_FRACTION must be in (0, 1) (got ${lowWaterFraction})`,
      );
   }
   if (lowWaterFraction >= highWaterFraction) {
      throw new Error(
         `PUBLISHER_MEMORY_LOW_WATER_FRACTION (${lowWaterFraction}) must be strictly less than PUBLISHER_MEMORY_HIGH_WATER_FRACTION (${highWaterFraction})`,
      );
   }
   if (checkIntervalMs < MIN_CHECK_INTERVAL_MS) {
      throw new Error(
         `PUBLISHER_MEMORY_CHECK_INTERVAL_MS must be >= ${MIN_CHECK_INTERVAL_MS} (got ${checkIntervalMs})`,
      );
   }

   return {
      maxMemoryBytes,
      highWaterFraction,
      lowWaterFraction,
      checkIntervalMs,
      backpressureEnabled,
   };
};

/**
 * Whether the operator opted out with `off`, as distinct from never setting
 * the variable: both mean "no limit", but only the unset case deserves the
 * startup warning about unbounded instances.
 */
export const isDuckDBMemoryLimitDisabled = (): boolean =>
   process.env.PUBLISHER_DUCKDB_MEMORY_LIMIT?.trim().toLowerCase() === "off";

/**
 * DuckDB `memory_limit` for every Publisher-owned DuckDB session and instance,
 * as a FLAT value (`'1GB'`); `off` or unset leaves DuckDB's default. Absolute,
 * not derived from the container: DuckDB sizes its default per instance, so N
 * instances overcommit N-fold, and N is unknown when a session opens. See
 * docs/configuration.md.
 */
export const getDuckDBMemoryLimit = (): string | undefined => {
   const raw = process.env.PUBLISHER_DUCKDB_MEMORY_LIMIT?.trim();
   if (raw === undefined || raw === "" || raw.toLowerCase() === "off") {
      return undefined;
   }
   return raw;
};

/**
 * Bytes DuckLake buffers per column before flushing a Parquet row group
 * (`PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES`, e.g. `16MB`); unset issues
 * nothing on attach. Write memory scales with table WIDTH and sits outside
 * `memory_limit`; bytes rather than rows so one value suits narrow and wide
 * tables. See docs/configuration.md.
 */
export const getDuckLakeRowGroupSizeBytes = (): string | undefined => {
   const raw = process.env.PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES?.trim();
   return raw === undefined || raw === "" ? undefined : raw;
};

/**
 * Bound on how large a Parquet file DuckLake writes before rotating to the next
 * one (`PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES`, e.g. `256MB`). Unset leaves
 * DuckLake's own default and issues nothing at all on attach.
 *
 * Writing to object storage keeps a file's bytes resident until it completes,
 * outside `memory_limit`, so peak memory tracks FILE size. A separate term from
 * {@link getDuckLakeRowGroupSizeBytes}, which bounds the buffer within a file.
 * Want the largest value that clears the memory ceiling; see
 * docs/configuration.md.
 */
export const getDuckLakeTargetFileSizeBytes = (): string | undefined => {
   const raw = process.env.PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES?.trim();
   return raw === undefined || raw === "" ? undefined : raw;
};

/**
 * How many rows the appender buffers before a partitioned storage build flushes
 * them to the partition files (`partitioned_write_flush_threshold`,
 * `PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD`). Default 8192 rows. `off` turns
 * the whole partitioned-insert treatment off -- this threshold, the ordered
 * insert and the single appender thread together -- and leaves DuckDB's own
 * behaviour, which is the one the treatment exists to avoid; it is the escape
 * hatch for a source that built fine before, at the cost of the sort.
 *
 * A partitioned COPY buffers rows per partition per thread inside the buffer
 * manager, so this term fails as `failed to pin block`. The threshold bounds
 * rows buffered before a flush; the per-partition up-front cost is bounded by
 * the ordered, single-threaded insert (`orderByPartitionColumns` in
 * service/materialization_build_session.ts). Neither half suffices alone. See
 * docs/configuration.md.
 */
export const DEFAULT_PARTITIONED_WRITE_FLUSH_THRESHOLD = 8192;

export const getPartitionedWriteFlushThreshold = (): number | undefined => {
   const raw = process.env.PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD?.trim();
   if (raw === undefined || raw === "") {
      return DEFAULT_PARTITIONED_WRITE_FLUSH_THRESHOLD;
   }
   if (raw.toLowerCase() === "off") {
      return undefined;
   }
   return Number(raw);
};

/**
 * Directory DuckDB spills to. A materialization build overrides this with its
 * own disposable working directory; every other session and instance uses this.
 *
 * No `off` sentinel, unlike {@link getDuckDBMemoryLimit}: a directory named
 * `off` is a legal path, and unset already means "DuckDB's default".
 */
export const getDuckDBTempDirectory = (): string | undefined => {
   const raw = process.env.PUBLISHER_DUCKDB_TEMP_DIRECTORY?.trim();
   return raw === undefined || raw === "" ? undefined : raw;
};

/**
 * The byte sizes DuckDB itself accepts (probed against v1.5.5): the 1000^i and
 * 1024^i units up to TB/TiB, no bare byte count. Shared by every size setting
 * because a validator narrower than the engine fails the pod on a value DuckDB
 * would take. Zero is rejected: DuckDB stores `0MB`, and no setting here has a
 * sensible zero.
 */
const DUCKDB_BYTE_SIZE =
   /^(?!0+(\.0+)?\s*[A-Za-z])\d+(\.\d+)?\s*(B|KB|KIB|MB|MIB|GB|GIB|TB|TIB)$/i;

/**
 * Validate the DuckDB resource settings at boot, and create the spill
 * directory. Otherwise a typo surfaces only on the first DuckDB session, after
 * the health probes (which never touch DuckDB) report ready. The directory is
 * created because `SET temp_directory` accepts a missing path and fails only
 * at the first spill, without naming the variable.
 */
export function assertDuckDBResourceConfig(): void {
   const memoryLimit = getDuckDBMemoryLimit();
   if (memoryLimit !== undefined && !DUCKDB_BYTE_SIZE.test(memoryLimit)) {
      throw new Error(
         `Invalid value for PUBLISHER_DUCKDB_MEMORY_LIMIT: expected a size like ` +
            `"1GB" or "512MB" (or "off" to disable), got "${memoryLimit}"`,
      );
   }
   const rowGroupSizeBytes = getDuckLakeRowGroupSizeBytes();
   if (
      rowGroupSizeBytes !== undefined &&
      !DUCKDB_BYTE_SIZE.test(rowGroupSizeBytes)
   ) {
      throw new Error(
         `Invalid value for PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES: expected a ` +
            `size like "16MB", got "${rowGroupSizeBytes}"`,
      );
   }
   const targetFileSizeBytes = getDuckLakeTargetFileSizeBytes();
   if (
      targetFileSizeBytes !== undefined &&
      !DUCKDB_BYTE_SIZE.test(targetFileSizeBytes)
   ) {
      throw new Error(
         `Invalid value for PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES: expected a ` +
            `size like "256MB", got "${targetFileSizeBytes}"`,
      );
   }
   const flushThreshold =
      process.env.PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD?.trim();
   if (
      flushThreshold !== undefined &&
      flushThreshold !== "" &&
      flushThreshold.toLowerCase() !== "off" &&
      !/^[1-9]\d{0,8}$/.test(flushThreshold)
   ) {
      // At most nine digits: above a billion rows the bound is
      // indistinguishable from `off`, so a larger value is a typo.
      throw new Error(
         `Invalid value for PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD: expected a ` +
            `row count like "8192" (up to nine digits), or "off" to turn the ` +
            `partitioned-insert bounds off, got "${flushThreshold}"`,
      );
   }
   const tempDirectory = getDuckDBTempDirectory();
   if (tempDirectory !== undefined) {
      try {
         mkdirSync(tempDirectory, { recursive: true });
         // mkdir succeeds on an existing directory whatever its permissions
         // (a volume mounted with the wrong owner), so check writability too.
         accessSync(tempDirectory, fsConstants.W_OK);
      } catch (error) {
         const detail = error instanceof Error ? error.message : String(error);
         throw new Error(
            `Cannot use PUBLISHER_DUCKDB_TEMP_DIRECTORY "${tempDirectory}": ` +
               `${detail}`,
         );
      }
   }
}

/**
 * Settings for the optional embedding provider behind semantic
 * `get_context` retrieval. See {@link getEmbeddingConfig}.
 */
export interface EmbeddingConfig {
   /** Bearer token sent to the embedding endpoint. */
   apiKey: string;
   /** Embedding model name, e.g. "text-embedding-3-small". */
   model: string;
   /** Base URL of an OpenAI-compatible API (no trailing slash). */
   baseUrl: string;
   /**
    * Optional `dimensions` request parameter. Omitted from requests when
    * unset; the vector length then comes from the provider's response.
    */
   dimensions?: number;
   /**
    * Cosine-similarity floor a match must clear to be returned at all.
    * See {@link DEFAULT_EMBEDDING_MIN_SIMILARITY}.
    */
   minSimilarity: number;
   /** Which provider the settings came from; absent means openai-compatible. */
   provider?: ProviderName;
   /** Put before a search query / before indexed text. Default ''. */
   queryPrefix?: string;
   documentPrefix?: string;
}

const DEFAULT_EMBEDDING_MODEL = "text-embedding-3-small";
const DEFAULT_EMBEDDING_API_BASE = "https://api.openai.com/v1";

/**
 * Default cosine-similarity floor for semantic retrieval. Tunable because
 * cosine similarity is not calibrated across embedding models; deliberately
 * permissive, preferring a weak match to a lost real one. See
 * docs/configuration.md.
 */
export const DEFAULT_EMBEDDING_MIN_SIMILARITY = 0.2;

/**
 * Embedding-provider settings for semantic `get_context` retrieval, or `null`
 * when disabled (no `EMBEDDING_API_KEY`), leaving lexical ranking.
 *
 * An ambient key such as `OPENAI_API_KEY` is deliberately NOT read: this
 * sends entity names, `#(doc)` text and queries to the endpoint, and that
 * egress must not switch on because a common variable happens to be set.
 * Throws on malformed companion values rather than degrading silently.
 *
 * `file` (`retrieval.embedding`) wins over the matching `EMBEDDING_*`
 * variable. OpenAI-style providers only; see {@link getEmbeddingSettings}.
 */
export const getEmbeddingConfig = (
   file?: RetrievalEmbeddingConfig,
): EmbeddingConfig | null => {
   const provider = file?.provider;
   if (provider === "google" || provider === "vertex") return null;
   const envKey = process.env.EMBEDDING_API_KEY?.trim();
   // Ollama serves without a key; for every other provider no key means the
   // feature is off, never an error.
   if (!envKey && provider !== "ollama") {
      return null;
   }
   const apiKey = envKey || "ollama";

   const rawBase = process.env.EMBEDDING_API_BASE;
   const baseUrl = (
      file?.baseUrl ||
      rawBase?.trim() ||
      (provider ? defaultOpenAiBaseUrl(provider) : undefined) ||
      DEFAULT_EMBEDDING_API_BASE
   ).replace(/\/+$/, "");
   try {
      new URL(baseUrl);
   } catch {
      throw new Error(
         `Invalid value for EMBEDDING_API_BASE: expected a URL, got "${rawBase}"`,
      );
   }

   const model =
      file?.model ||
      process.env.EMBEDDING_MODEL?.trim() ||
      DEFAULT_EMBEDDING_MODEL;

   const dimensions = file?.dimensions ?? parseIntEnv("EMBEDDING_DIMENSIONS");
   if (dimensions !== undefined && dimensions <= 0) {
      throw new Error(
         `EMBEDDING_DIMENSIONS must be a positive integer (got ${dimensions})`,
      );
   }

   return {
      apiKey,
      model,
      baseUrl,
      dimensions,
      minSimilarity: embeddingMinSimilarity(),
      ...(provider ? { provider } : {}),
      ...(file?.queryPrefix ? { queryPrefix: file.queryPrefix } : {}),
      ...(file?.documentPrefix ? { documentPrefix: file.documentPrefix } : {}),
   };
};

/** The cosine floor from `EMBEDDING_MIN_SIMILARITY`, validated. */
function embeddingMinSimilarity(): number {
   const minSimilarity =
      parseFloatEnv("EMBEDDING_MIN_SIMILARITY", "0.35") ??
      DEFAULT_EMBEDDING_MIN_SIMILARITY;
   // Rejected rather than clamped: a floor of 1 returns nothing and a negative
   // one everything, and both look like a broken index, not a bad setting.
   if (minSimilarity < 0 || minSimilarity >= 1) {
      throw new Error(
         `Invalid EMBEDDING_MIN_SIMILARITY: expected a number in [0, 1), got ${minSimilarity}. ` +
            `Fix: EMBEDDING_MIN_SIMILARITY=0.35 (default ${DEFAULT_EMBEDDING_MIN_SIMILARITY})`,
      );
   }
   return minSimilarity;
}

/**
 * The embedding provider settings in force, for every provider, or `null`
 * when the feature is off. On iff the provider is named and has what it needs
 * (a key in `EMBEDDING_API_KEY`, or the provider is Ollama or Vertex, which
 * need none), or, with no provider named, `EMBEDDING_API_KEY` is set.
 */
export function getEmbeddingSettings(
   file?: RetrievalEmbeddingConfig,
): EmbeddingSettings | null {
   const queryPrefix = file?.queryPrefix ?? "";
   const documentPrefix = file?.documentPrefix ?? "";
   const provider = file?.provider;
   if (provider === "google" || provider === "vertex") {
      const apiKey = process.env.EMBEDDING_API_KEY?.trim() || undefined;
      if (provider === "google" && !apiKey) return null;
      if (!file?.model) return null;
      return {
         provider,
         model: file.model,
         dimensions: file.dimensions,
         baseUrl: file.baseUrl,
         projectId: file.projectId,
         location: file.location,
         // Vertex authenticates with Application Default Credentials.
         apiKey: provider === "google" ? apiKey : undefined,
         minSimilarity: embeddingMinSimilarity(),
         queryPrefix,
         documentPrefix,
      };
   }
   const config = getEmbeddingConfig(file);
   if (!config) return null;
   const keyless =
      provider === "ollama" && !process.env.EMBEDDING_API_KEY?.trim();
   return {
      provider: config.provider ?? "openai-compatible",
      model: config.model,
      dimensions: config.dimensions,
      baseUrl: config.baseUrl,
      apiKey: keyless ? undefined : config.apiKey,
      minSimilarity: config.minSimilarity,
      queryPrefix,
      documentPrefix,
   };
}

/** A line for the startup log about the embedding settings. */
export interface StartupNotice {
   level: "info" | "warn";
   message: string;
}

const hostOf = (url: string): string => {
   try {
      return new URL(url).host;
   } catch {
      return "<invalid URL>";
   }
};

/**
 * What the startup log should say about the embedding settings that would
 * otherwise act silently:
 *
 * - a provider that needs a key is named but `EMBEDDING_API_KEY` is unset, so
 *   semantic search is off.
 * - a file value overrides a different `EMBEDDING_*` variable, and a changed
 *   model or dimensions re-embeds every package.
 *
 * A base URL is shown as its host only: the URL can carry credentials.
 */
export function embeddingStartupNotices(
   file?: RetrievalEmbeddingConfig,
): StartupNotice[] {
   if (!file) return [];
   const notices: StartupNotice[] = [];
   const key = process.env.EMBEDDING_API_KEY?.trim();
   const provider = file.provider;
   if (
      !key &&
      (provider === "openai" ||
         provider === "openai-compatible" ||
         provider === "google")
   ) {
      notices.push({
         level: "warn",
         message:
            `retrieval.embedding names provider "${provider}" but EMBEDDING_API_KEY is not set, so semantic search is off and get_context ranks lexically. ` +
            `Fix: set EMBEDDING_API_KEY in the server's environment.`,
      });
   }
   const reembeds =
      "Changing the model or its dimensions re-embeds every package.";
   const envModel = process.env.EMBEDDING_MODEL?.trim();
   if (file.model && envModel && envModel !== file.model) {
      notices.push({
         level: "info",
         message: `retrieval.embedding.model "${file.model}" in publisher.config.json overrides EMBEDDING_MODEL "${envModel}". ${reembeds}`,
      });
   }
   const envDimensions = process.env.EMBEDDING_DIMENSIONS?.trim();
   if (
      file.dimensions !== undefined &&
      envDimensions &&
      envDimensions !== String(file.dimensions)
   ) {
      notices.push({
         level: "info",
         message: `retrieval.embedding.dimensions ${file.dimensions} in publisher.config.json overrides EMBEDDING_DIMENSIONS ${envDimensions}. ${reembeds}`,
      });
   }
   const envBase = process.env.EMBEDDING_API_BASE?.trim();
   if (
      file.baseUrl &&
      envBase &&
      envBase.replace(/\/+$/, "") !== file.baseUrl.replace(/\/+$/, "")
   ) {
      notices.push({
         level: "info",
         message: `retrieval.embedding.baseUrl (host "${hostOf(file.baseUrl)}") in publisher.config.json overrides EMBEDDING_API_BASE (host "${hostOf(envBase)}").`,
      });
   }
   return notices;
}

/**
 * The LLM settings in force, or `null` when every LLM feature is off. On iff
 * `retrieval.llm.provider` is set and either `LLM_API_KEY` is present or the
 * provider needs none (Ollama, Vertex). The key is an environment variable and
 * never a file entry. An ambient `OPENAI_API_KEY`-style variable is not read,
 * for the reason {@link getEmbeddingConfig} gives.
 */
export function getLlmSettings(file?: RetrievalLlmConfig): LlmSettings | null {
   if (!file) return null;
   const apiKey = process.env.LLM_API_KEY?.trim() || undefined;
   const needsKey = file.provider !== "ollama" && file.provider !== "vertex";
   if (needsKey && !apiKey) return null;
   return {
      provider: file.provider,
      model: file.model,
      baseUrl: file.baseUrl,
      projectId: file.projectId,
      location: file.location,
      apiKey,
      timeoutMs: file.timeoutMs,
      concurrency: file.concurrency,
      maxCallsPerSync: file.maxCallsPerSync,
      maxCallsPerRequest: file.maxCallsPerRequest,
   };
}

/**
 * Whether `search_database_schema` may send a connection's table and
 * column names to the configured embedding provider. Off unless set.
 *
 * A second switch on top of `EMBEDDING_API_KEY` because it authorises a
 * different disclosure: that key covers the operator's own model text, while
 * a warehouse's table and column names are the customer's. Off, schema search
 * ranks lexically with no network.
 */
export const schemaEmbeddingEnabled = (): boolean =>
   parseBoolEnv("EMBEDDING_INDEX_CONNECTION_SCHEMA") ?? false;

/**
 * Tunables for the standalone {@link MaterializationScheduler}, which fires a
 * package's `materialization.schedule` cron when no control plane does. See
 * {@link getMaterializationSchedulerConfig}.
 */
export interface MaterializationSchedulerConfig {
   /** Cadence of the due-schedule sweep, in milliseconds. */
   tickIntervalMs: number;
   /** Max packages fired per tick — a stampede guard for large deployments. */
   maxFiresPerTick: number;
}

const DEFAULT_SCHEDULER_INTERVAL_MS = 60_000;
const MIN_SCHEDULER_INTERVAL_MS = 1_000;
const DEFAULT_SCHEDULER_MAX_FIRES_PER_TICK = 10;

/**
 * Standalone-scheduler settings, or `null` unless
 * `PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER` is set. Throws on malformed input.
 *
 * **Never set it on an orchestrated worker.** It is the primary guard: a
 * control-plane-loaded package serving live has `manifestLocation === null`,
 * so the scheduler's per-package skip does not cover it, and only this flag
 * keeps it from double-driving the control plane.
 */
export const getMaterializationSchedulerConfig =
   (): MaterializationSchedulerConfig | null => {
      const enabled =
         parseBoolEnv("PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER") ?? false;
      if (!enabled) {
         return null;
      }

      const tickIntervalMs =
         parseIntEnv("PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS") ??
         DEFAULT_SCHEDULER_INTERVAL_MS;
      const maxFiresPerTick =
         parseIntEnv(
            "PUBLISHER_MATERIALIZATION_SCHEDULER_MAX_FIRES_PER_TICK",
         ) ?? DEFAULT_SCHEDULER_MAX_FIRES_PER_TICK;

      if (tickIntervalMs < MIN_SCHEDULER_INTERVAL_MS) {
         throw new Error(
            `PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS must be >= ${MIN_SCHEDULER_INTERVAL_MS} (got ${tickIntervalMs})`,
         );
      }
      if (maxFiresPerTick <= 0) {
         throw new Error(
            `PUBLISHER_MATERIALIZATION_SCHEDULER_MAX_FIRES_PER_TICK must be a positive integer (got ${maxFiresPerTick})`,
         );
      }

      return { tickIntervalMs, maxFiresPerTick };
   };

/**
 * Row cap on ad-hoc connection SQL (`PUBLISHER_MAX_QUERY_ROWS`, default
 * {@link DEFAULT_MAX_QUERY_ROWS}). `0` disables it, leaving only the byte cap.
 * Like every getter here, throws on malformed input so a manifest typo fails
 * loudly instead of silently changing the bound.
 */
export const getMaxQueryRows = (): number => {
   const raw = parseIntEnv("PUBLISHER_MAX_QUERY_ROWS");
   if (raw === undefined) return DEFAULT_MAX_QUERY_ROWS;
   if (raw < 0) {
      throw new Error(
         `PUBLISHER_MAX_QUERY_ROWS must be a non-negative integer (got ${raw})`,
      );
   }
   return raw;
};

/**
 * Byte cap on ad-hoc connection SQL responses from a `StreamingConnection`
 * (`PUBLISHER_MAX_RESPONSE_BYTES`, default {@link DEFAULT_MAX_RESPONSE_BYTES}).
 * `0` disables it, leaving only the row cap.
 */
export const getMaxResponseBytes = (): number => {
   const raw = parseIntEnv("PUBLISHER_MAX_RESPONSE_BYTES");
   if (raw === undefined) return DEFAULT_MAX_RESPONSE_BYTES;
   if (raw < 0) {
      throw new Error(
         `PUBLISHER_MAX_RESPONSE_BYTES must be a non-negative integer (got ${raw})`,
      );
   }
   return raw;
};

/**
 * Default row limit for a Malloy model query that carries no `LIMIT` of its
 * own (`PUBLISHER_DEFAULT_QUERY_ROW_LIMIT`, default
 * {@link DEFAULT_QUERY_ROW_LIMIT}). `0` is rejected: "return zero rows" would
 * silently break every report, and the operator likely meant
 * `PUBLISHER_MAX_QUERY_ROWS=0`.
 */
export const getDefaultQueryRowLimit = (): number => {
   const raw = parseIntEnv("PUBLISHER_DEFAULT_QUERY_ROW_LIMIT");
   if (raw === undefined) return DEFAULT_QUERY_ROW_LIMIT;
   if (raw <= 0) {
      throw new Error(
         `PUBLISHER_DEFAULT_QUERY_ROW_LIMIT must be a positive integer (got ${raw})`,
      );
   }
   return raw;
};

/**
 * Per-query wall-clock timeout in ms (`PUBLISHER_QUERY_TIMEOUT_MS`, default
 * {@link DEFAULT_QUERY_TIMEOUT_MS}). `0` disables it.
 */
export const getQueryTimeoutMs = (): number => {
   const raw = parseIntEnv("PUBLISHER_QUERY_TIMEOUT_MS");
   if (raw === undefined) return DEFAULT_QUERY_TIMEOUT_MS;
   if (raw < 0) {
      throw new Error(
         `PUBLISHER_QUERY_TIMEOUT_MS must be a non-negative integer (got ${raw})`,
      );
   }
   return raw;
};

/**
 * Per-process inbound query concurrency cap
 * (`PUBLISHER_MAX_CONCURRENT_QUERIES`, default
 * {@link DEFAULT_MAX_CONCURRENT_QUERIES}). `0` disables it, for deployments
 * with concurrency control upstream.
 */
export const getMaxConcurrentQueries = (): number => {
   const raw = parseIntEnv("PUBLISHER_MAX_CONCURRENT_QUERIES");
   if (raw === undefined) return DEFAULT_MAX_CONCURRENT_QUERIES;
   if (raw < 0) {
      throw new Error(
         `PUBLISHER_MAX_CONCURRENT_QUERIES must be a non-negative integer (got ${raw})`,
      );
   }
   return raw;
};

/**
 * Cap on open sessions for one plain (non-proxied) Postgres connection
 * (`PUBLISHER_POSTGRES_POOL_MAX`, default {@link DEFAULT_POSTGRES_POOL_MAX}).
 * `0` is rejected: the pool could open nothing.
 */
export const getPostgresPoolMax = (): number => {
   const raw = parseIntEnv("PUBLISHER_POSTGRES_POOL_MAX");
   if (raw === undefined) return DEFAULT_POSTGRES_POOL_MAX;
   if (raw < 1) {
      throw new Error(
         `PUBLISHER_POSTGRES_POOL_MAX must be a positive integer (got ${raw})`,
      );
   }
   return raw;
};

/**
 * DuckDB extension-fetch policy. Governs whether Publisher's explicit extension
 * INSTALL step (see `installAndLoadExtension` in service/connection.ts) may
 * reach the DuckDB extension network.
 */
export type ExtensionFetchPolicy = "on-demand" | "local-only";

/**
 * The DuckDB extension-fetch policy from `EXTENSION_FETCH_POLICY` (default
 * `on-demand`).
 *
 * - `on-demand`: `INSTALL` a missing extension on first use, fetching it from
 *   the extension network unless it is already on disk.
 * - `local-only`: never `INSTALL`, and disable DuckDB's implicit auto-install,
 *   so nothing reaches the network; an on-disk extension still auto-loads and
 *   a missing one is a loud error. For air-gapped / pinned-image deployments.
 *
 * Throws on an unrecognised value. server.ts calls it at boot so a typo fails
 * there rather than on the first DuckDB connection, possibly long after
 * `serving`; it is also read per connection, so no restart is needed.
 */
export const getExtensionFetchPolicy = (): ExtensionFetchPolicy => {
   const raw = process.env.EXTENSION_FETCH_POLICY;
   if (raw === undefined || raw.trim() === "") return "on-demand";
   const normalised = raw.trim().toLowerCase();
   if (normalised === "on-demand" || normalised === "local-only") {
      return normalised;
   }
   throw new Error(
      `Invalid value for EXTENSION_FETCH_POLICY: expected "on-demand" or "local-only", got "${raw}"`,
   );
};

export const ALLOW_DUCKDB_SETUP_SQL_ENV = "PUBLISHER_ALLOW_DUCKDB_SETUP_SQL";

/**
 * Whether environment-authored DuckDB connections may carry `setupSQL`. Off
 * unless set.
 *
 * `setupSQL` runs arbitrary DuckDB statements (`COPY ... TO` a host path,
 * `CREATE PERSISTENT SECRET` shared by every instance, community extensions)
 * from connection config, which Malloy's restricted mode never sees. Only for
 * deployments whose connection authors are trusted with the host.
 */
export const isDuckdbSetupSqlAllowed = (): boolean =>
   parseBoolEnv(ALLOW_DUCKDB_SETUP_SQL_ENV) === true;

/**
 * Where an `s3` connection may select `provider: credential_chain` — host-resolved
 * credentials rather than a configured key pair.
 */
export type S3CredentialChainPolicy = "any" | "destinations-only" | "off";

/** Who authored a connection config, which is what S3CredentialChainPolicy turns on. */
export type ConnectionOrigin = "connection" | "storage-destination";

/**
 * Resolve the S3 credential-chain policy from `S3_CREDENTIAL_CHAIN_POLICY`; falls
 * back to `any` when unset or empty.
 *
 * `credential_chain` makes DuckDB act as the HOST (an EKS pod's IRSA role, an
 * EC2 instance profile). Connection create/test endpoints take an arbitrary
 * body, so whoever reaches them can lend the host's identity to any bucket
 * its role permits; this lets a deployment fronted by its own authorization
 * say so.
 *
 * - `any` (default): any `s3` connection may name it.
 * - `destinations-only`: allowed on an operator-configured
 *   `storageDestinations` entry, refused on an author-supplied connection.
 * - `off`: refused everywhere.
 *
 * Throws on an unrecognised value rather than choosing the permissive option.
 */
export const getS3CredentialChainPolicy = (): S3CredentialChainPolicy => {
   const raw = process.env.S3_CREDENTIAL_CHAIN_POLICY;
   if (raw === undefined || raw.trim() === "") return "any";
   const normalised = raw.trim().toLowerCase();
   if (
      normalised === "any" ||
      normalised === "destinations-only" ||
      normalised === "off"
   ) {
      return normalised;
   }
   throw new Error(
      `Invalid value for S3_CREDENTIAL_CHAIN_POLICY: expected "any", ` +
         `"destinations-only" or "off", got "${raw}"`,
   );
};

/**
 * The three `#@ persist storage=<conn>` deployment modes, read from
 * `PERSIST_STORAGE_MODE`. A runtime kill switch: flipping DOWN must never fail
 * an already-loaded package, only change how `storage=` is honored.
 *
 *  - `off` (default): `storage=` is inert; sources build and serve in their own
 *    warehouse, and a source declaring it is served live with a package warning.
 *  - `write-only`: builds materialize into the destination, but serving stays
 *    live.
 *  - `on`: build into storage AND serve from it, falling back to live per query
 *    for anything the transform cannot serve.
 *
 * No mode hard-fails a package on `storage=`; a stricter policy is the caller's.
 */
export type PersistStorageMode = "off" | "write-only" | "on";

const PERSIST_STORAGE_MODES: readonly PersistStorageMode[] = [
   "off",
   "write-only",
   "on",
];

/**
 * The `storage=` deployment mode from `PERSIST_STORAGE_MODE` (default `off`).
 * Case-insensitive; throws on an unrecognized value.
 */
export const getPersistStorageMode = (): PersistStorageMode =>
   parseModeSetting(
      process.env.PERSIST_STORAGE_MODE,
      "PERSIST_STORAGE_MODE",
      PERSIST_STORAGE_MODES,
   ) ?? "off";

/**
 * Whether a within-package persist-target COLLISION (two distinct persist
 * sources resolving to the same physical table in the same destination) is a
 * hard publish rejection, from `PERSIST_COLLISION_ENFORCE` (default `false`).
 *
 * Warn-only by default because existing packages may carry a latent
 * collision; set `true` only after remediating them, so routine re-publishes
 * do not break. Load is ALWAYS warn-only; the flag governs publish only.
 */
export const getPersistCollisionEnforce = (): boolean =>
   // parseBoolEnv, not `=== "true"`, so `1` or `yes` cannot fail open.
   parseBoolEnv("PERSIST_COLLISION_ENFORCE") ?? false;

/**
 * Which origins may read a cross-origin response from the MCP endpoint, from
 * `MCP_CORS_ORIGINS` (comma-separated; `*` allows any).
 *
 * Returns the value for `cors`'s `origin` option. The default, `false`, sends
 * no `Access-Control-Allow-Origin`: the safe choice for an unauthenticated
 * endpoint that can read whatever the models connect to.
 */
export const getMcpCorsOrigins = (): string[] | boolean | string => {
   const raw = process.env.MCP_CORS_ORIGINS;
   if (raw === undefined || raw.trim() === "") return false;
   const trimmed = raw.trim();
   if (trimmed === "*") return "*";
   const origins = trimmed
      .split(",")
      .map((origin) => origin.trim())
      .filter((origin) => origin.length > 0);
   // A value that parses to nothing (`","`) is a typo: deny, never allow all.
   return origins.length > 0 ? origins : false;
};

/**
 * Whether the publisher attaches per-query metadata at all, from
 * `PUBLISHER_QUERY_METADATA` (default `off`).
 *
 * Off by default because it touches EVERY statement: without a native tag
 * facility the bag rides as a leading SQL comment, visible in query logs. And
 * Malloy THROWS on a bag it cannot render, so if the contract mirrored in
 * `service/query_metadata.ts` drifts from upstream, queries fail; a
 * deployment should opt in deliberately. Case-insensitive; throws on an
 * unrecognized value.
 */
export type QueryMetadataMode = "on" | "off";

export const getQueryMetadataMode = (): QueryMetadataMode => {
   const raw = process.env.PUBLISHER_QUERY_METADATA;
   if (raw === undefined || raw.trim() === "") return "off";
   const value = raw.trim().toLowerCase();
   if (value === "on" || value === "off") return value;
   throw new Error(
      `PUBLISHER_QUERY_METADATA must be on | off (got ${JSON.stringify(raw)})`,
   );
};

function substituteEnvVars(value: string): string {
   const envVarPattern = /\$\{([A-Z_][A-Z0-9_]*)\}/g;

   return value.replace(envVarPattern, (_match, varName) => {
      const envValue = process.env[varName];

      if (envValue !== undefined) {
         return envValue;
      }

      throw new Error(
         `Environment variable '\${${varName}}' is not set in configuration file`,
      );
   });
}

function processConfigValue(value: unknown): unknown {
   if (typeof value === "string") {
      return substituteEnvVars(value);
   }

   if (Array.isArray(value)) {
      return value.map((item) => processConfigValue(item));
   }

   if (value !== null && typeof value === "object") {
      const result: Record<string, unknown> = {};
      for (const [key, val] of Object.entries(value)) {
         result[key] = processConfigValue(val);
      }
      return result;
   }

   return value;
}

/**
 * Absolute directory that a relative package `location` is resolved against:
 * the one holding the active config file.
 *
 * Null when there is nothing sensible to anchor to: no config, the bundled
 * default (inside node_modules), or a `--config` naming a directory.
 */
export const getPublisherConfigDir = (serverRoot: string): string | null => {
   const resolved = resolvePublisherConfigPath(serverRoot);
   if (!resolved || resolved.isBundledDefault) {
      return null;
   }
   try {
      if (!fs.statSync(resolved.path).isFile()) {
         return null;
      }
   } catch {
      return null;
   }
   // `--config` may be relative; a relative anchor would follow the reader's cwd.
   return path.resolve(path.dirname(resolved.path));
};

/**
 * Which of the server's APIs this process may run: `servingApiEnabled` (the
 * main API and MCP, and the environments they serve) and `adminApiEnabled` (the
 * admin API, when its port is set), both `true` by default.
 * `declaresEnvironments` is whether the config lists any.
 *
 * Each is a top-level setting of the config, given as its key in the file, as
 * `PUBLISHER_SERVING_API_ENABLED` / `PUBLISHER_ADMIN_API_ENABLED`, or as
 * `--[no-]serving-api-enabled` / `--[no-]admin-api-enabled` (cli_args.ts sets
 * the variable, so a flag wins over it), and the variable wins over the file.
 * In the file the value is `true` or `false`, or a `${VAR}` reference read as a
 * variable is. A config that cannot be read contributes nothing: the main
 * server reports that file's error itself, on /status.
 */
export type ServerApiSettings = {
   servingApiEnabled: boolean;
   adminApiEnabled: boolean;
   declaresEnvironments: boolean;
};

const SERVER_API_SETTINGS = {
   servingApiEnabled: "PUBLISHER_SERVING_API_ENABLED",
   adminApiEnabled: "PUBLISHER_ADMIN_API_ENABLED",
} as const;

function readConfigObject(serverRoot: string): Record<string, unknown> | null {
   const resolved = resolvePublisherConfigPath(serverRoot);
   if (!resolved || resolved.isBundledDefault) return null;
   try {
      const raw: unknown = JSON.parse(fs.readFileSync(resolved.path, "utf8"));
      return raw && typeof raw === "object" && !Array.isArray(raw)
         ? (raw as Record<string, unknown>)
         : null;
   } catch {
      return null;
   }
}

export const getServerApiSettings = (serverRoot: string): ServerApiSettings => {
   const config = readConfigObject(serverRoot) ?? {};
   // A misspelled value must not quietly run an API the operator turned off.
   const setting = (key: keyof typeof SERVER_API_SETTINGS): boolean => {
      const fromEnv = parseBoolEnv(SERVER_API_SETTINGS[key]);
      if (fromEnv !== undefined) return fromEnv;
      const value = config[key];
      if (value === undefined) return true;
      if (typeof value === "boolean") return value;
      if (typeof value === "string" && /\$\{[A-Z_][A-Z0-9_]*\}/.test(value)) {
         const fromFile = parseBoolString(
            substituteEnvVars(value),
            `"${key}" in ${PUBLISHER_CONFIG_NAME}`,
         );
         if (fromFile !== undefined) return fromFile;
      }
      throw new Error(
         `Invalid ${PUBLISHER_CONFIG_NAME}: "${key}" must be true or false, got ${JSON.stringify(value)}.`,
      );
   };
   const environments = config.environments ?? config.projects;
   return {
      servingApiEnabled: setting("servingApiEnabled"),
      adminApiEnabled: setting("adminApiEnabled"),
      declaresEnvironments:
         Array.isArray(environments) && environments.length > 0,
   };
};

/**
 * The `publisher.config.json` path that was looked for and not found, or null
 * whenever a config did resolve. `explicit` says the path came from `--config`.
 *
 * For a caller running ONCE at boot to explain an empty environment list;
 * `getPublisherConfig` runs per read, so it does not log this. It does log a
 * missing `--config` (flagged `explicit` here), so don't log that twice.
 */
export const getUnresolvedPublisherConfigPath = (
   serverRoot: string,
): { path: string; explicit: boolean } | null => {
   if (resolvePublisherConfigPath(serverRoot)) {
      return null;
   }
   const explicitPath = process.env.PUBLISHER_CONFIG_PATH;
   if (explicitPath && explicitPath.length > 0) {
      return { path: explicitPath, explicit: true };
   }
   return {
      path: path.join(serverRoot, PUBLISHER_CONFIG_NAME),
      explicit: false,
   };
};

export const getPublisherConfig = (serverRoot: string): PublisherConfig => {
   const resolved = resolvePublisherConfigPath(serverRoot);
   if (!resolved) {
      if (
         process.env.PUBLISHER_CONFIG_PATH &&
         process.env.PUBLISHER_CONFIG_PATH.length > 0
      ) {
         // A typo in --config must not boot silently with no environments.
         logger.error(
            `--config path not found: ${process.env.PUBLISHER_CONFIG_PATH}. Using default empty config.`,
         );
      }
      return {
         frozenConfig: false,
         environments: [],
      };
   }
   const publisherConfigPath = resolved.path;
   if (resolved.isBundledDefault) {
      logger.info(
         `No publisher.config.json found at ${path.join(serverRoot, PUBLISHER_CONFIG_NAME)}; falling back to bundled DuckDB-only default. Pass --config <path> or place a config in the server root to override.`,
      );
   }

   let rawConfig: unknown;
   try {
      const fileContent = fs.readFileSync(publisherConfigPath, "utf8");
      rawConfig = JSON.parse(fileContent);
   } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      // Raised, not absorbed into an empty config: that would boot a server
      // reporting "serving" with nothing loaded and the reason only in the
      // log. Callers that can carry on without a config catch it.
      throw new Error(`Failed to parse ${publisherConfigPath}: ${message}`);
   }

   const processedConfig = processConfigValue(rawConfig);

   // The deprecated top-level `projects` key is still accepted, with a
   // warning, so an old config does not silently parse as empty.
   if (
      processedConfig &&
      typeof processedConfig === "object" &&
      !("environments" in processedConfig) &&
      "projects" in processedConfig
   ) {
      logger.warn(
         `${PUBLISHER_CONFIG_NAME} uses deprecated "projects" key; rename to "environments".`,
      );
      (processedConfig as Record<string, unknown>).environments = (
         processedConfig as Record<string, unknown>
      ).projects;
   }

   if (
      processedConfig &&
      typeof processedConfig === "object" &&
      "environments" in processedConfig &&
      processedConfig.environments &&
      typeof processedConfig.environments === "object" &&
      !Array.isArray(processedConfig.environments)
   ) {
      logger.error(
         `Invalid ${PUBLISHER_CONFIG_NAME}: the "environments" field must be a JSON array. Using default empty config.`,
      );
      return {
         frozenConfig: false,
         environments: [],
      };
   }

   let environments: unknown[] = [];
   if (
      processedConfig &&
      typeof processedConfig === "object" &&
      "environments" in processedConfig &&
      Array.isArray((processedConfig as { environments: unknown }).environments)
   ) {
      environments = (processedConfig as { environments: unknown[] })
         .environments;
   }

   let frozenConfig = false;
   if (
      processedConfig &&
      typeof processedConfig === "object" &&
      "frozenConfig" in processedConfig
   ) {
      frozenConfig = Boolean(
         (processedConfig as { frozenConfig: unknown }).frozenConfig,
      );
   }

   const instanceTheme = sanitizeTheme(
      processedConfig &&
         typeof processedConfig === "object" &&
         "theme" in processedConfig
         ? (processedConfig as { theme: unknown }).theme
         : undefined,
      "publisher.config.json",
   );

   const mcp = sanitizeMcpConfig(
      processedConfig &&
         typeof processedConfig === "object" &&
         "mcp" in processedConfig
         ? (processedConfig as { mcp: unknown }).mcp
         : undefined,
   );

   const retrieval = parseRetrievalConfig(
      processedConfig && typeof processedConfig === "object"
         ? (processedConfig as { retrieval?: unknown }).retrieval
         : undefined,
   );

   const settings =
      processedConfig && typeof processedConfig === "object"
         ? (processedConfig as Record<string, unknown>)
         : {};
   const versionPromotion = parseModeSetting(
      settings.versionPromotion,
      `"versionPromotion" in ${PUBLISHER_CONFIG_NAME}`,
      VERSION_PROMOTION_MODES,
   );

   return {
      frozenConfig,
      ...(instanceTheme ? { theme: instanceTheme } : {}),
      ...(mcp ? { mcp } : {}),
      ...(retrieval ? { retrieval } : {}),
      ...(versionPromotion ? { versionPromotion } : {}),
      environments,
   } as PublisherConfig;
};

/**
 * One of a closed set of modes, or undefined when unset (absent, null, or
 * blank). Matched case-insensitively. A value outside the set throws, naming
 * where it came from and what is allowed: a typo must not leave a server in a
 * mode nobody chose. The one parser for every such knob: `PERSIST_STORAGE_MODE`,
 * `PUBLISHER_PACKAGE_VERSIONING` and `versionPromotion`.
 */
function parseModeSetting<T extends string>(
   raw: unknown,
   where: string,
   allowed: readonly T[],
): T | undefined {
   if (raw === undefined || raw === null) return undefined;
   if (typeof raw === "string") {
      const value = raw.trim().toLowerCase();
      if (value === "") return undefined;
      if ((allowed as readonly string[]).includes(value)) return value as T;
   }
   throw new Error(
      `${where} must be one of ${allowed.join(" | ")} (got ${JSON.stringify(raw)})`,
   );
}

/**
 * Whether a publish from a location publishes versions
 * (`PUBLISHER_PACKAGE_VERSIONING=on`; off when unset). Read at call time, so a
 * test can switch it; a value outside on/off throws, and it is read at startup
 * so a typo fails the boot.
 */
export const isVersioningEnabled = (): boolean =>
   parseModeSetting(
      process.env.PUBLISHER_PACKAGE_VERSIONING,
      "PUBLISHER_PACKAGE_VERSIONING",
      PACKAGE_VERSIONING_MODES,
   ) === "on";

/**
 * The server's {@link VersionPromotionMode}: `PUBLISHER_VERSION_PROMOTION` when
 * set, otherwise `versionPromotion` in publisher.config.json, otherwise
 * `on-publish`.
 */
export const getVersionPromotionMode = (
   serverRoot: string,
): VersionPromotionMode =>
   parseModeSetting(
      process.env.PUBLISHER_VERSION_PROMOTION,
      "PUBLISHER_VERSION_PROMOTION",
      VERSION_PROMOTION_MODES,
   ) ??
   readConfigMode(serverRoot, (config) => config.versionPromotion) ??
   "on-publish";

function readConfigMode<T>(
   serverRoot: string,
   pick: (config: PublisherConfig) => T | undefined,
): T | undefined {
   try {
      return pick(getPublisherConfig(serverRoot));
   } catch (error) {
      // A file that cannot be parsed has no setting to give, and is reported
      // where the config is loaded, the same split getSemanticIndexMaxEntities
      // makes. An invalid mode value is a different error and still throws.
      if (
         error instanceof Error &&
         error.message.startsWith("Failed to parse ")
      ) {
         return undefined;
      }
      throw error;
   }
}

/**
 * Read the `mcp` block. Like `theme`, a bad field is warned about and dropped,
 * so it takes its default rather than failing the whole config.
 */
function sanitizeMcpConfig(raw: unknown): McpConfig | undefined {
   if (raw === undefined) return undefined;
   if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
      logger.warn(
         `Invalid "mcp" in ${PUBLISHER_CONFIG_NAME}: expected an object. Ignoring.`,
      );
      return undefined;
   }
   const mcp: McpConfig = {};
   const value = (raw as Record<string, unknown>).includeHiddenFilesAndSources;
   if (typeof value === "boolean") {
      mcp.includeHiddenFilesAndSources = value;
   } else if (value !== undefined) {
      logger.warn(
         `Invalid "mcp.includeHiddenFilesAndSources" in ${PUBLISHER_CONFIG_NAME}: expected a boolean (got ${JSON.stringify(value)}). Ignoring field.`,
      );
   }
   return Object.keys(mcp).length > 0 ? mcp : undefined;
}

/**
 * The entity cap for the semantic index: `retrieval.indexing.maxEntities`
 * from publisher.config.json, or {@link DEFAULT_SEMANTIC_INDEX_MAX_ENTITIES}.
 * Read once at startup. Throws, with the fix, on an invalid value; a config
 * file that cannot be read or parsed gives the default.
 */
export const getSemanticIndexMaxEntities = (serverRoot: string): number => {
   try {
      return (
         getPublisherConfig(serverRoot).retrieval?.indexing?.maxEntities ??
         DEFAULT_SEMANTIC_INDEX_MAX_ENTITIES
      );
   } catch (error) {
      // An unparseable file is reported where the config is used (/status
      // names it). This runs at module load, before /status exists, so
      // throwing would kill the process unexplained. An invalid `retrieval`
      // value still throws.
      if (
         error instanceof Error &&
         error.message.startsWith("Failed to parse ")
      ) {
         return DEFAULT_SEMANTIC_INDEX_MAX_ENTITIES;
      }
      throw error;
   }
};

/**
 * The validated `retrieval` block of publisher.config.json, or undefined when
 * the file has none. Read once at startup; throws, with the fix, on an
 * invalid value.
 */
export const getRetrievalConfig = (
   serverRoot: string,
): RetrievalConfig | undefined => getPublisherConfig(serverRoot).retrieval;

/**
 * Sanitize a raw theme value pulled from JSON. Returns a Theme on success
 * or `undefined` if the input is missing/invalid. Bad shapes log a warning
 * and are dropped rather than failing the whole config; an unthemed config
 * still boots fine.
 */
export function sanitizeTheme(
   raw: unknown,
   context: string,
): Theme | undefined {
   if (raw === undefined || raw === null) return undefined;
   if (typeof raw !== "object" || Array.isArray(raw)) {
      logger.warn(
         `Invalid "theme" in ${context}: expected an object. Ignoring.`,
      );
      return undefined;
   }
   const obj = raw as Record<string, unknown>;
   const theme: Theme = {};

   if ("defaultMode" in obj) {
      const mode = obj.defaultMode;
      if (mode === "light" || mode === "dark" || mode === "auto") {
         theme.defaultMode = mode;
      } else {
         logger.warn(
            `Invalid "theme.defaultMode" in ${context}: expected "light" | "dark" | "auto" (got ${JSON.stringify(mode)}). Ignoring field.`,
         );
      }
   }
   if ("allowUserToggle" in obj) {
      const value = obj.allowUserToggle;
      if (typeof value === "boolean") {
         theme.allowUserToggle = value;
      } else {
         // Don't coerce: Boolean("false") is true.
         logger.warn(
            `Invalid "theme.allowUserToggle" in ${context}: expected a boolean (got ${JSON.stringify(value)}). Ignoring field.`,
         );
      }
   }
   if ("palette" in obj && obj.palette && typeof obj.palette === "object") {
      const palette = obj.palette as Record<string, unknown>;
      const sanitized: NonNullable<Theme["palette"]> = {};
      if (Array.isArray(palette.series)) {
         // An explicit [] is kept: resolveTheme treats it as clearing.
         sanitized.series = palette.series.filter(
            (c): c is string => typeof c === "string",
         );
      }
      for (const key of PER_MODE_COLOR_KEYS) {
         const raw = palette[key];
         if (!raw || typeof raw !== "object" || Array.isArray(raw)) continue;
         const r = raw as Record<string, unknown>;
         const out: { light?: string; dark?: string } = {};
         if (typeof r.light === "string") out.light = r.light;
         if (typeof r.dark === "string") out.dark = r.dark;
         if (Object.keys(out).length > 0) {
            (sanitized as Record<string, unknown>)[key] = out;
         }
      }
      if (Object.keys(sanitized).length > 0) theme.palette = sanitized;
   }
   if ("font" in obj && obj.font && typeof obj.font === "object") {
      const font = obj.font as Record<string, unknown>;
      const sanitized: NonNullable<Theme["font"]> = {};
      if (typeof font.family === "string") sanitized.family = font.family;
      if (typeof font.size === "number" && Number.isFinite(font.size)) {
         sanitized.size = font.size;
      }
      if (Object.keys(sanitized).length > 0) theme.font = sanitized;
   }

   return Object.keys(theme).length > 0 ? theme : undefined;
}

/**
 * Merge an environment-level theme on top of the instance default. Both
 * inputs are already sanitized. The override wins per key at every level;
 * absent keys fall through to the base. Returns `undefined` only when both
 * sides are absent.
 *
 * Per-mode colour objects merge per-mode so an environment that sets
 * only `palette.tile.dark` keeps the instance's `palette.tile.light`.
 */
export function mergeThemes(
   base: Theme | undefined,
   override: Theme | undefined,
): Theme | undefined {
   if (!base) return override;
   if (!override) return base;
   const merged: Theme = { ...base, ...override };
   if (base.palette || override.palette) {
      merged.palette = {
         ...(base.palette ?? {}),
         ...(override.palette ?? {}),
      };
      for (const key of PER_MODE_COLOR_KEYS) {
         const b = base.palette?.[key];
         const o = override.palette?.[key];
         if (b || o) {
            merged.palette[key] = { ...(b ?? {}), ...(o ?? {}) };
         }
      }
   }
   if (base.font || override.font) {
      merged.font = { ...(base.font ?? {}), ...(override.font ?? {}) };
   }
   return merged;
}

export const isPublisherConfigFrozen = (serverRoot: string) => {
   try {
      const publisherConfig = getPublisherConfig(serverRoot);
      return Boolean(publisherConfig.frozenConfig);
   } catch (error) {
      logger.error(
         `Error checking if ${PUBLISHER_CONFIG_NAME} is frozen. Defaulting to false.`,
         { error: error instanceof Error ? error.message : String(error) },
      );
      return false;
   }
};

/**
 * Whether MCP `execute_query` offers `includeHiddenFilesAndSources`, from
 * `mcp.includeHiddenFilesAndSources` in publisher.config.json (default false).
 *
 * The parameter runs what a package's index.malloy hides: an author testing
 * curation needs it, while an answering agent must stay on the curated
 * surface. Off removes it from the tool's schema, so an agent is never offered
 * it. REST's parameter of the same name is unaffected.
 */
export const isMcpIncludeHiddenFilesAndSources = (
   serverRoot: string,
): boolean => {
   try {
      return (
         getPublisherConfig(serverRoot).mcp?.includeHiddenFilesAndSources ===
         true
      );
   } catch (error) {
      logger.error(
         `Error reading "mcp" from ${PUBLISHER_CONFIG_NAME}. Defaulting to off.`,
         { error: error instanceof Error ? error.message : String(error) },
      );
      return false;
   }
};

export const convertConnectionsToApiConnections = (
   connections: Connection[],
): ApiConnection[] => {
   if (!Array.isArray(connections)) {
      return [];
   }

   return connections
      .filter((conn) => {
         if (!conn || typeof conn !== "object") {
            return false;
         }
         if (!conn.name || typeof conn.name !== "string") {
            // Type only: the config carries credentials, and log metadata
            // reaches the transport unredacted (redactSensitive is applied at
            // call sites, not as a winston format).
            logger.warn(
               `Invalid connection: missing or invalid "name" field. Skipping.`,
               { type: typeof conn.type === "string" ? conn.type : undefined },
            );
            return false;
         }
         if (!conn.type || typeof conn.type !== "string") {
            logger.warn(
               `Invalid connection "${conn.name}": missing or invalid "type" field. Skipping.`,
            );
            return false;
         }
         return true;
      })
      .map((conn) => ({
         ...conn,
         name: conn.name,
         type: conn.type as ApiConnection["type"],
         resource: `${API_PREFIX}/connections/${conn.name}`,
      }));
};

export const getProcessedPublisherConfig = (
   serverRoot: string,
): ProcessedPublisherConfig => {
   const rawConfig = getPublisherConfig(serverRoot);

   if (!Array.isArray(rawConfig.environments)) {
      logger.warn(
         `Invalid ${PUBLISHER_CONFIG_NAME}: the "environments" field must be a JSON array. Using empty array.`,
      );
      return {
         frozenConfig: rawConfig.frozenConfig ?? false,
         environments: [],
      };
   }

   const validEnvironments: ProcessedEnvironment[] = [];
   for (const [index, environment] of rawConfig.environments.entries()) {
      if (!environment || typeof environment !== "object") {
         logger.warn(
            `Invalid environment in ${PUBLISHER_CONFIG_NAME}: entry must be an object. Skipping.`,
         );
         continue;
      }

      if (!environment.name || typeof environment.name !== "string") {
         // Index only: the entry carries ${VAR}-substituted credentials, and
         // log metadata reaches the transport unredacted.
         logger.warn(
            `Invalid environment in ${PUBLISHER_CONFIG_NAME}: missing or invalid "name" field. Skipping entry.`,
            { index },
         );
         continue;
      }

      if (!Array.isArray(environment.packages)) {
         logger.warn(
            `Invalid environment "${environment.name}" in ${PUBLISHER_CONFIG_NAME}: missing or invalid "packages" field (must be an array). Skipping entry.`,
         );
         continue;
      }

      const validPackages = environment.packages.filter((pkg) => {
         if (!pkg || typeof pkg !== "object") {
            logger.warn(
               `Invalid package in environment "${environment.name}": package must be an object. Skipping.`,
            );
            return false;
         }
         if (!pkg.name || typeof pkg.name !== "string") {
            logger.warn(
               `Invalid package in environment "${environment.name}": missing or invalid "name" field. Skipping.`,
            );
            return false;
         }
         if (!pkg.location || typeof pkg.location !== "string") {
            logger.warn(
               `Invalid package "${pkg.name}" in environment "${environment.name}": missing or invalid "location" field. Skipping.`,
            );
            return false;
         }
         return true;
      });

      if (validPackages.length === 0) {
         logger.warn(
            `Environment "${environment.name}" has no valid packages. Skipping entry.`,
         );
         continue;
      }

      // The per-environment theme is computed but not applied:
      // addEnvironment() drops it, so only the instance theme reaches viewers.
      const envTheme = sanitizeTheme(
         (environment as { theme?: unknown }).theme,
         `environment "${environment.name}"`,
      );
      const resolvedTheme = mergeThemes(rawConfig.theme, envTheme);

      validEnvironments.push({
         name: environment.name,
         packages: validPackages,
         connections: convertConnectionsToApiConnections(
            environment.connections || [],
         ),
         storageDestinations: Array.isArray(environment.storageDestinations)
            ? (environment.storageDestinations as ApiConnection[])
            : [],
         ...(resolvedTheme ? { theme: resolvedTheme } : {}),
      });
   }

   return {
      frozenConfig: rawConfig.frozenConfig ?? false,
      ...(rawConfig.theme ? { theme: rawConfig.theme } : {}),
      environments: validEnvironments,
   };
};

/**
 * The instance-wide default theme, which ServerStatus carries so the app shell
 * can apply it before any environment is opened.
 */
export const getInstanceTheme = (serverRoot: string): Theme | undefined => {
   try {
      return getPublisherConfig(serverRoot).theme;
   } catch (error) {
      logger.error(
         `Error reading instance theme from ${PUBLISHER_CONFIG_NAME}`,
         { error: error instanceof Error ? error.message : String(error) },
      );
      return undefined;
   }
};
