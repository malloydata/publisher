// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `retrieval` block of a package's publisher.json: how THIS package is
 * searched and indexed. The server operator's side (credentials, egress,
 * ceilings) is publisher.config.json; see ../retrieval_config.ts. A package
 * can switch a feature on, never past what the operator allows.
 *
 * Only keys whose code exists are accepted. Any other key is an error naming
 * the valid ones, so a typo (or a key from a later release) does not silently
 * do nothing.
 */

import { createHash } from "crypto";
import * as fs from "fs";
import * as path from "path";
import { PackageManifestError } from "../errors";
import {
   DEFAULT_QUESTIONS_PER_NODE,
   QUESTIONS_PROMPTS,
   type QuestionsPrompt,
} from "../prompts/questions";

export const PACKAGE_REPRESENTATIONS = ["single", "facets"] as const;
export type PackageRepresentation = (typeof PACKAGE_REPRESENTATIONS)[number];

export const KEYPHRASE_MODES = ["auto", "never", "always"] as const;
export type KeyphraseMode = (typeof KEYPHRASE_MODES)[number];

/** The levels refine rates a candidate at, lowest first. */
export const REFINE_LEVEL_NAMES = ["LOW", "MEDIUM", "HIGH"] as const;
export type RefineLevelName = (typeof REFINE_LEVEL_NAMES)[number];

/** Sources rerank keeps when a package does not say. */
export const DEFAULT_RERANK_TOP_SOURCES = 8;
export const DEFAULT_REFINE_MIN_LEVEL: RefineLevelName = "MEDIUM";

/**
 * `auto`: on when the operator has an LLM configured, off otherwise.
 * `true`: always on; a package that says so on a server with no LLM does not
 * load. `false`: off.
 */
export type StageEnabled = "auto" | boolean;

const RETRIEVAL_KEYS = [
   "representation",
   "keyphrases",
   "refine",
   "rerank",
   "sourceMatch",
   "sourceSummary",
   "prompts",
   "engine",
   "fcj",
] as const;
const PROMPT_KEYS = [
   "keyphrase",
   "refine",
   "rerank",
   "sourceMatch",
   "sourceSummary",
   "judge",
   "docParts",
   "questions",
] as const;
export type PromptKey = (typeof PROMPT_KEYS)[number];
const REFINE_KEYS = ["enabled", "minLevel"] as const;
const RERANK_KEYS = ["enabled", "topSources"] as const;
const SOURCE_MATCH_KEYS = ["enabled"] as const;
const SOURCE_SUMMARY_KEYS = ["enabled"] as const;

/** A prompt a package overrides: the text, read at package load. */
export interface PackagePrompt {
   /** The path as written in publisher.json, for messages. */
   path: string;
   text: string;
   /** sha256 of the text; part of the key that invalidates stored output. */
   hash: string;
}

export interface RefineSettings {
   enabled: StageEnabled;
   /** Candidates rated below this are dropped. */
   minLevel: RefineLevelName;
}

export interface RerankSettings {
   enabled: StageEnabled;
   /** Cards kept after sorting by relevance; the rest are discarded. */
   topSources: number;
}

export interface SourceMatchSettings {
   enabled: StageEnabled;
}

export interface SourceSummarySettings {
   enabled: StageEnabled;
}

/**
 * Which request-time search answers a ranked get_context call. `legacy` is the
 * shipped pipeline (scan, refine, cards, rerank). `fcj` is find, connect,
 * judge (see mcp/context/), which is new and off unless a package asks for it.
 */
export const RETRIEVAL_ENGINES = ["legacy", "fcj"] as const;
export type RetrievalEngine = (typeof RETRIEVAL_ENGINES)[number];

/**
 * The knobs of the `fcj` engine, from `retrieval.fcj`. Every key is optional
 * and has a default; see DEFAULT_FCJ_SETTINGS.
 */
export interface FcjSettings {
   /** Candidates kept per target after the find stage (N). */
   perTarget: number;
   /** Score multiplier per join hop when a candidate is seen from another source. */
   hopDiscount: number;
   /** A source covers a target when its best candidate is at least this share of the target's best anywhere. */
   coverFraction: number;
   /** Weight of a measure target in a source's score; other targets weigh 1. */
   measureWeight: number;
   /** Most clusters the connect stage picks. */
   maxClusters: number;
   /** Fields kept per target inside one cluster after rescoring. */
   clusterTopK: number;
   /** Deepest join chain a source's reach follows. */
   reachDepth: number;
   /** Rank by keyword (BM25) beside embedding closeness. */
   keyword: boolean;
   /** Index-time questions written by the LLM, embedded as extra rows. */
   questions: StageEnabled;
   /** Index-time split of a long doc by the LLM; a rule-based split otherwise. */
   docParts: StageEnabled;
   /** The value index of `#(index)` dimensions and doc value lists. */
   values: boolean;
   /** Most distinct values indexed per dimension, the most frequent kept. */
   valueCap: number;
   /** One LLM call per cluster that picks the fields to return. */
   judge: StageEnabled;
   /**
    * PROVISIONAL, off (0) by default; a diagnostic of the connect rule. A
    * source that scores at least this share of the home's score on the same
    * targets joins the home's cluster (up to three), so the judge chooses
    * between near-tied sources instead of set cover keeping only the first.
    */
   contenderShare: number;
   /**
    * PROVISIONAL, off by default; a diagnostic of the connect rule. A field an
    * extension inherits unchanged is folded into the field where it is
    * defined, so an extension competes only with the fields it adds.
    */
   collapseCopies: boolean;
   /**
    * PROVISIONAL, `clusters` by default. `list` skips the cluster filter: the
    * judge sees every target's find-stage top `listTopK`, grouped by owning
    * source, plus the structural extras. The baseline a connect rule must beat.
    */
   connect: FcjConnect;
   /** With `connect: "list"`: find-stage candidates per target the judge sees. */
   listTopK: number;
   /**
    * PROVISIONAL, `questions` by default. Which wording of prompts/questions.ts
    * writes the index-time rows of a field: questions a person asks, the
    * search phrases an agent types, or questions that use the field's values.
    * A package prompt file (`retrieval.prompts.questions`) wins over it.
    */
   questionsPrompt: QuestionsPrompt;
   /** PROVISIONAL, 3 by default. Index-time rows written per field and view. */
   questionsPerNode: number;
}

export const FCJ_CONNECT_MODES = ["clusters", "list"] as const;
export type FcjConnect = (typeof FCJ_CONNECT_MODES)[number];

export const DEFAULT_FCJ_SETTINGS: FcjSettings = {
   perTarget: 30,
   hopDiscount: 0.9,
   coverFraction: 0.5,
   measureWeight: 1.5,
   maxClusters: 3,
   clusterTopK: 10,
   reachDepth: 3,
   keyword: true,
   questions: "auto",
   docParts: "auto",
   values: true,
   valueCap: 50_000,
   judge: "auto",
   contenderShare: 0,
   collapseCopies: false,
   connect: "clusters",
   listTopK: 15,
   questionsPrompt: "questions",
   questionsPerNode: DEFAULT_QUESTIONS_PER_NODE,
};

export interface PackageRetrievalSettings {
   /** Which request-time search answers a ranked call; absent means `legacy`. */
   engine?: RetrievalEngine;
   /** The `fcj` engine's knobs; absent means DEFAULT_FCJ_SETTINGS. */
   fcj?: FcjSettings;
   /** `single`: one row per entity. `facets`: a name row plus doc chunks. */
   representation: PackageRepresentation;
   /**
    * `auto`: a short description is its own keyphrase, anything else gets an
    * LLM-written one (needs the operator's LLM; otherwise acts as `never`).
    * `always`: an LLM keyphrase for every entity. `never`: none.
    */
   keyphrases: KeyphraseMode;
   /** Absent means the defaults: `{ enabled: "auto", minLevel: "MEDIUM" }`. */
   refine?: RefineSettings;
   /** Absent means the defaults: `{ enabled: "auto", topSources: 8 }`. */
   rerank?: RerankSettings;
   /** Absent means the default: `{ enabled: "auto" }`. */
   sourceMatch?: SourceMatchSettings;
   /**
    * Absent means the default: `{ enabled: "auto" }`. An index-time stage: the
    * embedding sync writes each source's summary, so it runs only on a server
    * that has both an LLM and an embedding provider.
    */
   sourceSummary?: SourceSummarySettings;
   prompts: { [K in PromptKey]?: PackagePrompt };
}

export const DEFAULT_PACKAGE_RETRIEVAL: PackageRetrievalSettings = {
   representation: "single",
   keyphrases: "auto",
   prompts: {},
};

/** The refine settings in force: the package's, or the defaults. */
export function refineSettingsOf(
   retrieval: PackageRetrievalSettings,
): RefineSettings {
   return (
      retrieval.refine ?? {
         enabled: "auto",
         minLevel: DEFAULT_REFINE_MIN_LEVEL,
      }
   );
}

/** The rerank settings in force: the package's, or the defaults. */
export function rerankSettingsOf(
   retrieval: PackageRetrievalSettings,
): RerankSettings {
   return (
      retrieval.rerank ?? {
         enabled: "auto",
         topSources: DEFAULT_RERANK_TOP_SOURCES,
      }
   );
}

/** The source-match settings in force: the package's, or the defaults. */
export function sourceMatchSettingsOf(
   retrieval: PackageRetrievalSettings,
): SourceMatchSettings {
   return retrieval.sourceMatch ?? { enabled: "auto" };
}

/** The source-summary settings in force: the package's, or the defaults. */
export function sourceSummarySettingsOf(
   retrieval: PackageRetrievalSettings,
): SourceSummarySettings {
   return retrieval.sourceSummary ?? { enabled: "auto" };
}

/** The engine in force: the package's, or `legacy`. */
export function engineOf(
   retrieval: PackageRetrievalSettings | undefined,
): RetrievalEngine {
   return retrieval?.engine ?? "legacy";
}

/** The fcj knobs in force: the package's, or the defaults. */
export function fcjSettingsOf(
   retrieval: PackageRetrievalSettings | undefined,
): FcjSettings {
   return retrieval?.fcj ?? DEFAULT_FCJ_SETTINGS;
}

function fail(message: string): never {
   throw new PackageManifestError(message);
}

function describe(value: unknown): string {
   return JSON.stringify(value) ?? "nothing";
}

/**
 * Whether a path relative to the package root steps outside it: `..` itself or
 * a path that starts with `..` as a whole segment. A name that merely begins
 * with two dots (`..prompts`) is an ordinary name and stays inside.
 */
export function stepsOutside(within: string): boolean {
   return (
      within === ".." ||
      within.startsWith(`..${path.sep}`) ||
      path.isAbsolute(within)
   );
}

/**
 * `rel` as a path inside `packageRoot`, or a PackageManifestError. Rejects an
 * absolute path, a NUL byte, and anything that resolves outside the package
 * directory, including through a symlink: the check runs on the real paths
 * once the file exists.
 */
export function resolvePromptPath(
   packageRoot: string,
   rel: string,
   promptKey: PromptKey = "keyphrase",
): string {
   const key = `retrieval.prompts.${promptKey}`;
   const fix = `Fix: use a path inside the package, e.g. "prompts/${promptKey}.md".`;
   if (
      rel.includes("\0") ||
      path.isAbsolute(rel) ||
      /^[A-Za-z]:[\\/]/.test(rel) ||
      rel.startsWith("\\")
   ) {
      fail(
         `Invalid publisher.json ${key}: ${describe(rel)} is an absolute path. ${fix}`,
      );
   }
   const root = path.resolve(packageRoot);
   const resolved = path.resolve(root, rel);
   const within = path.relative(root, resolved);
   if (within === "" || stepsOutside(within)) {
      fail(
         `Invalid publisher.json ${key}: ${describe(rel)} resolves outside the package directory. ${fix}`,
      );
   }
   return resolved;
}

function objectBlock(
   raw: unknown,
   key: string,
   valid: readonly string[],
   fix: string,
): Record<string, unknown> {
   if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
      fail(
         `Invalid publisher.json ${key}: expected an object, got ${describe(raw)}. Fix: ${fix}`,
      );
   }
   const obj = raw as Record<string, unknown>;
   const unknown = Object.keys(obj).filter((k) => !valid.includes(k));
   if (unknown.length > 0) {
      fail(
         `Invalid publisher.json ${key}: unknown key ${unknown.map((k) => `'${k}'`).join(", ")}. ` +
            `Valid keys: ${valid.join(", ")}.`,
      );
   }
   return obj;
}

function parseEnabled(value: unknown, key: string): StageEnabled {
   if (value === undefined || value === null) return "auto";
   if (value === "auto" || value === true || value === false) return value;
   return fail(
      `Invalid publisher.json ${key}: expected "auto", true or false, got ${describe(value)}. ` +
         `Fix: "enabled": "auto" (on when the server has an LLM), true (require one) or false.`,
   );
}

function parseRefine(raw: unknown): RefineSettings {
   const obj = objectBlock(
      raw,
      "retrieval.refine",
      REFINE_KEYS,
      `"refine": { "enabled": "auto", "minLevel": "MEDIUM" }`,
   );
   const minLevel = obj.minLevel ?? DEFAULT_REFINE_MIN_LEVEL;
   if (
      typeof minLevel !== "string" ||
      !(REFINE_LEVEL_NAMES as readonly string[]).includes(minLevel)
   ) {
      fail(
         `Invalid publisher.json retrieval.refine.minLevel: expected one of ${REFINE_LEVEL_NAMES.join(", ")}, got ${describe(obj.minLevel)}. ` +
            `Fix: "minLevel": "MEDIUM" (drop candidates the model rates below it).`,
      );
   }
   return {
      enabled: parseEnabled(obj.enabled, "retrieval.refine.enabled"),
      minLevel: minLevel as RefineLevelName,
   };
}

function parseRerank(raw: unknown): RerankSettings {
   const obj = objectBlock(
      raw,
      "retrieval.rerank",
      RERANK_KEYS,
      `"rerank": { "enabled": "auto", "topSources": ${DEFAULT_RERANK_TOP_SOURCES} }`,
   );
   const top = obj.topSources ?? DEFAULT_RERANK_TOP_SOURCES;
   if (typeof top !== "number" || !Number.isSafeInteger(top) || top <= 0) {
      fail(
         `Invalid publisher.json retrieval.rerank.topSources: expected a positive integer, got ${describe(obj.topSources)}. ` +
            `Fix: "topSources": ${DEFAULT_RERANK_TOP_SOURCES}`,
      );
   }
   return {
      enabled: parseEnabled(obj.enabled, "retrieval.rerank.enabled"),
      topSources: top,
   };
}

function parseSourceMatch(raw: unknown): SourceMatchSettings {
   const obj = objectBlock(
      raw,
      "retrieval.sourceMatch",
      SOURCE_MATCH_KEYS,
      `"sourceMatch": { "enabled": "auto" }`,
   );
   return {
      enabled: parseEnabled(obj.enabled, "retrieval.sourceMatch.enabled"),
   };
}

function parseSourceSummary(raw: unknown): SourceSummarySettings {
   const obj = objectBlock(
      raw,
      "retrieval.sourceSummary",
      SOURCE_SUMMARY_KEYS,
      `"sourceSummary": { "enabled": "auto" }`,
   );
   return {
      enabled: parseEnabled(obj.enabled, "retrieval.sourceSummary.enabled"),
   };
}

const FCJ_KEYS = Object.keys(DEFAULT_FCJ_SETTINGS);

/** A number setting of `retrieval.fcj`, with a message that says what fits. */
function fcjNumber(
   obj: Record<string, unknown>,
   key: keyof FcjSettings,
   check: { min: number; max: number; integer: boolean },
): number {
   const value = obj[key] ?? DEFAULT_FCJ_SETTINGS[key];
   const ok =
      typeof value === "number" &&
      Number.isFinite(value) &&
      value >= check.min &&
      value <= check.max &&
      (!check.integer || Number.isSafeInteger(value));
   if (!ok) {
      fail(
         `Invalid publisher.json retrieval.fcj.${key}: expected ${check.integer ? "an integer" : "a number"} from ${check.min} to ${check.max}, got ${describe(obj[key])}. ` +
            `Fix: "${key}": ${JSON.stringify(DEFAULT_FCJ_SETTINGS[key])}`,
      );
   }
   return value as number;
}

function fcjBoolean(
   obj: Record<string, unknown>,
   key: keyof FcjSettings,
): boolean {
   const value = obj[key] ?? DEFAULT_FCJ_SETTINGS[key];
   if (typeof value !== "boolean") {
      fail(
         `Invalid publisher.json retrieval.fcj.${key}: expected true or false, got ${describe(obj[key])}. ` +
            `Fix: "${key}": ${JSON.stringify(DEFAULT_FCJ_SETTINGS[key])}`,
      );
   }
   return value as boolean;
}

function fcjConnect(obj: Record<string, unknown>): FcjConnect {
   const value = obj.connect ?? DEFAULT_FCJ_SETTINGS.connect;
   if (!(FCJ_CONNECT_MODES as readonly unknown[]).includes(value)) {
      fail(
         `Invalid publisher.json retrieval.fcj.connect: expected one of ${FCJ_CONNECT_MODES.join(", ")}, got ${describe(obj.connect)}. ` +
            `Fix: "connect": "clusters" (source groups) or "list" (the find stage's top listTopK per target).`,
      );
   }
   return value as FcjConnect;
}

function fcjQuestionsPrompt(obj: Record<string, unknown>): QuestionsPrompt {
   const value = obj.questionsPrompt ?? DEFAULT_FCJ_SETTINGS.questionsPrompt;
   if (!(QUESTIONS_PROMPTS as readonly unknown[]).includes(value)) {
      fail(
         `Invalid publisher.json retrieval.fcj.questionsPrompt: expected one of ${QUESTIONS_PROMPTS.join(", ")}, got ${describe(obj.questionsPrompt)}. ` +
            `Fix: "questionsPrompt": "questions" (what a person asks), "phrases" (what an agent types as a search target) or "values" (questions that filter by the field's values).`,
      );
   }
   return value as QuestionsPrompt;
}

function parseFcj(raw: unknown): FcjSettings {
   const obj = objectBlock(
      raw,
      "retrieval.fcj",
      FCJ_KEYS,
      `"fcj": { "perTarget": ${DEFAULT_FCJ_SETTINGS.perTarget} }`,
   );
   const int = (key: keyof FcjSettings, min: number, max: number) =>
      fcjNumber(obj, key, { min, max, integer: true });
   const real = (key: keyof FcjSettings, min: number, max: number) =>
      fcjNumber(obj, key, { min, max, integer: false });
   return {
      perTarget: int("perTarget", 1, 500),
      hopDiscount: real("hopDiscount", 0, 1),
      coverFraction: real("coverFraction", 0, 1),
      measureWeight: real("measureWeight", 0, 10),
      maxClusters: int("maxClusters", 1, 10),
      clusterTopK: int("clusterTopK", 1, 100),
      reachDepth: int("reachDepth", 0, 10),
      keyword: fcjBoolean(obj, "keyword"),
      // Unset means the default, which for these three is "auto".
      questions: parseEnabled(obj.questions, "retrieval.fcj.questions"),
      docParts: parseEnabled(obj.docParts, "retrieval.fcj.docParts"),
      values: fcjBoolean(obj, "values"),
      valueCap: int("valueCap", 1, 1_000_000),
      judge: parseEnabled(obj.judge, "retrieval.fcj.judge"),
      contenderShare: real("contenderShare", 0, 1),
      collapseCopies: fcjBoolean(obj, "collapseCopies"),
      connect: fcjConnect(obj),
      listTopK: int("listTopK", 1, 500),
      questionsPrompt: fcjQuestionsPrompt(obj),
      questionsPerNode: int("questionsPerNode", 1, 8),
   };
}

function parseEngine(raw: unknown): RetrievalEngine {
   if (
      typeof raw !== "string" ||
      !(RETRIEVAL_ENGINES as readonly string[]).includes(raw)
   ) {
      fail(
         `Invalid publisher.json retrieval.engine: expected one of ${RETRIEVAL_ENGINES.join(", ")}, got ${describe(raw)}. ` +
            `Fix: "engine": "legacy" (the shipped search) or "fcj" (find, connect, judge).`,
      );
   }
   return raw as RetrievalEngine;
}

/**
 * Validate the block's shape and return what it says, with the prompt path
 * still unread. Throws a PackageManifestError (424: the package is not served
 * until the author fixes the file) naming the key and a fix.
 */
export function parsePackageRetrieval(raw: unknown): {
   engine?: RetrievalEngine;
   fcj?: FcjSettings;
   representation: PackageRepresentation;
   keyphrases: KeyphraseMode;
   refine?: RefineSettings;
   rerank?: RerankSettings;
   sourceMatch?: SourceMatchSettings;
   sourceSummary?: SourceSummarySettings;
   promptPaths: { [K in PromptKey]?: string };
} {
   if (raw === undefined || raw === null) {
      return {
         representation: DEFAULT_PACKAGE_RETRIEVAL.representation,
         keyphrases: DEFAULT_PACKAGE_RETRIEVAL.keyphrases,
         promptPaths: {},
      };
   }
   if (typeof raw !== "object" || Array.isArray(raw)) {
      fail(
         `Invalid publisher.json retrieval: expected an object, got ${describe(raw)}. ` +
            `Fix: "retrieval": { "representation": "single" }`,
      );
   }
   const obj = raw as Record<string, unknown>;
   const unknown = Object.keys(obj).filter(
      (k) => !(RETRIEVAL_KEYS as readonly string[]).includes(k),
   );
   if (unknown.length > 0) {
      fail(
         `Invalid publisher.json retrieval: unknown key ${unknown.map((k) => `'${k}'`).join(", ")}. ` +
            `Valid keys: ${RETRIEVAL_KEYS.join(", ")}.`,
      );
   }

   const representation = obj.representation ?? "single";
   if (
      typeof representation !== "string" ||
      !(PACKAGE_REPRESENTATIONS as readonly string[]).includes(representation)
   ) {
      fail(
         `Invalid publisher.json retrieval.representation: expected one of ${PACKAGE_REPRESENTATIONS.join(", ")}, got ${describe(obj.representation)}. ` +
            `Fix: "representation": "single" (one row per entity) or "facets" (a name row plus doc chunks).`,
      );
   }
   const keyphrases = obj.keyphrases ?? "auto";
   if (
      typeof keyphrases !== "string" ||
      !(KEYPHRASE_MODES as readonly string[]).includes(keyphrases)
   ) {
      fail(
         `Invalid publisher.json retrieval.keyphrases: expected one of ${KEYPHRASE_MODES.join(", ")}, got ${describe(obj.keyphrases)}. ` +
            `Fix: "keyphrases": "auto".`,
      );
   }
   const refine =
      obj.refine === undefined || obj.refine === null
         ? undefined
         : parseRefine(obj.refine);
   const rerank =
      obj.rerank === undefined || obj.rerank === null
         ? undefined
         : parseRerank(obj.rerank);
   const sourceMatch =
      obj.sourceMatch === undefined || obj.sourceMatch === null
         ? undefined
         : parseSourceMatch(obj.sourceMatch);
   const sourceSummary =
      obj.sourceSummary === undefined || obj.sourceSummary === null
         ? undefined
         : parseSourceSummary(obj.sourceSummary);
   const engine =
      obj.engine === undefined || obj.engine === null
         ? undefined
         : parseEngine(obj.engine);
   const fcj =
      obj.fcj === undefined || obj.fcj === null ? undefined : parseFcj(obj.fcj);

   const promptPaths: { [K in PromptKey]?: string } = {};
   if (obj.prompts !== undefined && obj.prompts !== null) {
      if (typeof obj.prompts !== "object" || Array.isArray(obj.prompts)) {
         fail(
            `Invalid publisher.json retrieval.prompts: expected an object, got ${describe(obj.prompts)}. ` +
               `Fix: "prompts": { "keyphrase": "prompts/keyphrase.md" }`,
         );
      }
      const prompts = obj.prompts as Record<string, unknown>;
      const badPrompt = Object.keys(prompts).filter(
         (k) => !(PROMPT_KEYS as readonly string[]).includes(k),
      );
      if (badPrompt.length > 0) {
         fail(
            `Invalid publisher.json retrieval.prompts: unknown key ${badPrompt.map((k) => `'${k}'`).join(", ")}. ` +
               `Valid keys: ${PROMPT_KEYS.join(", ")}.`,
         );
      }
      for (const key of PROMPT_KEYS) {
         const value = prompts[key];
         if (value === undefined || value === null) continue;
         if (typeof value !== "string" || value.trim() === "") {
            fail(
               `Invalid publisher.json retrieval.prompts.${key}: expected a file path inside the package, got ${describe(value)}. ` +
                  `Fix: "${key}": "prompts/${key}.md"`,
            );
         }
         promptPaths[key] = value;
      }
   }
   return {
      ...(engine ? { engine } : {}),
      ...(fcj ? { fcj } : {}),
      representation: representation as PackageRepresentation,
      keyphrases: keyphrases as KeyphraseMode,
      ...(refine ? { refine } : {}),
      ...(rerank ? { rerank } : {}),
      ...(sourceMatch ? { sourceMatch } : {}),
      ...(sourceSummary ? { sourceSummary } : {}),
      promptPaths,
   };
}

async function readPromptFile(
   packageRoot: string,
   key: PromptKey,
   rel: string,
): Promise<PackagePrompt> {
   const file = resolvePromptPath(packageRoot, rel, key);
   const at = `retrieval.prompts.${key}`;
   let text: string;
   try {
      // The real path must also be inside the real package directory: a
      // symlink in the package can point anywhere.
      const [realRoot, realFile] = await Promise.all([
         fs.promises.realpath(packageRoot),
         fs.promises.realpath(file),
      ]);
      const within = path.relative(realRoot, realFile);
      if (stepsOutside(within)) {
         fail(
            `Invalid publisher.json ${at}: ${describe(rel)} resolves outside the package directory through a link. ` +
               `Fix: copy the file into the package.`,
         );
      }
      text = await fs.promises.readFile(realFile, "utf8");
   } catch (error) {
      if (error instanceof PackageManifestError) throw error;
      return fail(
         `Invalid publisher.json ${at}: cannot read ${describe(rel)} (${(error as Error).message}). ` +
            `Fix: create the file inside the package or remove the key.`,
      );
   }
   if (text.trim() === "") {
      fail(
         `Invalid publisher.json ${at}: ${describe(rel)} is empty. ` +
            `Fix: write the prompt, or remove the key to use the built-in one.`,
      );
   }
   return {
      path: rel,
      text,
      hash: createHash("sha256").update(text).digest("hex"),
   };
}

/**
 * Parse the block and read the prompt files it names. Called when the package
 * loads, so an edit takes effect on reload and a bad path stops the load with
 * a message naming the file.
 */
export async function readPackageRetrieval(
   packageRoot: string,
   raw: unknown,
): Promise<PackageRetrievalSettings> {
   const parsed = parsePackageRetrieval(raw);
   const prompts: PackageRetrievalSettings["prompts"] = {};
   for (const key of PROMPT_KEYS) {
      const rel = parsed.promptPaths[key];
      if (rel !== undefined) {
         prompts[key] = await readPromptFile(packageRoot, key, rel);
      }
   }
   return {
      ...(parsed.engine ? { engine: parsed.engine } : {}),
      ...(parsed.fcj ? { fcj: parsed.fcj } : {}),
      representation: parsed.representation,
      keyphrases: parsed.keyphrases,
      ...(parsed.refine ? { refine: parsed.refine } : {}),
      ...(parsed.rerank ? { rerank: parsed.rerank } : {}),
      ...(parsed.sourceMatch ? { sourceMatch: parsed.sourceMatch } : {}),
      ...(parsed.sourceSummary ? { sourceSummary: parsed.sourceSummary } : {}),
      prompts,
   };
}

/**
 * A package that sets a stage to `true` needs the operator's LLM. With none
 * configured the package does not load, with a message that names both fixes,
 * rather than serving answers that quietly skip the stage it asked for.
 * `auto` and `false` never fail here.
 */
export function assertRequiredStagesAvailable(
   retrieval: PackageRetrievalSettings,
   llmConfigured: boolean,
): void {
   if (llmConfigured) return;
   for (const [key, stage] of [
      ["refine", retrieval.refine],
      ["rerank", retrieval.rerank],
      ["sourceMatch", retrieval.sourceMatch],
      ["sourceSummary", retrieval.sourceSummary],
   ] as const) {
      if (stage?.enabled === true) {
         fail(
            `Invalid publisher.json retrieval.${key}.enabled: true needs an LLM, and this server has none configured. ` +
               `Fix: set retrieval.llm in publisher.config.json and LLM_API_KEY in the server's environment, ` +
               `or set "enabled": "auto" (on only when an LLM is configured) or false.`,
         );
      }
   }
}
