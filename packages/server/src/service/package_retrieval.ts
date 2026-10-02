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
   "prompts",
] as const;
const PROMPT_KEYS = ["keyphrase", "refine", "rerank", "sourceMatch"] as const;
export type PromptKey = (typeof PROMPT_KEYS)[number];
const REFINE_KEYS = ["enabled", "minLevel"] as const;
const RERANK_KEYS = ["enabled", "topSources"] as const;
const SOURCE_MATCH_KEYS = ["enabled"] as const;

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

export interface PackageRetrievalSettings {
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
function stepsOutside(within: string): boolean {
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

/**
 * Validate the block's shape and return what it says, with the prompt path
 * still unread. Throws a PackageManifestError (424: the package is not served
 * until the author fixes the file) naming the key and a fix.
 */
export function parsePackageRetrieval(raw: unknown): {
   representation: PackageRepresentation;
   keyphrases: KeyphraseMode;
   refine?: RefineSettings;
   rerank?: RerankSettings;
   sourceMatch?: SourceMatchSettings;
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
      representation: representation as PackageRepresentation,
      keyphrases: keyphrases as KeyphraseMode,
      ...(refine ? { refine } : {}),
      ...(rerank ? { rerank } : {}),
      ...(sourceMatch ? { sourceMatch } : {}),
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
      representation: parsed.representation,
      keyphrases: parsed.keyphrases,
      ...(parsed.refine ? { refine: parsed.refine } : {}),
      ...(parsed.rerank ? { rerank: parsed.rerank } : {}),
      ...(parsed.sourceMatch ? { sourceMatch: parsed.sourceMatch } : {}),
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
