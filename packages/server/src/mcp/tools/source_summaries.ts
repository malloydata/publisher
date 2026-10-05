// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Index-time source summaries: for each source, a dense paragraph that names
 * its fields and a one-line label, written once by the LLM and stored.
 * get_context shows the one-liner on a source's card, the full summary when the
 * request pins the source (or a source search matched only it), and the source
 * match stage shows the summary to the model that picks sources.
 *
 * What is sent to the LLM for one source: its name, its `#(doc)` text (or
 * "No source docs."), and a list of its fields grouped as Dimensions, Measures,
 * Views and Joins, each as `name (type): doc`, with each joined source nested
 * under it with its own fields. Never code, and never an access predicate: the
 * doc text is `#(doc)`-only to begin with and is scrubbed again here.
 *
 * The inputs are a pure function of the entity list the sync already has, so
 * the readiness fingerprint can include them (see sourceSummaryInputsDigest)
 * and a change to any field the model would see moves a package back to
 * `indexing` until the summary is rewritten.
 *
 * A source is identified by the file that defines it and its name: two model
 * files can each define `orders`, and each gets its own summary, its own row
 * and its own nested joins. A join names the file of its target, so it expands
 * the right one; a join with no file recorded expands only when exactly one
 * source has that name.
 *
 * A stored summary is reused only while its input hash still matches. The hash
 * covers the exact user message (source, docs, field list), the prompt text and
 * the model, so editing a doc, a field, the prompt file or the model rewrites
 * exactly the sources it touches, and an unchanged source costs no LLM call on
 * a restart, a reload or a republish. After a sync every stored row matches
 * the current inputs: a row that no longer does (and could not be rewritten
 * because the per-sync call limit was reached) is deleted rather than served.
 * Between a reload and the next sync a row can be stale, so a request reads
 * them against the current inputs (see loadSourceSummaries) and serves only
 * the ones that still match.
 *
 * The message has a size. One field's `#(doc)` is cut to
 * SOURCE_SUMMARY_MAX_FIELD_DOC_CHARS, and the whole message to
 * SOURCE_SUMMARY_MAX_PROMPT_CHARS by showing fewer joined sources, then fewer
 * fields, with a line that says what was left out. Without a bound, a package
 * with long docs sent a message past the model's context window, which the
 * vendor refuses with a 400 that no retry can change.
 */

import { createHash } from "crypto";
import { logger } from "../../logger";
import {
   ONE_LINE_SUMMARY_MAX_CHARS,
   renderSourceSummaryUserPrompt,
   undocumentedOneLine,
} from "../../prompts/source_summary";
import type { ChatModel } from "../../providers/types";
import { publicMessage } from "../../service/http_retry";
import type { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { KEY_SEPARATOR, type EmbeddableEntity } from "./embedding_index";
import {
   SyncBudgetReached,
   SyncRequestBudget,
   runPooled,
} from "./get_context_llm";
import { scrubForEgress } from "./keyphrases";

/** Most fields rendered for one source (and for each joined source). */
export const SOURCE_SUMMARY_MAX_FIELDS = 200;
/** Most joined sources rendered under one source. */
export const SOURCE_SUMMARY_MAX_JOINED_SOURCES = 20;
/** Deepest join chain rendered under one source. */
export const SOURCE_SUMMARY_JOIN_DEPTH = 3;
/** One field's (or the source's own) `#(doc)` is cut to this many characters. */
export const SOURCE_SUMMARY_MAX_FIELD_DOC_CHARS = 500;
/**
 * The message sent for one source is cut to at most this many characters (about
 * 15,000 tokens). That fits the hosted models' context windows, which are far
 * larger, and does NOT fit a local model run at its default window.
 */
export const SOURCE_SUMMARY_MAX_PROMPT_CHARS = 60_000;
/**
 * The cap for a local model (Ollama). Its default context is 2,048 to 4,096
 * tokens and a prompt past it is cut silently, so the summary would be written
 * from a partial field list with no error. 6,000 characters is about 1,500
 * tokens: room for the instructions and the reply inside the smaller default.
 * A source with more fields than that shows fewer of them, with the line that
 * says what was left out.
 */
export const SOURCE_SUMMARY_SMALL_CONTEXT_PROMPT_CHARS = 6_000;
/** A summary longer than this is refused; the prompt asks for far less. */
export const SOURCE_SUMMARY_MAX_CHARS = 4000;
/** Reply budget: 500 tokens of prose, the one-liner and the JSON around them. */
const SOURCE_SUMMARY_MAX_TOKENS = 1000;

/** The source summary step failed. The sync reports it with the stage name `source_summary`. */
export class SourceSummaryStageError extends Error {
   readonly stage = "source_summary" as const;
   constructor(
      message: string,
      cause?: unknown,
      /** The same failure worded for a caller: no endpoint. See publicMessage. */
      readonly publicMessage?: string,
   ) {
      super(message, { cause });
      this.name = "SourceSummaryStageError";
   }
}

/** What the step needs to run, resolved once per sync. */
export interface SourceSummarySettings {
   chat: ChatModel;
   /** Stored with each summary: `provider/model`. */
   modelId: string;
   /** The instructions in force: the package's prompt file or the built-in one. */
   instructions: string;
   /** See sourceSummaryPromptHash. */
   promptHash: string;
   concurrency: number;
   maxCallsPerSync: number;
   /**
    * The longest message sent for one source; default
    * SOURCE_SUMMARY_MAX_PROMPT_CHARS. Part of the input hash, because it
    * decides how much of the field list the model sees.
    */
   maxPromptChars?: number;
}

// ---------------------------------------------------------------------------
// Rendering a source's fields
// ---------------------------------------------------------------------------

/** The entity fields rendering reads; a subset of EmbeddableEntity. */
type SummaryEntity = Pick<
   EmbeddableEntity,
   | "kind"
   | "name"
   | "source"
   | "modelPath"
   | "embedDoc"
   | "dataType"
   | "relationship"
   | "joinTarget"
   | "joinTargetModelPath"
   | "joinPath"
>;

/**
 * The key a source's summary is stored and read under: its file and its name.
 * The same form as sourceContextKey in get_context_tool, so a card finds its
 * own summary.
 */
export function summaryKey(modelPath: string, source: string): string {
   return [modelPath, source].join(KEY_SEPARATOR);
}

const FIELD_KINDS = ["dimension", "measure", "view", "join"] as const;
const GROUPS = [
   { kind: "dimension", title: "Dimensions" },
   { kind: "measure", title: "Measures" },
   { kind: "view", title: "Views" },
   { kind: "join", title: "Joins" },
] as const;

const isFieldKind = (kind: string): boolean =>
   (FIELD_KINDS as readonly string[]).includes(kind);

/** Text on one line, cut to `max` characters with "..." when it was longer. */
function clipDoc(text: string, max: number): string {
   return text.length > max ? `${text.slice(0, max)}...` : text;
}

/** Finds the source a join leads to, or undefined when it is not one the package indexes. */
type ResolveJoin = (join: SummaryEntity) => SourceFields | undefined;

/**
 * `name (type): description`, leaving out the parts a field does not have. A
 * join names its target source only when that source is one the package
 * indexes (`resolve` finds it): a target that is not an entity is hidden or
 * denied, and its name must not reach the model through the join that points at
 * it. A field's description is cut to SOURCE_SUMMARY_MAX_FIELD_DOC_CHARS.
 */
function fieldLine(e: SummaryEntity, resolve: ResolveJoin): string {
   const doc = clipDoc(
      scrubForEgress(e.embedDoc ?? ""),
      SOURCE_SUMMARY_MAX_FIELD_DOC_CHARS,
   );
   let type = e.dataType ?? "";
   if (e.kind === "join") {
      type = [
         e.relationship,
         e.joinTarget && resolve(e) ? `source ${e.joinTarget}` : "",
      ]
         .filter(Boolean)
         .join(", ");
   }
   return `- ${e.name}${type ? ` (${type})` : ""}${doc ? `: ${doc}` : ""}`;
}

/**
 * The lines of one source's field groups, at most `SOURCE_SUMMARY_MAX_FIELDS`
 * of them. When there are more, the cap is spread over the groups (one field
 * from each in turn) so no group is cut away entirely, and a final line says
 * how many are shown.
 */
function renderGroups(
   fields: readonly SummaryEntity[],
   includeJoins: boolean,
   indent: string,
   resolve: ResolveJoin,
   maxFields: number,
): string[] {
   const groups = GROUPS.filter((g) => includeJoins || g.kind !== "join").map(
      (g) => ({
         title: g.title,
         lines: fields
            .filter((f) => f.kind === g.kind)
            .map((f) => fieldLine(f, resolve)),
      }),
   );
   const total = groups.reduce((n, g) => n + g.lines.length, 0);
   const keep = groups.map((g) => (total > maxFields ? 0 : g.lines.length));
   if (total > maxFields) {
      let left = maxFields;
      for (let round = 0; left > 0; round++) {
         let took = false;
         groups.forEach((g, i) => {
            if (left > 0 && round < g.lines.length) {
               keep[i] += 1;
               left -= 1;
               took = true;
            }
         });
         if (!took) break;
      }
   }
   const out: string[] = [];
   groups.forEach((g, i) => {
      if (g.lines.length === 0) return;
      out.push(`${indent}${g.title}:`);
      for (const line of g.lines.slice(0, keep[i])) out.push(indent + line);
   });
   if (total > maxFields) {
      out.push(
         `${indent}(Showing ${maxFields} of ${total} fields. The rest are not shown.)`,
      );
   }
   return out;
}

/** One source's entities and where its joins lead, indexed once per package. */
interface SourceFields {
   modelPath: string;
   source: string;
   fields: SummaryEntity[];
}

function indexSources(entities: readonly SummaryEntity[]): {
   byKey: Map<string, SourceFields>;
   resolve: ResolveJoin;
} {
   const byKey = new Map<string, SourceFields>();
   const byName = new Map<string, SourceFields[]>();
   for (const e of entities) {
      if (e.kind !== "source") continue;
      const entry: SourceFields = {
         modelPath: e.modelPath,
         source: e.name,
         fields: [],
      };
      byKey.set(summaryKey(e.modelPath, e.name), entry);
      byName.set(e.name, [...(byName.get(e.name) ?? []), entry]);
   }
   for (const e of entities) {
      // A joined copy (`buyer.name`) is not a field of the source: the join
      // that leads to it is listed, and the joined source is nested under it.
      if (!isFieldKind(e.kind) || e.source === undefined || e.joinPath) {
         continue;
      }
      byKey.get(summaryKey(e.modelPath, e.source))?.fields.push(e);
   }
   // A join names its target's file when the compiled model said so. With no
   // file recorded it resolves only when one source has that name: guessing
   // among several would nest another file's fields.
   const resolve: ResolveJoin = (join) => {
      if (!join.joinTarget) return undefined;
      if (join.joinTargetModelPath !== undefined) {
         return byKey.get(
            summaryKey(join.joinTargetModelPath, join.joinTarget),
         );
      }
      const named = byName.get(join.joinTarget) ?? [];
      return named.length === 1 ? named[0] : undefined;
   };
   return { byKey, resolve };
}

/** How much of a source's fields and joins one rendering shows. */
interface RenderLimits {
   maxFields: number;
   maxJoinedSources: number;
}

/**
 * The field list for one source: its own groups, then each joined source it
 * reaches (breadth first, up to SOURCE_SUMMARY_JOIN_DEPTH deep and
 * `maxJoinedSources` in all) with that source's own fields, nested under a
 * header that names the join path. A join whose target is not in the package,
 * or one that would loop back to a source already on its path, is listed under
 * Joins and not expanded.
 */
function renderSourceFields(
   root: SourceFields,
   resolve: ResolveJoin,
   limits: RenderLimits,
): string {
   const lines = renderGroups(root.fields, true, "", resolve, limits.maxFields);
   const queue: Array<{
      from: SourceFields;
      path: string[];
      chain: string[];
   }> = [
      {
         from: root,
         path: [],
         chain: [summaryKey(root.modelPath, root.source)],
      },
   ];
   let shown = 0;
   let skipped = 0;
   while (queue.length > 0) {
      const { from, path, chain } = queue.shift() as (typeof queue)[number];
      for (const join of from.fields.filter((f) => f.kind === "join")) {
         const target = resolve(join);
         const targetKey = target
            ? summaryKey(target.modelPath, target.source)
            : undefined;
         if (!target || !targetKey || chain.includes(targetKey)) continue;
         const nextPath = [...path, join.name];
         if (shown >= limits.maxJoinedSources) {
            skipped += 1;
            continue;
         }
         shown += 1;
         lines.push(
            "",
            `Joined source ${target.source} as ${nextPath.join(".")}${join.relationship ? ` (${join.relationship})` : ""}:`,
            ...renderGroups(
               target.fields,
               false,
               "  ",
               resolve,
               limits.maxFields,
            ),
         );
         if (nextPath.length < SOURCE_SUMMARY_JOIN_DEPTH) {
            queue.push({
               from: target,
               path: nextPath,
               chain: [...chain, targetKey],
            });
         }
      }
   }
   if (skipped > 0) {
      lines.push(
         "",
         `(${skipped} more joined ${skipped === 1 ? "source is" : "sources are"} not shown.)`,
      );
   }
   return lines.join("\n");
}

/** What one source's summary is made from. */
export interface SourceSummaryInput {
   source: string;
   modelPath: string;
   /** True when the source has `#(doc)` text. */
   hasDoc: boolean;
   /** The exact user message the model is sent. */
   prompt: string;
}

/**
 * The sources that get a summary, with the message each is sent. A source with
 * no fields of its own (nothing to summarize) is skipped; a source the
 * discovery surface hides or an access rule locks never became an entity, so it
 * is not here either.
 */
export function buildSourceSummaryInputs(
   entities: readonly SummaryEntity[],
   maxPromptChars: number = SOURCE_SUMMARY_MAX_PROMPT_CHARS,
): SourceSummaryInput[] {
   const { byKey, resolve } = indexSources(entities);
   const out: SourceSummaryInput[] = [];
   const seen = new Set<string>();
   for (const e of entities) {
      const key = summaryKey(e.modelPath, e.name);
      if (e.kind !== "source" || seen.has(key)) continue;
      seen.add(key);
      const own = byKey.get(key);
      if (!own || own.fields.length === 0) continue;
      const doc = scrubForEgress(e.embedDoc ?? "");
      out.push({
         source: e.name,
         modelPath: e.modelPath,
         hasDoc: doc !== "",
         prompt: boundedPrompt(e.name, doc, own, resolve, maxPromptChars),
      });
   }
   return out;
}

/**
 * The message for one source, at most `maxPromptChars` long. When
 * the full rendering is longer, fewer joined sources are shown (halving down to
 * none), then fewer fields (halving, never under 5), until it fits. The lines
 * that say what is not shown stay in, so the model does not take a part for the
 * whole.
 */
function boundedPrompt(
   name: string,
   doc: string,
   own: SourceFields,
   resolve: ResolveJoin,
   maxPromptChars: number,
): string {
   const limits: RenderLimits = {
      maxFields: SOURCE_SUMMARY_MAX_FIELDS,
      maxJoinedSources: SOURCE_SUMMARY_MAX_JOINED_SOURCES,
   };
   const docText = clipDoc(doc, SOURCE_SUMMARY_MAX_FIELD_DOC_CHARS);
   for (;;) {
      const prompt = renderSourceSummaryUserPrompt({
         source: name,
         doc: docText,
         fields: renderSourceFields(own, resolve, limits),
      });
      if (prompt.length <= maxPromptChars) return prompt;
      if (limits.maxJoinedSources > 0) {
         limits.maxJoinedSources = Math.floor(limits.maxJoinedSources / 2);
      } else if (limits.maxFields > 5) {
         limits.maxFields = Math.max(5, Math.floor(limits.maxFields / 2));
      } else {
         // Five fields of at most 500 characters each cannot reach the
         // default cap, so this is unreachable unless the constants change (a
         // small-context cap can reach it). Cut hard rather than loop.
         return prompt.slice(0, maxPromptChars);
      }
   }
}

/** Hash of everything that decides a stored summary: the message, the prompt and the model. */
export function sourceSummaryInputHash(
   input: SourceSummaryInput,
   settings: Pick<SourceSummarySettings, "promptHash" | "modelId">,
): string {
   return createHash("sha256")
      .update(settings.promptHash)
      .update("\u0000")
      .update(settings.modelId)
      .update("\u0000")
      .update(input.prompt)
      .digest("hex");
}

/**
 * Extra inputs the readiness fingerprint must see. The fingerprint is built
 * from row texts, which carry names and docs but not field types, join targets
 * or the prompt; with summaries on, those also decide a stored summary, so
 * they are folded in. Empty string when summaries are off.
 */
export function sourceSummaryInputsDigest(
   entities: readonly SummaryEntity[],
   settings:
      | Pick<SourceSummarySettings, "promptHash" | "modelId" | "maxPromptChars">
      | undefined,
): string {
   if (!settings) return "";
   const rows = buildSourceSummaryInputs(entities, settings.maxPromptChars).map(
      (i) =>
         `${i.modelPath}\u0000${i.source}\u0000${sourceSummaryInputHash(i, settings)}`,
   );
   rows.sort();
   return createHash("sha256").update(rows.join("\n")).digest("hex");
}

const hashesCache = new WeakMap<
   readonly SummaryEntity[],
   Map<string, Map<string, string>>
>();

/**
 * The input hash a stored summary must carry to be current, by summaryKey, for
 * each source that gets one. What a request checks stored rows against, so it
 * must be cheap: for a frozen entity list (get_context's package index hands
 * over the same one every call) it is computed once per prompt and model and
 * reused.
 */
export function currentSummaryHashes(
   entities: readonly SummaryEntity[],
   settings: Pick<
      SourceSummarySettings,
      "promptHash" | "modelId" | "maxPromptChars"
   >,
): ReadonlyMap<string, string> {
   const frozen = Object.isFrozen(entities);
   const variant = `${settings.promptHash}\u0000${settings.modelId}\u0000${settings.maxPromptChars ?? ""}`;
   const cached = frozen ? hashesCache.get(entities)?.get(variant) : undefined;
   if (cached) return cached;
   const hashes = new Map(
      buildSourceSummaryInputs(entities, settings.maxPromptChars).map((i) => [
         summaryKey(i.modelPath, i.source),
         sourceSummaryInputHash(i, settings),
      ]),
   );
   if (frozen) {
      const byVariant = hashesCache.get(entities) ?? new Map();
      byVariant.set(variant, hashes);
      hashesCache.set(entities, byVariant);
   }
   return hashes;
}

/** How many sources get a summary: the progress denominator. */
export function countSummarizableSources(
   entities: readonly SummaryEntity[],
): number {
   return buildSourceSummaryInputs(entities).length;
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

export interface GeneratedSummary {
   summary: string;
   oneLineSummary: string;
}

/**
 * Validate a reply: `{summary, one_line_summary}`. Every problem is named in
 * one message, which the provider layer shows the model for its single re-ask.
 * A source with no documentation gets exactly "The `<name>` source." as its
 * one-liner, so the model cannot invent a purpose the inputs do not state. The
 * validator writes that line itself and ignores what the model sent for it:
 * the line is fixed and known, so rejecting a miss would only spend a re-ask,
 * and a second miss would fail the whole sync and put the package in cooldown.
 */
export function validateSourceSummary(source: string, hasDoc: boolean) {
   return (value: unknown): GeneratedSummary => {
      if (typeof value !== "object" || value === null || Array.isArray(value)) {
         throw new Error(
            'expected a JSON object {"summary": "...", "one_line_summary": "..."}',
         );
      }
      const reply = value as Record<string, unknown>;
      const problems: string[] = [];

      let summary = "";
      if (typeof reply.summary !== "string" || reply.summary.trim() === "") {
         problems.push('"summary" is missing or is not a non-empty string');
      } else {
         summary = reply.summary.replace(/\r\n?/g, "\n").trim();
         if (summary.length > SOURCE_SUMMARY_MAX_CHARS) {
            problems.push(
               `"summary" is ${summary.length} characters; the limit is ${SOURCE_SUMMARY_MAX_CHARS} (aim for 200 to 500 tokens)`,
            );
         }
      }

      let oneLine = "";
      if (!hasDoc) {
         oneLine = undocumentedOneLine(source);
      } else if (
         typeof reply.one_line_summary !== "string" ||
         reply.one_line_summary.trim() === ""
      ) {
         problems.push(
            '"one_line_summary" is missing or is not a non-empty string',
         );
      } else {
         oneLine = reply.one_line_summary.trim();
         if (/[\r\n]/.test(oneLine)) {
            problems.push('"one_line_summary" must be one line');
         } else if (oneLine.length > ONE_LINE_SUMMARY_MAX_CHARS) {
            problems.push(
               `"one_line_summary" is ${oneLine.length} characters; the limit is ${ONE_LINE_SUMMARY_MAX_CHARS}`,
            );
         }
      }

      if (problems.length > 0) throw new Error(problems.join("; "));
      return { summary, oneLineSummary: oneLine };
   };
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

interface StoredSummaryRow {
   model_path: string;
   source_name: string;
   input_hash: string;
   summary: string;
   one_line_summary: string;
}

async function loadRows(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
): Promise<StoredSummaryRow[]> {
   return db.all<StoredSummaryRow>(
      `SELECT model_path, source_name, input_hash, summary, one_line_summary
       FROM source_summaries
       WHERE environment_name = ? AND package_name = ?`,
      [environmentName, packageName],
   );
}

async function saveRow(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
   input: SourceSummaryInput,
   inputHash: string,
   modelId: string,
   generated: GeneratedSummary,
): Promise<void> {
   await db.run(
      `INSERT INTO source_summaries (
         environment_name, package_name, model_path, source_name, input_hash,
         model, summary, one_line_summary, created_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (environment_name, package_name, model_path, source_name)
       DO UPDATE SET
         input_hash = EXCLUDED.input_hash,
         model = EXCLUDED.model,
         summary = EXCLUDED.summary,
         one_line_summary = EXCLUDED.one_line_summary,
         created_at = EXCLUDED.created_at`,
      [
         environmentName,
         packageName,
         input.modelPath,
         input.source,
         inputHash,
         modelId,
         generated.summary,
         generated.oneLineSummary,
         new Date().toISOString(),
      ],
   );
}

async function deleteSources(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
   rows: Array<{ modelPath: string; source: string }>,
): Promise<void> {
   for (let i = 0; i < rows.length; i += 100) {
      const chunk = rows.slice(i, i + 100);
      await db.run(
         `DELETE FROM source_summaries
          WHERE environment_name = ? AND package_name = ?
            AND (${chunk.map(() => "(model_path = ? AND source_name = ?)").join(" OR ")})`,
         [
            environmentName,
            packageName,
            ...chunk.flatMap((r) => [r.modelPath, r.source]),
         ],
      );
   }
}

/** Remove a deleted package's summaries. */
export async function deletePackageSourceSummaries(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
): Promise<void> {
   await db.run(
      `DELETE FROM source_summaries
       WHERE environment_name = ? AND package_name = ?`,
      [environmentName, packageName],
   );
}

/** Remove a deleted environment's summaries. */
export async function deleteEnvironmentSourceSummaries(
   db: DuckDBConnection,
   environmentName: string,
): Promise<void> {
   await db.run(`DELETE FROM source_summaries WHERE environment_name = ?`, [
      environmentName,
   ]);
}

/** A stored summary, as get_context reads it. */
export interface StoredSourceSummary {
   summary: string;
   oneLineSummary: string;
}

/**
 * Every stored summary of a package, by {@link summaryKey}. Read once per
 * request; the table holds one small row per source. A source with no row is
 * simply absent, and the response then has no summary fields for it.
 *
 * With `current` (see currentSummaryHashes), a row is kept only when its input
 * hash is the current one. A reload that changed a source leaves its old
 * summary in the table until the next sync rewrites it, and a request that
 * skips the readiness gate (source targets only) would otherwise show it, and
 * send it to source match, as if it described the source now.
 */
export async function loadSourceSummaries(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
   current?: ReadonlyMap<string, string>,
): Promise<Map<string, StoredSourceSummary>> {
   const rows = await loadRows(db, environmentName, packageName);
   return new Map(
      rows.flatMap((r): Array<[string, StoredSourceSummary]> => {
         const key = summaryKey(r.model_path, r.source_name);
         if (current && current.get(key) !== r.input_hash) return [];
         return [
            [key, { summary: r.summary, oneLineSummary: r.one_line_summary }],
         ];
      }),
   );
}

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

export interface SourceSummaryProgress {
   /** Sources that get a summary and have a current one stored. */
   done: number;
   /** Sources that get a summary. */
   total: number;
   /** True when maxCallsPerSync stopped the step before every source had one. */
   capped: boolean;
}

export interface SourceSummaryOutcome {
   progress: SourceSummaryProgress;
   /** Chat calls made, counted against the sync's `maxCallsPerSync`. */
   calls: number;
}

/**
 * Bring the stored summaries in line with the package: keep the ones whose
 * input hash still matches, delete the rest (and those of sources that left),
 * and write one new summary per remaining source, one call each,
 * `concurrency` calls at a time, each saved as it returns. The calls draw on
 * the sync's request budget (`maxCallsPerSync` HTTP requests, shared with the
 * keyphrase step, a JSON repair counting as a second request); sources the
 * budget does not reach are left without a summary and reported through
 * `progress.capped`. A call that fails after the provider layer's retries
 * stops the step with a {@link SourceSummaryStageError}: loud, never a silent
 * downgrade, and what was saved stays.
 */
export async function resolveSourceSummaries(args: {
   db: DuckDBConnection;
   environmentName: string;
   packageName: string;
   /**
    * Every entity of the package, one per model file. Not deduplicated by
    * (kind, source, name): two files can each define `orders`, and each gets
    * its own summary read from its own fields and joins.
    */
   entities: readonly EmbeddableEntity[];
   settings: SourceSummarySettings;
   /**
    * The sync's request budget, shared with the keyphrase step. Absent: a
    * fresh one of the settings' `maxCallsPerSync` requests.
    */
   budget?: SyncRequestBudget;
   /** True when the package is going away: no further request is sent. */
   shouldStop?: () => boolean;
   onProgress?: (progress: SourceSummaryProgress) => void;
}): Promise<SourceSummaryOutcome> {
   const { db, environmentName, packageName, entities, settings, onProgress } =
      args;
   const budget =
      args.budget ?? new SyncRequestBudget(settings.maxCallsPerSync);
   const requestsBefore = budget.requests;
   const inputs = buildSourceSummaryInputs(entities, settings.maxPromptChars);
   const stored = await loadRows(db, environmentName, packageName);
   const storedByKey = new Map(
      stored.map((r) => [summaryKey(r.model_path, r.source_name), r]),
   );

   const pending: Array<{ input: SourceSummaryInput; hash: string }> = [];
   const stale = new Map(
      stored.map((r) => [
         summaryKey(r.model_path, r.source_name),
         { modelPath: r.model_path, source: r.source_name },
      ]),
   );
   for (const input of inputs) {
      const key = summaryKey(input.modelPath, input.source);
      const hash = sourceSummaryInputHash(input, settings);
      if (storedByKey.get(key)?.input_hash === hash) {
         stale.delete(key);
      } else {
         pending.push({ input, hash });
      }
   }
   // A row whose inputs changed, or whose source left or no longer qualifies,
   // is removed before anything is regenerated, so nothing stale is ever served.
   if (stale.size > 0) {
      await deleteSources(db, environmentName, packageName, [
         ...stale.values(),
      ]);
   }

   const progress: SourceSummaryProgress = {
      done: inputs.length - pending.length,
      total: inputs.length,
      capped: false,
   };
   onProgress?.({ ...progress });
   if (pending.length === 0) return { progress, calls: 0 };

   try {
      await runPooled(
         pending,
         settings.concurrency,
         async ({ input, hash }) => {
            // Out of budget, or the package is going away: this source and the
            // rest are left for the next sync, which resumes from what is stored.
            if (args.shouldStop?.() || budget.exhausted) return;
            let reply;
            try {
               reply = await budget.spend((onRequest) =>
                  settings.chat.completeJson({
                     system: settings.instructions,
                     prompt: input.prompt,
                     maxTokens: SOURCE_SUMMARY_MAX_TOKENS,
                     validate: validateSourceSummary(
                        input.source,
                        input.hasDoc,
                     ),
                     onRequest,
                  }),
               );
            } catch (error) {
               if (error instanceof SyncBudgetReached) return;
               throw error;
            }
            await saveRow(
               db,
               environmentName,
               packageName,
               input,
               hash,
               settings.modelId,
               reply.value,
            );
            progress.done += 1;
            onProgress?.({ ...progress });
         },
      );
   } catch (failure) {
      const message =
         failure instanceof Error ? failure.message : String(failure);
      const after = `Source summary generation failed after ${progress.done} of ${progress.total} sources`;
      throw new SourceSummaryStageError(
         `${after}: ${message}`,
         failure,
         `${after}: ${publicMessage(failure)}`,
      );
   }
   // What the budget did not reach. Published through onProgress too: the
   // sync set the progress before this step started, so a `capped` set only
   // here would never be seen.
   if (progress.done < progress.total && !args.shouldStop?.()) {
      progress.capped = true;
      onProgress?.({ ...progress });
      logger.warn(
         "[get_context] Source summary generation stopped at the per-sync call limit; the rest are left for the next sync",
         {
            environmentName,
            packageName,
            limit: settings.maxCallsPerSync,
            setting: "retrieval.llm.maxCallsPerSync",
            remainingSources: progress.total - progress.done,
         },
      );
   }
   return { progress, calls: budget.requests - requestsBefore };
}
