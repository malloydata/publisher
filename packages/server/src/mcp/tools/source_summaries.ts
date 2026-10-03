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
 * A stored summary is reused only while its input hash still matches. The hash
 * covers the exact user message (source, docs, field list), the prompt text and
 * the model, so editing a doc, a field, the prompt file or the model rewrites
 * exactly the sources it touches, and an unchanged source costs no LLM call on
 * a restart, a reload or a republish. After a sync every stored row matches
 * the current inputs: a row that no longer does (and could not be rewritten
 * because the per-sync call limit was reached) is deleted rather than served.
 */

import { createHash } from "crypto";
import { logger } from "../../logger";
import {
   NO_SOURCE_DOCS,
   ONE_LINE_SUMMARY_MAX_CHARS,
   renderSourceSummaryUserPrompt,
   undocumentedOneLine,
} from "../../prompts/source_summary";
import type { ChatModel } from "../../providers/types";
import type { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import type { EmbeddableEntity } from "./embedding_index";
import { runPooled } from "./get_context_llm";
import { scrubForEgress } from "./keyphrases";

/** Most fields rendered for one source (and for each joined source). */
export const SOURCE_SUMMARY_MAX_FIELDS = 200;
/** Most joined sources rendered under one source. */
export const SOURCE_SUMMARY_MAX_JOINED_SOURCES = 20;
/** Deepest join chain rendered under one source. */
export const SOURCE_SUMMARY_JOIN_DEPTH = 3;
/** A summary longer than this is refused; the prompt asks for far less. */
export const SOURCE_SUMMARY_MAX_CHARS = 4000;
/** Reply budget: 500 tokens of prose, the one-liner and the JSON around them. */
const SOURCE_SUMMARY_MAX_TOKENS = 1000;

/** The source summary step failed. The sync reports it with the stage name `source_summary`. */
export class SourceSummaryStageError extends Error {
   readonly stage = "source_summary" as const;
   constructor(message: string, cause?: unknown) {
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
>;

const FIELD_KINDS = ["dimension", "measure", "view", "join"] as const;
const GROUPS = [
   { kind: "dimension", title: "Dimensions" },
   { kind: "measure", title: "Measures" },
   { kind: "view", title: "Views" },
   { kind: "join", title: "Joins" },
] as const;

const isFieldKind = (kind: string): boolean =>
   (FIELD_KINDS as readonly string[]).includes(kind);

/** `name (type): description`, leaving out the parts a field does not have. */
function fieldLine(e: SummaryEntity): string {
   const doc = scrubForEgress(e.embedDoc ?? "");
   let type = e.dataType ?? "";
   if (e.kind === "join") {
      type = [e.relationship, e.joinTarget ? `source ${e.joinTarget}` : ""]
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
): string[] {
   const groups = GROUPS.filter((g) => includeJoins || g.kind !== "join").map(
      (g) => ({
         title: g.title,
         lines: fields.filter((f) => f.kind === g.kind).map(fieldLine),
      }),
   );
   const total = groups.reduce((n, g) => n + g.lines.length, 0);
   const keep = groups.map((g) =>
      total > SOURCE_SUMMARY_MAX_FIELDS ? 0 : g.lines.length,
   );
   if (total > SOURCE_SUMMARY_MAX_FIELDS) {
      let left = SOURCE_SUMMARY_MAX_FIELDS;
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
   if (total > SOURCE_SUMMARY_MAX_FIELDS) {
      out.push(
         `${indent}(Showing ${SOURCE_SUMMARY_MAX_FIELDS} of ${total} fields. The rest are not shown.)`,
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
   byName: Map<string, SourceFields>;
} {
   const byKey = new Map<string, SourceFields>();
   const byName = new Map<string, SourceFields>();
   const keyOf = (modelPath: string, source: string) =>
      JSON.stringify([modelPath, source]);
   for (const e of entities) {
      if (e.kind !== "source") continue;
      const entry: SourceFields = {
         modelPath: e.modelPath,
         source: e.name,
         fields: [],
      };
      byKey.set(keyOf(e.modelPath, e.name), entry);
      if (!byName.has(e.name)) byName.set(e.name, entry);
   }
   for (const e of entities) {
      if (!isFieldKind(e.kind) || e.source === undefined) continue;
      byKey.get(keyOf(e.modelPath, e.source))?.fields.push(e);
   }
   return { byKey, byName };
}

/**
 * The field list for one source: its own groups, then each joined source it
 * reaches (breadth first, up to SOURCE_SUMMARY_JOIN_DEPTH deep and
 * SOURCE_SUMMARY_MAX_JOINED_SOURCES in all) with that source's own fields,
 * nested under a header that names the join path. A join whose target is not
 * in the package, or one that would loop back to a source already on its path,
 * is listed under Joins and not expanded.
 */
export function renderSourceFields(
   root: SourceFields,
   byName: ReadonlyMap<string, SourceFields>,
): string {
   const lines = renderGroups(root.fields, true, "");
   const queue: Array<{
      from: SourceFields;
      path: string[];
      chain: string[];
   }> = [{ from: root, path: [], chain: [root.source] }];
   let shown = 0;
   let skipped = 0;
   while (queue.length > 0) {
      const { from, path, chain } = queue.shift() as (typeof queue)[number];
      for (const join of from.fields.filter((f) => f.kind === "join")) {
         if (!join.joinTarget) continue;
         const target = byName.get(join.joinTarget);
         if (!target || chain.includes(target.source)) continue;
         const nextPath = [...path, join.name];
         if (shown >= SOURCE_SUMMARY_MAX_JOINED_SOURCES) {
            skipped += 1;
            continue;
         }
         shown += 1;
         lines.push(
            "",
            `Joined source ${target.source} as ${nextPath.join(".")}${join.relationship ? ` (${join.relationship})` : ""}:`,
            ...renderGroups(target.fields, false, "  "),
         );
         if (nextPath.length < SOURCE_SUMMARY_JOIN_DEPTH) {
            queue.push({
               from: target,
               path: nextPath,
               chain: [...chain, target.source],
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
): SourceSummaryInput[] {
   const { byKey, byName } = indexSources(entities);
   const out: SourceSummaryInput[] = [];
   const seen = new Set<string>();
   for (const e of entities) {
      if (e.kind !== "source" || seen.has(e.name)) continue;
      seen.add(e.name);
      const own = byKey.get(JSON.stringify([e.modelPath, e.name]));
      if (!own || own.fields.length === 0) continue;
      const doc = scrubForEgress(e.embedDoc ?? "");
      out.push({
         source: e.name,
         modelPath: e.modelPath,
         hasDoc: doc !== "",
         prompt: renderSourceSummaryUserPrompt({
            source: e.name,
            doc,
            fields: renderSourceFields(own, byName),
         }),
      });
   }
   return out;
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
   settings: Pick<SourceSummarySettings, "promptHash" | "modelId"> | undefined,
): string {
   if (!settings) return "";
   const rows = buildSourceSummaryInputs(entities).map(
      (i) => `${i.source}\u0000${sourceSummaryInputHash(i, settings)}`,
   );
   rows.sort();
   return createHash("sha256").update(rows.join("\n")).digest("hex");
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
 * A source with no documentation must get exactly "The `<name>` source.", so
 * the model cannot invent a purpose the inputs do not state.
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
      if (
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
         if (!hasDoc && oneLine !== undocumentedOneLine(source)) {
            problems.push(
               `the source documentation is "${NO_SOURCE_DOCS}", so "one_line_summary" must be exactly: ${undocumentedOneLine(source)}`,
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
      `SELECT source_name, input_hash, summary, one_line_summary
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
   names: string[],
): Promise<void> {
   for (let i = 0; i < names.length; i += 200) {
      const chunk = names.slice(i, i + 200);
      await db.run(
         `DELETE FROM source_summaries
          WHERE environment_name = ? AND package_name = ?
            AND source_name IN (${chunk.map(() => "?").join(", ")})`,
         [environmentName, packageName, ...chunk],
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
 * Every stored summary of a package, by source name. Read once per request;
 * the table holds one small row per source. A source with no row is simply
 * absent, and the response then has no summary fields for it.
 */
export async function loadSourceSummaries(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
): Promise<Map<string, StoredSourceSummary>> {
   const rows = await loadRows(db, environmentName, packageName);
   return new Map(
      rows.map((r) => [
         r.source_name,
         { summary: r.summary, oneLineSummary: r.one_line_summary },
      ]),
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
 * `concurrency` calls at a time, each saved as it returns. At most
 * `callBudget` calls run (the sync's `maxCallsPerSync` less what the keyphrase
 * step used); sources beyond that are left without a summary and reported
 * through `progress.capped`. A call that fails after the provider layer's
 * retries stops the step with a {@link SourceSummaryStageError}: loud, never a
 * silent downgrade, and what was saved stays.
 */
export async function resolveSourceSummaries(args: {
   db: DuckDBConnection;
   environmentName: string;
   packageName: string;
   /** Deduplicated entities, as the sync sees them. */
   entities: readonly EmbeddableEntity[];
   settings: SourceSummarySettings;
   /** Calls this step may make; defaults to the settings' `maxCallsPerSync`. */
   callBudget?: number;
   onProgress?: (progress: SourceSummaryProgress) => void;
}): Promise<SourceSummaryOutcome> {
   const { db, environmentName, packageName, entities, settings, onProgress } =
      args;
   const inputs = buildSourceSummaryInputs(entities);
   const stored = await loadRows(db, environmentName, packageName);
   const storedByName = new Map(stored.map((r) => [r.source_name, r]));

   const pending: Array<{ input: SourceSummaryInput; hash: string }> = [];
   const stale = new Set(stored.map((r) => r.source_name));
   for (const input of inputs) {
      const hash = sourceSummaryInputHash(input, settings);
      if (storedByName.get(input.source)?.input_hash === hash) {
         stale.delete(input.source);
      } else {
         pending.push({ input, hash });
      }
   }
   // A row whose inputs changed, or whose source left or no longer qualifies,
   // is removed before anything is regenerated, so nothing stale is ever served.
   if (stale.size > 0) {
      await deleteSources(db, environmentName, packageName, [...stale]);
   }

   const progress: SourceSummaryProgress = {
      done: inputs.length - pending.length,
      total: inputs.length,
      capped: false,
   };
   onProgress?.({ ...progress });
   if (pending.length === 0) return { progress, calls: 0 };

   const budget = Math.max(0, args.callBudget ?? settings.maxCallsPerSync);
   const jobs = pending.slice(0, budget);
   if (jobs.length < pending.length) {
      progress.capped = true;
      logger.warn(
         "[get_context] Source summary generation stopped at the per-sync call limit; the rest are left for the next sync",
         {
            environmentName,
            packageName,
            limit: settings.maxCallsPerSync,
            setting: "retrieval.llm.maxCallsPerSync",
            remainingSources: pending.length - jobs.length,
         },
      );
   }

   let calls = 0;
   try {
      await runPooled(jobs, settings.concurrency, async ({ input, hash }) => {
         calls += 1;
         const reply = await settings.chat.completeJson({
            system: settings.instructions,
            prompt: input.prompt,
            maxTokens: SOURCE_SUMMARY_MAX_TOKENS,
            validate: validateSourceSummary(input.source, input.hasDoc),
         });
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
      });
   } catch (failure) {
      const message =
         failure instanceof Error ? failure.message : String(failure);
      throw new SourceSummaryStageError(
         `Source summary generation failed after ${progress.done} of ${progress.total} sources: ${message}`,
         failure,
      );
   }
   return { progress, calls };
}
