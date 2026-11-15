// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `#(materialize)` source's build copy: the author's file as the build
 * reads it, with the source's given-reading terms replaced by `true`, its own
 * and its inherited `internal` fields' access modifiers lifted (the hidden-
 * field lift), and an extension's materialized parent re-rooted on the
 * parent's generated build source.
 *
 * Read by the companion compiler (`materialize_compile.ts`) and the stored
 * chain (`materialize_stored_chain.ts`).
 *
 * Pure: text in, text out.
 */

import type { MaterializePlan } from "../domain/table_plans";
import {
   applyEdits,
   afterModelHeader,
   declarationSpans,
   accessLiftEdits,
   dropDimensions,
   inverseRenameBlocks,
   type TextEdit,
} from "../text/declaration_text";
import { offsetOf } from "../text/malloy_lexer";
import {
   NOT_FOUND,
   declarationOf,
   termSpans,
   writtenSpan,
   WINDOW_NOT_FOUND,
} from "./materialize_read_time";
import { identifier, stringLiteral } from "../../malloy_text";
import type { TermUse } from "../domain/read_time_terms";
import { givenReadTerms, type SourceDefLike } from "./build_file_generator";
import { leftOutWindowTerms } from "./rolling_window";
import {
   dropJoinDefinition,
   intoFirstExtend,
   inlinedJoinStatements,
   inlinedJoinRenames,
   type InlinedJoinText,
} from "../text/inlined_join_text";

/**
 * Ends the name of a build copy: the author's file with one materialized
 * source's given-reading `where:` terms taken out (P2).
 */
const MATERIALIZE_BUILD_COPY_SUFFIX = ".materialize-build.malloy";

/**
 * The `index`th build copy of an author model, by its file name. One copy per
 * source with read-time terms, so each source's build leaves out its own
 * terms and no other's (see {@link buildCopyText}).
 */
export function buildCopyName(modelFileName: string, index: number): string {
   return `${modelFileName}.${index}${MATERIALIZE_BUILD_COPY_SUFFIX}`;
}

/**
 * The build copy of one planned source's declaring file: `text` with that
 * source's left-out read-time terms replaced by `true` (P2, and a current-time
 * upper bound), so a query over the
 * copy's source applies every `where:` but those, or why a term cannot be
 * placed (see {@link termSpans}), which refuses the source rather than guess.
 *
 * With `liftAccess` (P3), the source's own access modifiers are lifted too
 * ({@link accessLiftEdits}) and the declaration is wrapped in
 * `include { public: * }`, which lifts the `internal` fields it inherits, so
 * the build can select its hidden fields. A field private on a source it
 * extends stays private, and is never stored. The copy then needs the
 * `access_modifiers` flag, which its first line adds.
 *
 * One copy per source, never one for the file: a copy with two sources' terms
 * taken out would let the one source's build read the other (a join to it, a
 * query over it) without that other's term, and store its rows for every
 * caller. In a copy of its own, a source that reads another such source still
 * reads that one's given, and the eligibility gate refuses its build
 * (`given_in_persisted_query`).
 *
 * For an extension table (P10), `chain` names its parent's plan and the file
 * the parent's build source is imported from: the copy also re-roots the
 * parent's declaration on that build source ({@link rerootOnBuildSource}) and
 * imports it, so the extension, as written, reads the parent's table.
 */
export function buildCopyText(
   text: string,
   plan: MaterializePlan,
   chain?: { parent: MaterializePlan; importFrom: string },
   options: { liftAccess?: boolean } = {},
): { copy: string } | { reason: string } {
   const parts = buildCopyParts(text, plan, chain, options);
   return "reason" in parts ? parts : { copy: parts.copy };
}

/**
 * {@link buildCopyText}, with the edits it made to `text` and where it put
 * the parent's import line, so a place in the copy can be traced back to the
 * author's text ({@link buildCopyDeclarationAt}).
 */
function buildCopyParts(
   text: string,
   plan: MaterializePlan,
   chain?: { parent: MaterializePlan; importFrom: string },
   options: { liftAccess?: boolean } = {},
):
   | {
        copy: string;
        edits: TextEdit[];
        imported?: { at: number; length: number };
     }
   | { reason: string } {
   const found = copyEdits(text, plan, chain?.parent, options.liftAccess);
   if ("reason" in found) return found;
   const edits = [...found.edits];
   if (chain) {
      for (const omitted of plan.buildsOver?.omit ?? []) {
         const from = offsetOf(text, omitted.range.start);
         const to = offsetOf(text, omitted.range.end);
         const stub =
            from >= 0 && to >= from
               ? emptySourceOver(text.slice(from, to), chain.parent)
               : undefined;
         if (stub === undefined) return { reason: PARENT_NOT_REROOTED };
         edits.push({ start: from, end: to, insert: stub });
      }
   }
   let copy = applyEdits(text, edits);
   if (copy === undefined) return { reason: NOT_FOUND };
   if (!chain) return { copy, edits };
   const at = afterModelHeader(copy);
   const line = `import { ${identifier(chain.parent.buildSourceName)} } from ${stringLiteral(chain.importFrom)}\n`;
   copy = `${copy.slice(0, at)}${line}${copy.slice(at)}`;
   return { copy, edits, imported: { at, length: line.length } };
}

/**
 * A declaration replaced by an empty source of the same name over the
 * parent's build source: every column taken away, so nothing can read a
 * field of it and compile. Undefined when the name cannot be read.
 */
function emptySourceOver(
   declaration: string,
   parent: MaterializePlan,
): string | undefined {
   const spans = declarationSpans(declaration);
   if (!spans) return undefined;
   const name = declaration.slice(0, spans.nameEnd).trimEnd();
   if (name === "") return undefined;
   const columns = parent.columns.map((c) => identifier(c.column));
   return (
      `${name} is ${identifier(parent.buildSourceName)}` +
      (columns.length > 0 ? ` extend { except: ${columns.join(", ")} }` : "")
   );
}

/**
 * Which of `candidates` (declarations in `text`, the author's file) the place
 * `at` in `plan`'s build copy (zero-based line and character, as Malloy
 * reports a problem) lies in, or undefined: one the copy edits (the plan's
 * own, its parent's, or one already omitted) or a place in none.
 */
export function buildCopyDeclarationAt(
   text: string,
   plan: MaterializePlan,
   chain: { parent: MaterializePlan; importFrom: string },
   options: { liftAccess?: boolean },
   contents: Record<string, unknown>,
   at: { line: number; character: number },
): { sourceID: string; declaration: TermUse } | undefined {
   const parts = buildCopyParts(text, plan, chain, options);
   if ("reason" in parts) return undefined;
   const own = plan.declaration;
   const onChain = [
      ...(own ? [own] : []),
      ...(plan.buildsOver ? [plan.buildsOver.declaration] : []),
      ...(plan.buildsOver?.path ?? []),
   ];
   // The chain is all in one file (see chainExtensionTables).
   const chainURL = plan.buildsOver?.declaration.url;
   const same = (a: TermUse, b: TermUse) =>
      a.url === b.url &&
      a.range.start.line === b.range.start.line &&
      a.range.start.character === b.range.start.character;
   const candidates = Object.values(contents).flatMap((def) => {
      const d = def as SourceDefLike | null;
      const declaration =
         d && typeof d === "object" && typeof d.sourceID === "string"
            ? declarationOf(d)
            : undefined;
      return declaration &&
         declaration.url === chainURL &&
         !onChain.some((c) => same(c, declaration))
         ? [{ sourceID: d?.sourceID as string, declaration }]
         : [];
   });
   const point = offsetOf(parts.copy, at);
   if (point < 0) return undefined;
   // An author offset outside every edit, where the copy has it.
   const inCopy = (offset: number) => {
      let out = offset;
      for (const e of parts.edits) {
         if (e.end <= offset) out += e.insert.length - (e.end - e.start);
      }
      if (parts.imported && parts.imported.at <= out) {
         out += parts.imported.length;
      }
      return out;
   };
   for (const { sourceID, declaration } of candidates) {
      const from = offsetOf(text, declaration.range.start);
      const to = offsetOf(text, declaration.range.end);
      if (from < 0 || to < from) continue;
      if (parts.edits.some((e) => e.start < to && from < e.end)) continue;
      if (inCopy(from) <= point && point <= inCopy(to)) {
         return { sourceID, declaration };
      }
   }
   return undefined;
}

/** Lifts the `internal` fields a build copy's source inherits (P3). */
const LIFT_INHERITED = " include { public: * }";

/** The compiler flag `include { … }` needs, as the build copy's first line. */
export const ACCESS_FLAG = "##! experimental.access_modifiers";

const ACCESS_NOT_LIFTED =
   "the source's declaration could not be read to lift its access " +
   "modifiers for the build";

/**
 * The edits {@link buildCopyText} makes to `text`: the plan's given-reading
 * terms (P2) and its current-time terms that hold an upper bound (Rolling
 * windows), its own or inherited, replaced by `true`; with `parent`, the parent's declaration
 * re-rooted on its build source (P10); and with `liftAccess`, the plan's own
 * declaration's access modifiers lifted and the declaration wrapped in
 * `include { public: * }` (P3), with the flag `include` needs as the copy's
 * first line. The lift edits only the plan's own declaration, so a chained
 * extension's build sees its hidden fields lifted over its parent's re-root
 * exactly as over the raw chain.
 */
export function copyEdits(
   text: string,
   plan: MaterializePlan,
   parent?: MaterializePlan,
   liftAccess = false,
   joinRecords = false,
): { edits: TextEdit[] } | { reason: string } {
   const edits: TextEdit[] = [];
   const given = givenReadTerms(plan);
   if (given.length > 0) {
      const found = termSpans(text, plan, given);
      if ("reason" in found) return found;
      for (const span of found.spans) edits.push({ ...span, insert: "true" });
   }
   for (const term of leftOutWindowTerms(plan.readTimeTerms)) {
      const span = writtenSpan(text, term);
      if (
         span === undefined ||
         edits.some((e) => e.start < span.end && span.start < e.end)
      ) {
         return { reason: WINDOW_NOT_FOUND };
      }
      edits.push({ ...span, insert: "true" });
   }
   if (parent) {
      const range = plan.buildsOver?.declaration.range;
      const from = range ? offsetOf(text, range.start) : -1;
      const to = range ? offsetOf(text, range.end) : -1;
      const rerooted =
         from >= 0 && to >= from
            ? rerootOnBuildSource(text.slice(from, to), parent, {
                 joinRecords,
              })
            : undefined;
      if (
         rerooted === undefined ||
         edits.some((e) => e.start < to && from < e.end)
      ) {
         return { reason: PARENT_NOT_REROOTED };
      }
      edits.push({ start: from, end: to, insert: rerooted });
   }
   if (liftAccess) {
      const from = plan.declaration
         ? offsetOf(text, plan.declaration.range.start)
         : -1;
      const to = plan.declaration
         ? offsetOf(text, plan.declaration.range.end)
         : -1;
      const lifted =
         from >= 0 && to >= from
            ? accessLiftEdits(text.slice(from, to))
            : undefined;
      if (!lifted) return { reason: ACCESS_NOT_LIFTED };
      edits.push(
         ...lifted.map((e) => ({
            start: from + e.start,
            end: from + e.end,
            insert: e.insert,
         })),
         { start: to, end: to, insert: LIFT_INHERITED },
         { start: 0, end: 0, insert: `${ACCESS_FLAG}\n` },
      );
   }
   if (edits.length === 0) return { reason: NOT_FOUND };
   return { edits };
}

const PARENT_NOT_REROOTED =
   "the parent's declaration could not be re-rooted on its build source";

/**
 * A parent's declaration in an extension's build copy (P10): `<name> is
 * <build source>`, then the blocks that undo its renames
 * ({@link inverseRenameBlocks}), then its own trailing `extend` run less each
 * computed dimension its table stores, so the stored column stands in for it,
 * as the served copy re-roots a source on its table. Its measures, joins,
 * views, its fields computed at read and its `where:` are kept as written, so
 * the extension reads every field it read from the parent. With
 * `joinRecords` (the build in a `storage=` destination, {@link storedChainText}),
 * each join_one the parent inlines is replaced by the record of its stored
 * columns ({@link inlinedJoinStatements}), since the destination holds no
 * joined source. Undefined when the declaration does not parse, or a join to
 * replace is declared there by another kind of join.
 */
function rerootOnBuildSource(
   declaration: string,
   parent: MaterializePlan,
   options: { joinRecords?: boolean } = {},
): string | undefined {
   const spans = declarationSpans(declaration);
   if (!spans) return undefined;
   const name = declaration.slice(0, spans.nameEnd).trimEnd();
   const run =
      spans.extendStart === undefined
         ? ""
         : declaration.slice(spans.extendStart).trimEnd();
   const stored = parent.columns.filter((c) => c.join === undefined);
   const kept = dropDimensions(
      run,
      new Set(stored.filter((c) => c.computed).map((c) => c.dimension)),
   );
   if (!kept) return undefined;
   const renames = inverseRenameBlocks(
      kept.text,
      new Set(stored.map((c) => c.column)),
   );
   if (renames === undefined) return undefined;
   let text = kept.text;
   if (options.joinRecords) {
      const records = parentJoinRecords(parent);
      for (const j of records) {
         const dropped = dropJoinDefinition(text, j.join);
         if (dropped === undefined) return undefined;
         text = dropped;
      }
      if (records.length > 0) {
         const opened = intoFirstExtend(
            text,
            inlinedJoinStatements(
               records,
               inlinedJoinRenames(
                  records,
                  parent.columns.map((c) => c.column),
               ),
               true,
            ),
         );
         if (opened === undefined) return undefined;
         text = opened;
      }
   }
   const base = identifier(parent.buildSourceName);
   return `${name} is ${base}${renames}${text ? ` ${text}` : ""}`;
}

/**
 * The join_ones a parent's table stores inline (`inline=`), each with its
 * stored columns and the join's modifier, as {@link inlinedJoinStatements}
 * writes their records. A pair table's join_many is not one: its table is
 * a row per pair, never chained on as the parent's rows.
 */
function parentJoinRecords(parent: MaterializePlan): InlinedJoinText[] {
   const out: InlinedJoinText[] = [];
   for (const join of parent.inline ?? []) {
      if (join === parent.pair?.join) continue;
      const columns = parent.columns.filter((c) => c.join === join);
      if (columns.length === 0) continue;
      const access = columns[0].access;
      out.push({
         join,
         columns: columns.map((c) => ({ name: c.dimension, column: c.column })),
         ...(access ? { access } : {}),
      });
   }
   return out;
}
