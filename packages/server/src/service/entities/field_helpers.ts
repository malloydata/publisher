// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Entity, JoinSourceMetadata } from "./internal_types";
import { isOnRoute } from "../annotation_reader";
import { AccessModifierEnum, EntityTypeEnum } from "./internal_types";
import { extractCodeFromEntity } from "./model_text";
import { motlyTag } from "../motly";
import type {
   AtomicField,
   Explore,
   Field,
   URLReader,
} from "@malloydata/malloy";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

export class FileURLReader implements URLReader {
   async readURL(url: URL): Promise<string> {
      let path = url.toString();
      if (url.protocol === "file:") {
         path = fileURLToPath(url);
      }
      const content = await readFile(path, "utf-8");
      return content;
   }
}

/**
 * Returns the access modifier for a field (public, private, or internal).
 *
 * Malloy stores `accessModifier` on a different property of the Field
 * instance depending on its runtime subclass:
 *   - AtomicField  (dimension / measure / calculate) -> fieldTypeDef.accessModifier
 *   - QueryField   (view / turtle)                   -> turtleDef.accessModifier
 *   - ExploreField (join)                            -> _structDef.accessModifier
 * Reading only `fieldTypeDef` silently treats every non-public view and
 * join as Public, which leaks private / internal joins (and the nested
 * sources they wrap) into both the entity index and source summaries.
 */
export function getAccessModifier(field: Field): AccessModifierEnum {
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const f = field as any;
   const accessModifier: string | undefined =
      f.fieldTypeDef?.accessModifier ??
      f.turtleDef?.accessModifier ??
      f._structDef?.accessModifier;
   if (accessModifier === undefined || accessModifier === "public") {
      return AccessModifierEnum.Public;
   }
   if (accessModifier === "private") {
      return AccessModifierEnum.Private;
   }
   if (accessModifier === "internal") {
      return AccessModifierEnum.Internal;
   }
   return AccessModifierEnum.Public;
}

// A path, URL or glob, or a name ending in a file extension DuckDB reads.
const FILE_TABLE_PATH =
   /[/\\*?]|:\/\/|\.(?:csv|tsv|txt|json|jsonl|ndjson|parquet|arrow|xlsx|gz|zst)$/i;

/** Whether a table path names a file rather than a dotted `catalog.schema.table`. */
export function isFilePath(tablePath: string): boolean {
   return FILE_TABLE_PATH.test(tablePath);
}

/**
 * Returns the Entity to be indexed in the Malloy index.
 */
export function getEntityFromMalloyField(
   field: Field,
   source: string,
   docString: string,
   connection: string | null,
   tablePath: string | null,
   joinSourceMetadata: JoinSourceMetadata | null = null,
   nameOverride: string | null = null,
   dialect: string | null = null,
): Entity {
   // For a leaf field reached through a nested-index join chain, the caller
   // passes a join-qualified path (e.g. "people.primaryName") here while
   // `source` is set to the top-level runnable source (e.g. "principals"), so
   // the stored (source, name) pair is a runnable `run: <source> -> { ...
   // <name> }` and matches the (sourceName, dimension) form a caller queries
   // by. Plain fields pass null and keep `field.name`.
   const name = nameOverride ?? field.name;
   const field_type = inferType(field, docString);
   const field_data_type = inferFieldDataType(field);
   const code = extractCodeFromEntity(field, docString);
   const tags = getTags(field);
   const description = getDescriptionFromTags(tags);
   let catalog_name: string | null = null;
   let schema_name: string | null = null;
   let table_name: string | null = null;
   if (tablePath && dialect === "duckdb" && isFilePath(tablePath)) {
      // A file DuckDB reads (`duckdb.table('orders.csv')`) is not a dotted
      // catalog.schema.table name: report it whole. Only on DuckDB: a
      // warehouse name may carry `*` (a BigQuery wildcard table) or a suffix
      // that looks like a file extension.
      table_name = tablePath;
   } else if (tablePath) {
      const parts = tablePath.split(".");
      table_name = parts[parts.length - 1]!;
      if (parts.length > 1) {
         schema_name = parts[parts.length - 2]!;
      }
      if (parts.length > 2) {
         catalog_name = parts[parts.length - 3]!;
         if (catalog_name.startsWith("{{")) {
            // This is just a template Malloy is using, not a real catalog
            catalog_name = null;
         }
      }
   }
   const table_info = {
      connection_name: connection,
      catalog_name,
      schema_name,
      table_name,
   };
   return {
      name,
      source,
      field_type,
      field_data_type,
      code,
      tags,
      description,
      table_info,
      join_source_metadata: joinSourceMetadata ?? undefined,
   };
}

/**
 * Infers the type of the field based on the code.
 */
export function inferType(field: Field, docString: string): string {
   if (!isFieldHidden(field)) {
      if (isFieldAggregate(field)) {
         return EntityTypeEnum.Measure;
      } else if (field.isExploreField()) {
         return EntityTypeEnum.Join;
      } else if (fieldType(field) === "query") {
         return EntityTypeEnum.View;
      } else {
         if (!isMalloyDefined(field, docString)) {
            return EntityTypeEnum.Column;
         } else {
            return EntityTypeEnum.Dimension;
         }
      }
   }
   return EntityTypeEnum.Unknown;
}

/**
 * Infers the data type of the field based on the field type.
 */
export function inferFieldDataType(field: Field): string | null {
   try {
      return (field as AtomicField).type.toString();
   } catch {
      return null;
   }
}

// Block-annotation forms (`#|(doc)\n…`) are emitted by Malloy as a single note
// whose first line is the prefix and whose dedented body follows, joined with
// `\n`. Line forms (`#(doc) …`) carry the body inline after the prefix. We
// accept both.
const DOC_BLOCK_PREFIXES = ["#|(doc)"] as const;
const DOC_LINE_PREFIXES = ["#(doc)"] as const;

// Normalize line endings to `\n`: CRLF (Windows-authored files) and lone CRs
// both collapse to `\n` so the stored description has clean newlines. (A lone
// mid-line CR is rejected by the Malloy lexer upstream, so this mainly handles
// the CRLF case.)
const normalizeNewlines = (s: string): string => s.replace(/\r\n?/g, "\n");

/**
 * Parses a single Malloy annotation/note into its documentation body, or null
 * if the annotation isn't a doc annotation. Internal newlines in the block-form
 * body are preserved (callers that embed the body into a line-oriented format
 * must flatten them); only outer whitespace is trimmed.
 */
export function parseDocAnnotation(text: string | undefined): string | null {
   // On the `doc` route as Publisher reads it, not by prefix: `startsWith`
   // took `#(docs) …` for a doc.
   if (!text || !isOnRoute(text, "doc")) return null;
   const trimmed = text.trim();
   for (const prefix of DOC_BLOCK_PREFIXES) {
      if (trimmed === prefix) return "";
      if (trimmed.startsWith(prefix)) {
         // The body is the lines after the prefix line, plus any text the
         // author put on the prefix line itself (`#|(doc) First line\n…`) —
         // the compiler emits that inline text as part of the same tagline,
         // so it must be kept as the first body line, not discarded.
         const afterPrefix = normalizeNewlines(trimmed.slice(prefix.length));
         const firstNewline = afterPrefix.indexOf("\n");
         if (firstNewline < 0) {
            return afterPrefix.trim();
         }
         const inline = afterPrefix.slice(0, firstNewline).trim();
         const rest = afterPrefix.slice(firstNewline + 1);
         return (inline ? `${inline}\n${rest}` : rest).trim();
      }
   }
   for (const prefix of DOC_LINE_PREFIXES) {
      if (trimmed.startsWith(prefix)) {
         return normalizeNewlines(trimmed.slice(prefix.length)).trim();
      }
   }
   return null;
}

interface MalloyNote {
   text?: string;
   at?: {
      url?: string;
      range?: {
         start?: { line?: number };
         end?: { line?: number };
      };
   };
}

interface MalloyDocLocation {
   url?: string;
   range?: {
      start?: { line?: number };
      end?: { line?: number };
   };
}

/**
 * Returns taglines that are *genuinely declared on* this explore.
 *
 * Malloy's annotation model is subtle when a source extends another, and
 * neither `Explore.getTaglines()` nor a naive read of
 * `structDef.annotation.blockNotes` is correct on its own:
 *
 *   - If the extending source adds its OWN annotation, the base's
 *     annotation is tucked under `annotation.inherits` and the source's
 *     own notes live in `blockNotes`/`notes`. Walking `inherits` (as
 *     `getTaglines()` does) would wrongly surface the base's tags.
 *   - If the extending source adds NO annotation of its own, Malloy does
 *     NOT create an `inherits` wrapper: it copies the base's annotation
 *     onto the extending source verbatim, so the base's `blockNotes`
 *     appear as top-level notes on the extending source. Reading
 *     `blockNotes` blindly would therefore wrongly treat the base's
 *     `#(agent-hidden)` as if it were declared on the extending source
 *     (e.g. `source: a is hidden_base extend {}` or `source: a is
 *     hidden_base`).
 *
 * The only reliable local signal is *location*: a note genuinely declared
 * on this source sits within the source's declaration or immediately above
 * the `source:` keyword (block annotations). A note copied from a base
 * points back at the base's declaration, which lives elsewhere. We keep
 * only the notes whose source location is contiguous with this explore's
 * declaration. We use contiguity (rather than a raw line-gap threshold) so
 * that a base defined immediately above its extending child is still
 * correctly excluded.
 */
export const getDirectTaglines = (explore: Explore): string[] => {
   const annotation = (
      explore.structDef as
         | { annotations?: { blockNotes?: MalloyNote[]; notes?: MalloyNote[] } }
         | undefined
   )?.annotations;
   if (!annotation) {
      return [];
   }
   const notes: MalloyNote[] = [
      ...(annotation.blockNotes ?? []),
      ...(annotation.notes ?? []),
   ];

   const exploreLoc = (explore as { location?: MalloyDocLocation }).location;
   const directNotes = filterDirectlyDeclaredNotes(notes, exploreLoc);

   return directNotes
      .map((n) => n.text)
      .filter((t): t is string => typeof t === "string");
};

/**
 * Returns ALL taglines Malloy attached to a NAMED join field's `structDef`
 * (`join_one:`/`join_many: alias is sourceName on ...`, no `extend`/direct-table).
 *
 * For a named join, Malloy does not build a new struct via `extend` — the
 * join field's `structDef` IS the joined source's own struct, copied
 * verbatim, annotations included. So a note's `at.url` there points at the
 * joined source's OWN declaration file (not the file with the join clause),
 * and its line range is that file's — never contiguous with the join
 * field's `location` (which sits at the join clause instead). Both of the
 * `filterDirectlyDeclaredNotes` heuristics (same-file, line-contiguity) are
 * therefore built for the `extend`-inheritance case and wrongly drop every
 * note here — see `getDirectTaglines` for that rationale. Since there is no
 * extend/inherits ambiguity to resolve for a named join, every note present
 * IS the joined source's own gate; skip the filter entirely.
 */
export const getRawTaglines = (explore: Explore): string[] => {
   const annotation = (
      explore.structDef as
         | { annotations?: { blockNotes?: MalloyNote[]; notes?: MalloyNote[] } }
         | undefined
   )?.annotations;
   if (!annotation) {
      return [];
   }
   const notes: MalloyNote[] = [
      ...(annotation.blockNotes ?? []),
      ...(annotation.notes ?? []),
   ];
   return notes
      .map((n) => n.text)
      .filter((t): t is string => typeof t === "string");
};

const noteLineRange = (
   note: MalloyNote,
): { start: number; end: number } | null => {
   const range = note.at?.range;
   const start = range?.start?.line;
   const end = range?.end?.line ?? start;
   if (typeof start !== "number" || typeof end !== "number") {
      return null;
   }
   return { start, end };
};

/**
 * Keeps only the notes that are genuinely declared on the explore at
 * `exploreLoc` — i.e. notes located inside the source declaration or in the
 * contiguous block of annotation lines immediately above it. Notes that were
 * copied verbatim from an extended base (whose location points elsewhere)
 * are dropped. See `getDirectTaglines` for the full rationale.
 */
const filterDirectlyDeclaredNotes = (
   notes: MalloyNote[],
   exploreLoc: MalloyDocLocation | undefined,
): MalloyNote[] => {
   const srcStart = exploreLoc?.range?.start?.line;
   const srcEnd = exploreLoc?.range?.end?.line ?? srcStart;
   // Without a usable source location we cannot distinguish direct from
   // inherited notes; preserve the historical behavior and keep them all.
   if (typeof srcStart !== "number" || typeof srcEnd !== "number") {
      return notes;
   }
   const exploreUrl = exploreLoc?.url;

   // Notes from a different file are always inherited from a base.
   const sameFileNotes = notes.filter((n) => {
      const noteUrl = n.at?.url;
      return !noteUrl || !exploreUrl || noteUrl === exploreUrl;
   });

   // Map each line to the notes that occupy it, so we can walk upward from
   // the `source:` keyword while every intervening line is an annotation.
   const lineToNotes = new Map<number, MalloyNote[]>();
   const direct = new Set<MalloyNote>();
   for (const note of sameFileNotes) {
      const lr = noteLineRange(note);
      if (!lr) {
         // A note without a location can't be proven inherited; keep it.
         direct.add(note);
         continue;
      }
      // Notes overlapping the source declaration itself are direct.
      if (lr.start <= srcEnd && lr.end >= srcStart) {
         direct.add(note);
      }
      for (let line = lr.start; line <= lr.end; line++) {
         const existing = lineToNotes.get(line);
         if (existing) {
            existing.push(note);
         } else {
            lineToNotes.set(line, [note]);
         }
      }
   }

   // Walk up the contiguous block of annotation lines directly above the
   // source. Stop as soon as a line isn't covered by an annotation note.
   let line = srcStart - 1;
   while (lineToNotes.has(line)) {
      const lineNotes = lineToNotes.get(line)!;
      let topmost = line;
      for (const note of lineNotes) {
         direct.add(note);
         const lr = noteLineRange(note);
         if (lr && lr.start < topmost) {
            topmost = lr.start;
         }
      }
      line = topmost - 1;
   }

   // Preserve original ordering.
   return notes.filter((n) => direct.has(n));
};

/**
 * Builds a field's description from its taglines. Every doc annotation
 * contributes a line: a single `#|(doc)` block carries its own newlines, while
 * the legacy form repeated on consecutive lines (`#(doc) a` / `#(doc) b`)
 * arrives as separate taglines that we join with `\n`. Both forms therefore
 * produce the same multi-line description.
 */
export function getDescriptionFromTags(tags: string[]): string | undefined {
   const parts = tags
      .map((tag) => parseDocAnnotation(tag))
      .filter((doc): doc is string => !!doc && doc.length > 0);
   return parts.length > 0 ? parts.join("\n") : undefined;
}

export function isFieldHidden(field: Field): boolean {
   const { name, parentExplore } = field;
   // Through motlyTag, which drops a line that reads the server's environment:
   // `tagParse()` would hydrate `# hidden.pattern=@env.X` into a RegExp, and
   // which fields come back hidden would then tell a caller about the secret.
   const tag = motlyTag(parentExplore.getTaglines() ?? []);
   if (!tag) return false;
   const strings =
      tag
         .array("hidden")
         ?.map((tag) => tag.text())
         .filter(isStringTag) || [];

   const patternText = tag.text("hidden", "pattern");
   const pattern = patternText ? new RegExp(patternText) : undefined;

   return !!(pattern?.test(name) || strings.includes(name));
}

const isStringTag = (tag: string | undefined): tag is string =>
   typeof tag === "string";

function isFieldAggregate(field: Field) {
   return field.isAtomicField() && field.isCalculation();
}

export function fieldType(field: Field) {
   return field.isAtomicField() ? field.type.toString() : "query";
}

/**
 * Returns true if the field is defined in the document.
 */
export function isMalloyDefined(field: Field, docString: string): boolean {
   const code = extractCodeFromEntity(field, docString);
   // Remove any tags from the code
   const lines = code.split("\n");
   const keep: string[] = [];
   for (const line of lines) {
      if (!line.trim().startsWith("#")) {
         keep.push(line);
      }
   }
   // Strip backticks so quoted identifiers like `Major Recall` match the
   // same regex as plain identifiers — field.name has backticks stripped
   // by the compiler, but the source code keeps them.
   const newCode = keep.join("\n").replace(/`/g, "");
   const name = field.name;
   const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
   const regex = new RegExp(`${escaped}\\s+is\\b`);
   return regex.test(newCode);
}

function getTags(field: Field): string[] {
   const tags = field.getTaglines() || []; // Default to an empty array if null/undefined
   if (tags.length > 0) {
      return tags.map((tag) => tag.trim()).filter((tag) => tag !== "");
   }
   return [];
}
