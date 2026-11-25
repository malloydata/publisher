// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as Malloy from "@malloydata/malloy-interfaces";
import { parseAnnotation } from "@malloydata/malloy-tag";

/**
 * Extract human-readable documentation from a list of Malloy annotations.
 *
 * Supports both forms a model author can use:
 *   - `#" markdown doc comment` lines (Malloy doc comments)
 *   - `#(doc) "text"` tag-style annotations
 *
 * Multiple matching lines are joined with newlines. Returns undefined when
 * there is no documentation, so callers can `doc ?? fallback`.
 */
export function getDocString(
   annotations: Malloy.Annotation[] | undefined,
): string | undefined {
   const doc = getDocParts(annotations).join("\n").trim();
   return doc || undefined;
}

/** Each doc annotation's text, in order — what `getDocString` joins. */
export function getDocParts(
   annotations: Malloy.Annotation[] | undefined,
): string[] {
   if (!annotations) return [];

   const parts: string[] = [];
   for (const annotation of annotations) {
      const text = annotation.value?.trim();
      if (!text) continue;

      if (text.startsWith('#"')) {
         parts.push(text.slice(2).trim());
      } else if (text.startsWith("#(doc)")) {
         let body = text.slice("#(doc)".length).trim();
         if (body.length >= 2 && body.startsWith('"') && body.endsWith('"')) {
            body = body.slice(1, -1);
         }
         parts.push(body);
      }
   }
   return parts.filter((p) => p.trim() !== "");
}

/**
 * A model file's own doc: its `##"` and `##(doc)` lines (a model's annotations
 * take the double hash), read as `getDocString` reads a source's. Other `##`
 * lines (`##! experimental…`, model tags) are not prose and are left out.
 */
export function getModelDocString(
   annotations: Malloy.Annotation[] | undefined,
): string | undefined {
   const single = (annotations ?? [])
      .map((a) => ({ ...a, value: a.value?.trim() ?? "" }))
      .filter((a) => a.value.startsWith('##"') || a.value.startsWith("##(doc)"))
      .map((a) => ({ ...a, value: a.value.slice(1) }));
   return getDocString(single);
}

/**
 * A source's own doc, without what it inherited. A source that `extend`s another
 * carries the base's doc annotations ahead of its own (the compiled source has no
 * mark saying which are which), so a header printing all of them repeats the base
 * source's description above the one written for this source. `base` is the source
 * this one extends, read from the model's text (sourceLineage's `baseSourceName`);
 * its whole doc, as a leading run of this one's, is what was inherited. Without a
 * known base nothing is dropped: two unrelated sources can share boilerplate. A
 * source with no doc of its own keeps the inherited one — it still describes it.
 */
export function ownSourceDoc(
   source: { docParts?: string[] },
   base: { docParts?: string[] } | undefined,
): string | undefined {
   const parts = source.docParts ?? [];
   const inherited = base?.docParts ?? [];
   const isInherited =
      inherited.length > 0 &&
      inherited.length < parts.length &&
      inherited.every((p, i) => p === parts[i]);
   const doc = (isInherited ? parts.slice(inherited.length) : parts)
      .join("\n")
      .trim();
   return doc || undefined;
}

/**
 * The non-doc annotation lines — renderer/behavior tags like `# bar_chart`,
 * `# size=lg`, or `#(index)` — normalized for chip display: the `#` prefix
 * (and any `#(malloy)`-style parens wrapper) stripped, whitespace trimmed.
 * Doc lines (`#"`, `#(doc)`) are excluded (they're `getDocString`'s). Returns
 * [] when there are none.
 */
export function getTagLines(
   annotations: Malloy.Annotation[] | undefined,
): string[] {
   if (!annotations) return [];
   const tags: string[] = [];
   for (const annotation of annotations) {
      const text = annotation.value?.trim();
      if (!text) continue;
      if (text.startsWith('#"') || text.startsWith("#(doc)")) continue;
      const stripped = text.replace(/^#+@?\s*/, "").trim();
      // Namespaced tags — `#(malloy) drill_filters=…`, `#(index)` — are
      // compiler/system metadata, not author-facing renderer tags; they read
      // as noise on a chip, so only plain `# bar_chart`-style tags surface.
      if (stripped && !stripped.startsWith("(")) tags.push(stripped);
   }
   return tags;
}

/**
 * What a view's tag lines (getTagLines) make it, as names for its row: the chart
 * type and any other bare tag (`line_chart`, `dashboard`, `percent`), each once.
 * A tag's parameters (`{ y=checklist_count }`) and valued settings (`size=lg`)
 * are left to the view's code, where they are read in full (except `viz=bar`, the
 * chart type's other spelling, which names `bar`): on the row they
 * crowded its name off the line. A removed tag (`-bar_chart`) names nothing.
 *
 * Parsed by Malloy's own tag parser; a line it can't parse names its first word,
 * cut at any `{` or `=`.
 */
export function tagNames(tagLines: string[]): string[] {
   const names = new Set<string>();
   for (const line of tagLines) {
      const { tag, log } = parseAnnotation([`# ${line}`]);
      if (log.length > 0) {
         const first = /^[A-Za-z_][\w.]*/.exec(line.trim())?.[0];
         if (first && !/^\s*[\w.]+\s*=/.test(line)) names.add(first);
         continue;
      }
      for (const [name, property] of tag.entries()) {
         if (property.deleted) continue;
         if (property.eq === undefined) names.add(name);
         // `# viz=bar { … }` is `# bar_chart` spelled with a value: name the chart.
         else if (name === "viz" && typeof property.eq === "string")
            names.add(property.eq);
      }
   }
   return [...names];
}

/**
 * Whether a source's annotations carry a Malloy Persistence `persist` tag
 * (`#@ persist …`) — the marker that this source is served from a
 * materialized table rather than computed live.
 */
export function hasPersistTag(
   annotations: Malloy.Annotation[] | undefined,
): boolean {
   return hasPersistenceTag(annotations, "persist");
}

/**
 * Whether a measure's annotations carry a Malloy Persistence `preaggregate` tag
 * (`#@ preaggregate grain="…"`) — the marker that this measure is rolled up
 * into a table grouped at that grain.
 *
 * <p>The measure-level counterpart to {@link hasPersistTag}. Like it, this
 * reports what the model DECLARES, which is true whether or not a rollup has
 * been built — the built artifact is a binding, and the two are shown
 * differently on purpose (see {@link import("./DeclarationPill").DeclarationPill}).
 */
export function hasPreaggregateTag(
   annotations: Malloy.Annotation[] | undefined,
): boolean {
   return hasPersistenceTag(annotations, "preaggregate");
}

/** Shared matcher for the `#@ <verb>` persistence annotations. */
function hasPersistenceTag(
   annotations: Malloy.Annotation[] | undefined,
   verb: string,
): boolean {
   const re = new RegExp(`^#@?\\s*${verb}\\b`);
   return (annotations ?? []).some((a) => re.test(a.value?.trim() ?? ""));
}

/**
 * A field's type as a reader needs it, with the detail the kind alone drops:
 * a time field's timeframe ("date · month"), a number's subtype
 * ("number · integer"), an array's element type ("array<string>"), a record's
 * field count, a native column's SQL type.
 */
export function atomicTypeDetail(
   type: Malloy.AtomicType | undefined,
): string | undefined {
   const kind = atomicTypeLabel(type);
   if (!type || !kind) return kind;
   switch (type.kind) {
      case "number_type":
         return type.subtype ? `${kind} · ${type.subtype}` : kind;
      case "date_type":
      case "timestamp_type":
      case "timestamptz_type":
         return type.timeframe ? `${kind} · ${type.timeframe}` : kind;
      case "sql_native_type":
         return type.sql_type ? `sql native · ${type.sql_type}` : "sql native";
      case "array_type":
         return `array<${atomicTypeDetail(type.element_type) ?? "unknown"}>`;
      case "record_type": {
         const n = type.fields.length;
         return `record · ${n} ${n === 1 ? "field" : "fields"}`;
      }
      default:
         return kind;
   }
}

/** A record's fields, one per line ("name string"), for its type chip's tooltip;
 *  undefined for any other type. */
export function recordFieldsTip(
   type: Malloy.AtomicType | undefined,
): string | undefined {
   if (type?.kind !== "record_type") return undefined;
   return type.fields
      .map((f) => `${f.name} ${atomicTypeDetail(f.type) ?? ""}`.trim())
      .join("\n");
}

/**
 * A field's author-facing tags (`# label="Revenue"`, `# currency`, `# percent`,
 * `# number="0.0%"`) as `getTagLines` reads them, less the persistence
 * tags (`#@ preaggregate …`), which the row states as its own pill.
 */
export function getFieldTagLines(
   annotations: Malloy.Annotation[] | undefined,
): string[] {
   const persistence = (annotations ?? []).filter((a) =>
      /^#@/.test(a.value?.trim() ?? ""),
   );
   return getTagLines(
      (annotations ?? []).filter((a) => !persistence.includes(a)),
   );
}

/** Strip the "_type" suffix off an AtomicType kind ("string_type" → "string"). */
export function atomicTypeLabel(
   type: Malloy.AtomicType | undefined,
): string | undefined {
   return type?.kind?.replace(/_type$/, "");
}
