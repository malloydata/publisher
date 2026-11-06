// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as Malloy from "@malloydata/malloy-interfaces";
import { isFieldIndexed } from "./modelGivens";
import {
   atomicTypeDetail,
   getFieldTagLines,
   recordFieldsTip,
   getDocParts,
   getDocString,
   getTagLines,
   hasPersistTag,
   hasPreaggregateTag,
} from "./docAnnotations";
import { AccentName, sourceAccentName } from "./tokens";
import type { ModelNamedQuery } from "./useModelSources";

/**
 * Render-ready view model for the model viewer, flattened from the compiled
 * `Malloy.SourceInfo` metadata.
 *
 * SourceInfo doesn't expose everything the design mock shows — table name,
 * grain, primary key, measure expressions, join `on` conditions, and view
 * group-by/aggregate breakdowns aren't in the compiled metadata, so the
 * explorer renders only name / type / docstring / join relationship. If the
 * model API grows those fields, extend these types rather than re-deriving
 * in components.
 */

export interface ExplorerField {
   name: string;
   kind: "dimension" | "measure" | "calculate" | "view";
   /** Type with its detail, e.g. "string", "number · integer", "date · month",
    *  "array<string>": dimensions, measures and calculations only. */
   dataType?: string;
   /** A record type's fields, one per line, for the type chip's tooltip. */
   typeTip?: string;
   doc?: string;
   /** True when the dimension carries `#(index)` / `#(index_values)` —
    *  its values can be fetched from the dimensional index. */
   indexed?: boolean;
   /** True when the measure carries `#@ preaggregate` — it is DECLARED to roll
    *  up into a table at some grain. Says nothing about whether one has been
    *  built; that is the measure's binding. */
   preaggregate?: boolean;
   /** Author-facing tags from the non-doc annotation lines: a view's renderer
    *  tags (`bar_chart`, `size=lg`), a field's display tags (`label="Revenue"`,
    *  `currency`, `percent`). */
   tags?: string[];
}

/**
 * A model-level named query (`query: name is …`). Model-scoped, not a field of
 * any source — so it is listed beside the sources in the rail, not inside one.
 * The compiled metadata carries its name and annotations only (no definition:
 * `modelInfo.entries` never includes queries, and a host's entities have no
 * query kind), so a query is something to RUN rather than something to read.
 */
export interface ExplorerQuery {
   name: string;
   /** The source the query runs against, when the publisher reports it — the
    *  source whose required filters gate a run. */
   sourceName?: string;
   doc?: string;
   /** Renderer/behavior tags (`bar_chart`, …), from the non-doc annotation lines. */
   tags?: string[];
}

export interface ExplorerJoin {
   name: string;
   relationship: Malloy.Relationship;
   doc?: string;
   dimensions: ExplorerField[];
   measures: ExplorerField[];
}

export interface ExplorerSource {
   name: string;
   doc?: string;
   /** Each doc annotation, in order, inherited ones first (see `ownSourceDoc`). */
   docParts?: string[];
   accent: AccentName;
   /** The source carries a `#@ persist` tag — served from a materialized
    *  table rather than computed live. */
   persist?: boolean;
   dimensions: ExplorerField[];
   measures: ExplorerField[];
   /** `calculate:` fields. */
   calculations: ExplorerField[];
   views: ExplorerField[];
   joins: ExplorerJoin[];
}

function toField(
   field:
      | Malloy.FieldInfoWithDimension
      | Malloy.FieldInfoWithMeasure
      | Malloy.FieldInfoWithCalculate,
): ExplorerField {
   return {
      name: field.name,
      kind: field.kind,
      dataType: atomicTypeDetail(field.type),
      typeTip: recordFieldsTip(field.type),
      tags: getFieldTagLines(field.annotations),
      doc: getDocString(field.annotations),
      indexed: field.kind === "dimension" ? isFieldIndexed(field) : undefined,
      preaggregate:
         field.kind === "measure"
            ? hasPreaggregateTag(field.annotations)
            : undefined,
   };
}

function toJoin(field: Malloy.FieldInfoWithJoin): ExplorerJoin {
   const dimensions: ExplorerField[] = [];
   const measures: ExplorerField[] = [];
   for (const child of field.schema?.fields ?? []) {
      // One level deep is enough for the explorer; nested joins are rare and
      // their fields read better in their own source's entry.
      if (child.kind !== "dimension" && child.kind !== "measure") continue;
      // Join children are indexed under the *joined* source, whose name we
      // can't recover from the join field (the join name may differ). Force
      // indexed=false so the inspector never fetches values against the
      // parent source — a same-named parent dimension would silently return
      // the wrong values. `preaggregate` is cleared for the same reason: the
      // rollup belongs to the joined source, so offering a build from here
      // would scope it to the wrong one.
      const childField = {
         ...toField(child),
         indexed: false,
         preaggregate: false,
      };
      if (child.kind === "dimension") dimensions.push(childField);
      else measures.push(childField);
   }
   return {
      name: field.name,
      relationship: field.relationship,
      doc: getDocString(field.annotations),
      dimensions,
      measures,
   };
}

/** Flatten one compiled source into the explorer view model. */
export function toExplorerSource(
   source: Malloy.SourceInfo,
   index: number,
): ExplorerSource {
   const out: ExplorerSource = {
      name: source.name,
      doc: getDocString(source.annotations),
      docParts: getDocParts(source.annotations),
      accent: sourceAccentName(index),
      persist: hasPersistTag(source.annotations),
      dimensions: [],
      measures: [],
      calculations: [],
      views: [],
      joins: [],
   };
   for (const field of source.schema?.fields ?? []) {
      switch (field.kind) {
         case "dimension":
         case "measure":
            out[field.kind === "dimension" ? "dimensions" : "measures"].push(
               toField(field),
            );
            break;
         case "calculate":
            out.calculations.push(toField(field));
            break;
         case "view":
            out.views.push({
               name: field.name,
               kind: "view",
               doc: getDocString(field.annotations),
               // Views surface their renderer tags (bar_chart, size=lg, …) —
               // how the view presents is part of what it IS.
               tags: getTagLines(field.annotations),
            });
            break;
         case "join":
            out.joins.push(toJoin(field));
            break;
      }
   }
   return out;
}

/**
 * Flatten the model's named queries. The publisher's `CompiledModel.queries`
 * carries annotations as raw lines (not `Malloy.Annotation` objects), so they
 * are wrapped to reuse the one doc/tag parser the fields use.
 */
export function toExplorerQueries(
   queries: ReadonlyArray<ModelNamedQuery> | undefined,
): ExplorerQuery[] {
   const out: ExplorerQuery[] = [];
   for (const q of queries ?? []) {
      if (!q.name) continue;
      const annotations = (q.annotations ?? []).map((value) => ({ value }));
      out.push({
         name: q.name,
         sourceName: q.sourceName || undefined,
         doc: getDocString(annotations),
         tags: getTagLines(annotations),
      });
   }
   return out;
}

/**
 * The fields a source declares: its dimensions, measures, calculations and views.
 *
 * A join's own fields are NOT counted: the pane header's count has to add
 * up to the group counts printed directly beneath it — Dimensions · N, Measures
 * · N, Views · N — and counting fields reachable THROUGH a join made the header
 * read higher than anything visible, with the difference buried inside a
 * collapsed Joins group. A join is counted as a join, not as its fields.
 */
export function fieldCount(source: ExplorerSource): number {
   return (
      source.dimensions.length +
      source.measures.length +
      source.calculations.length +
      source.views.length
   );
}

/**
 * Which source a model opens on when the address names none.
 *
 * Declaration order is not a ranking. `flights.malloy` declares `carriers`
 * first and `flights` last; `imdb.malloy` declares `people` first and `movies`
 * last — so `sources[0]` opened both files on a lookup table while the source
 * the file is about sat at the bottom of the rail.
 *
 * The signal is **views**: a source someone has written views over is one
 * someone means to be read, and the analytically-central source is where the
 * views accumulate. Ties break toward the file's namesake (`flights.malloy` →
 * `flights`), then toward declaration order, so the pick is total and stable.
 *
 * Namesake alone would not do it: `imdb.malloy` has no `imdb` source, and its
 * subject is `movies`. Views get both files right.
 */
export function pickDefaultSource(
   sources: ExplorerSource[],
   modelPath?: string,
): ExplorerSource | null {
   if (sources.length === 0) return null;
   // `-` and `_` are the same character for this purpose: a file named
   // `purchase-events.malloy` declaring `purchase_events` is a namesake match.
   const norm = (v: string) => v.toLowerCase().replace(/[-_]/g, "");
   const namesake = modelPath
      ? norm((modelPath.split("/").pop() ?? modelPath).replace(/\.malloy$/, ""))
      : undefined;

   // A source written as `B is A extend { … }` carries A's views plus its own,
   // so it ALWAYS outranks its own base on a view count — `ga4.malloy` would
   // open on `purchase_events` (3) over `events` (1), the narrow slice over the
   // subject. The schema does not say what a source extends, but a strict
   // superset of another's view names is that relationship's signature, so a
   // source that contains another's views entire is set aside while any
   // candidate that is not stands.
   const viewNames = new Map(
      sources.map((s) => [s.name, new Set(s.views.map((v) => v.name))]),
   );
   const extendsAnother = (s: ExplorerSource) =>
      sources.some((other) => {
         if (other.name === s.name || other.views.length === 0) return false;
         const mine = viewNames.get(s.name)!;
         return (
            other.views.length < s.views.length &&
            [...viewNames.get(other.name)!].every((v) => mine.has(v))
         );
      });
   const base = sources.filter((s) => !extendsAnother(s));
   const pool = base.length > 0 ? base : sources;

   let best = pool[0];
   for (const s of pool.slice(1)) {
      if (s.views.length !== best.views.length) {
         if (s.views.length > best.views.length) best = s;
         continue;
      }
      // Equal views: the namesake wins, and only it — anything else would be
      // declaration order, which `best` already holds.
      if (
         namesake &&
         norm(s.name) === namesake &&
         norm(best.name) !== namesake
      ) {
         best = s;
      }
   }
   return best;
}
