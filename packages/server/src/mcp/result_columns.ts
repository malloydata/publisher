// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// `_columns` on an execute_query answer: what each column IS, read once from
// the result's schema so an agent need not decode `_meta.schema` and its
// annotation strings to learn it. Flat rows lose it on the way to JSON: a date
// is a string, a bigint may be a string or a number, a revenue is a bare
// number with nothing saying it is money, and a timestamp is UTC text with
// nothing saying which zone it should be read in.

import { Tag } from "@malloydata/malloy-tag";
import type * as Malloy from "@malloydata/malloy-interfaces";
import {
   hasEnvReference,
   motlyTag,
   parseBounded,
   tagText,
} from "../service/motly";

/** One column of a result, as `_columns` lists it. */
export interface ResultColumn {
   name: string;
   /**
    * `string`, `number`, `boolean`, `date`, `timestamp`, `timestamptz`,
    * `json`, `sql_native`, `array` or `record`. A nest is an `array` of
    * records, with `fields`.
    */
   type: string;
   /** Numbers: `integer`, `decimal` or `bigint`. A bigint past 2^53 is a string in `rows`. */
   subtype?: string;
   /**
    * A truncated date or timestamp: its grain (`month`). The value in `rows`
    * is still a full date or timestamp, at the start of that period.
    */
   timeframe?: string;
   /** The label the model or query gives the column (`# label="Revenue"`). */
   label?: string;
   /** True for an aggregate (`aggregate:`, a measure, a calculation); absent for a grouping or a nest. */
   measure?: true;
   /**
    * The display format the model or query declares, as its tag spells it:
    * `currency`, `currency = usd0k`, `percent`, `number = "#,##0.0"`,
    * `duration = minutes`. Absent when it declares none, which means show the
    * number as it is.
    */
   format?: string;
   /**
    * Timestamps only: the zone the value's wall clock reads in, from the
    * query's `timezone:` or a nest's own. Absent means UTC. `rows` carry the
    * instant as UTC either way.
    */
   timezone?: string;
   /** A record's fields, or the fields of each record in an array of records (a nest). */
   fields?: ResultColumn[];
   /** An array of scalars: the element's type. */
   element?: Omit<ResultColumn, "name">;
}

/**
 * The tag properties that set how one value displays: the renderer's cell
 * formats, in the order it checks them. The renderer applies the first one a
 * tag has, so a column tagged both `# percent` and `# currency` shows as
 * currency.
 */
const FORMAT_PROPERTIES = [
   "currency",
   "percent",
   "duration",
   "number",
] as const;

type Annotations = Malloy.Annotation[] | undefined;

const lines = (annotations: Annotations): string[] =>
   (annotations ?? []).map((a) => a.value);

/**
 * The `#(malloy)` tag Malloy writes on a result field (`calculation`, a nest's
 * `query_timezone`). A line carrying `@env.` is dropped before the parse, as
 * the MOTLY path drops it: the parser would fill it from the server's own
 * environment, and the value would reach the caller as a `timezone`.
 */
function malloyTag(annotations: Annotations): Tag | undefined {
   const own = lines(annotations).filter(
      (l) => l.startsWith("#(malloy)") && !hasEnvReference(l),
   );
   if (own.length === 0) return undefined;
   return parseBounded(own).tag;
}

/**
 * The display format the renderer applies, re-serialized from the parsed tag
 * so it reads exactly as the renderer reads it. Only that one format property:
 * a `label=`, a chart tag, or a second format the renderer ignores is not
 * included.
 */
function formatOf(tag: Tag | undefined): string | undefined {
   if (!tag) return undefined;
   const p = FORMAT_PROPERTIES.find((name) => tag.has(name));
   if (!p) return undefined;
   const properties = { [p]: tag.dict[p] };
   try {
      return new Tag({ properties } as ConstructorParameters<typeof Tag>[0])
         .toString()
         .replace(/^#\s*/, "")
         .trim();
   } catch {
      return undefined;
   }
}

function typeOf(
   t: Malloy.AtomicType,
   timezone: string | undefined,
): Omit<ResultColumn, "name"> {
   switch (t.kind) {
      case "number_type":
         return t.subtype
            ? { type: "number", subtype: t.subtype }
            : { type: "number" };
      case "date_type":
         return t.timeframe
            ? { type: "date", timeframe: t.timeframe }
            : { type: "date" };
      case "timestamp_type":
      case "timestamptz_type": {
         // The zone is set here, not in columnOf, so an array's timestamp
         // elements carry it too.
         const type = t.kind.replace(/_type$/, "");
         return {
            type,
            ...(t.timeframe ? { timeframe: t.timeframe } : {}),
            ...(timezone && timezone !== "UTC" ? { timezone } : {}),
         };
      }
      case "record_type":
         return {
            type: "record",
            fields: t.fields.map((f) =>
               columnOf(f.name, f.type, f.annotations, timezone),
            ),
         };
      case "array_type": {
         const element = typeOf(t.element_type, timezone);
         return element.type === "record"
            ? { type: "array", fields: element.fields }
            : { type: "array", element };
      }
      default:
         return { type: t.kind.replace(/_type$/, "") };
   }
}

function columnOf(
   name: string,
   t: Malloy.AtomicType,
   annotations: Annotations,
   timezone: string | undefined,
   measure = false,
): ResultColumn {
   const internal = malloyTag(annotations);
   const nested = t.kind === "record_type" || t.kind === "array_type";
   // A nest with its own `timezone:` carries it as `#(malloy) query_timezone`;
   // its fields read in that zone, else the one they inherit. The renderer's
   // rule.
   const zone = nested
      ? (tagText(internal, "query_timezone") ?? timezone)
      : timezone;
   const tag = motlyTag(lines(annotations));
   const label = tagText(tag, "label");
   const format = formatOf(tag);
   const { fields, element, timezone: ownZone, ...type } = typeOf(t, zone);
   // Keys in the order the interface documents them, nested fields last.
   return {
      name,
      ...type,
      ...(label ? { label } : {}),
      ...(measure || internal?.has("calculation") ? { measure: true } : {}),
      ...(format ? { format } : {}),
      ...(ownZone ? { timezone: ownZone } : {}),
      ...(element ? { element } : {}),
      ...(fields ? { fields } : {}),
   };
}

/**
 * The columns of a result, in output order: each one's type, grain, label,
 * declared format, and the zone a timestamp reads in, read from the schema
 * alone. A column that fails to read is listed with its name and type only,
 * rather than failing the answer.
 */
export function resultColumns(result: Malloy.Result): ResultColumn[] {
   const columns: ResultColumn[] = [];
   for (const field of result.schema?.fields ?? []) {
      if (
         field.kind !== "dimension" &&
         field.kind !== "measure" &&
         field.kind !== "calculate"
      ) {
         continue;
      }
      try {
         columns.push(
            columnOf(
               field.name,
               field.type,
               field.annotations,
               result.query_timezone,
               field.kind !== "dimension",
            ),
         );
      } catch {
         const kind = (field.type as { kind?: unknown } | undefined)?.kind;
         columns.push({
            name: field.name,
            type:
               typeof kind === "string"
                  ? kind.replace(/_type$/, "")
                  : "unknown",
         });
      }
   }
   return columns;
}
