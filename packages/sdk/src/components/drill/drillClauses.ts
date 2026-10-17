// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as Malloy from "@malloydata/malloy-interfaces";
import { escapeMalloyString } from "../../utils/malloyString";

/**
 * The clicked row of a table, as `drill:` clauses.
 *
 * Malloy's `drill:` needs a value for EVERY dimension of the view it drills
 * through, and for every dimension of each enclosing row when the click is
 * inside a nest. Only the renderer knows which row a cell belongs to, so the
 * clauses come from its `onDrill` (`DrillData.stableDrillClauses`); the click
 * itself carries the clicked field alone.
 *
 * Spelled here rather than taken from the renderer's `DrillData.query`, which
 * Malloy 0.0.435 cannot run as written: it puts a string between double
 * quotes without escaping it, it reads every hour as `00`, and `drill:`
 * refuses the quarter, week and minute literals it writes (`@2026-Q1` fails
 * with "cannot contain"). What `drill:` does accept for every truncation is
 * the truncated value itself at full precision, so that is what is written:
 * a date as its day, a timestamp to the second.
 */
export type DrillClause = Malloy.DrillOperation;

/** One clause as the title reads it: `region` is `West`. */
export interface DrillClauseLabel {
   field: string;
   label: string;
}

/**
 * The clauses as `drill:` text, or undefined when any of them cannot be
 * written. Never a subset: dropping a clause is the error this exists to fix.
 *
 * `timezone` is the tile's query timezone. The rows query declares it too,
 * because `drill:` recomputes the view's dimensions in the drilling query's
 * zone, so a timestamp's digits here are its wall clock there.
 */
export function drillClausesText(
   clauses: readonly DrillClause[],
   timezone?: string,
): string | undefined {
   if (clauses.length === 0) return undefined;
   const parts: string[] = [];
   for (const { filter } of clauses) {
      if (filter.kind !== "literal_equality") return undefined;
      const field = fieldText(filter.expression);
      const value = literalText(filter.value, timezone);
      if (field === undefined || value === undefined) return undefined;
      parts.push(`${field} = ${value}`);
   }
   return parts.join(", ");
}

/**
 * The dimensions whose value in this row is null. `drill:` has no spelling
 * for one in Malloy 0.0.435: `= null` asks for `is null`, which it then
 * refuses, as it does a filter.
 */
export function nullDimensions(clauses: readonly DrillClause[]): string[] {
   return clauses.flatMap(({ filter }) =>
      filter.kind === "literal_equality" &&
      filter.value.kind === "null_literal" &&
      filter.expression.kind === "field_reference"
         ? [filter.expression.name]
         : [],
   );
}

/** The clauses as the dialog title names them, each value as the table showed it. */
export function drillClauseLabels(
   clauses: readonly DrillClause[],
   timezone?: string,
): DrillClauseLabel[] {
   return clauses.flatMap(({ filter }) => {
      if (
         filter.kind !== "literal_equality" ||
         filter.expression.kind !== "field_reference"
      )
         return [];
      const label = literalLabel(filter.value, timezone);
      return label === undefined
         ? []
         : [{ field: filter.expression.name, label }];
   });
}

function fieldText(expression: Malloy.Expression): string | undefined {
   if (expression.kind !== "field_reference") return undefined;
   return [...(expression.path ?? []), expression.name]
      .map(identifier)
      .join(".");
}

function identifier(name: string): string {
   return /^[A-Za-z_][A-Za-z_0-9]*$/.test(name)
      ? name
      : `\`${name.replace(/\\/g, "\\\\").replace(/`/g, "\\`")}\``;
}

/** A literal as `drill:` accepts it. Undefined for a null, which it does not. */
export function literalText(
   value: Malloy.LiteralValue,
   timezone?: string,
): string | undefined {
   switch (value.kind) {
      case "string_literal":
         return `'${escapeMalloyString(value.string_value)}'`;
      case "number_literal":
         return Number.isFinite(value.number_value)
            ? String(value.number_value)
            : undefined;
      case "boolean_literal":
         return String(value.boolean_value);
      case "date_literal": {
         const day = dateDigits(value.date_value);
         return day && `@${day[0]}-${day[1]}-${day[2]}`;
      }
      case "timestamp_literal": {
         const t = wallClock(value.timestamp_value, timezone);
         return t && `@${t[0]}-${t[1]}-${t[2]} ${t[3]}:${t[4]}:${t[5]}`;
      }
      default:
         return undefined;
   }
}

/** A value as the table printed it: a quarter as `2026-Q1`, an hour as `2026-02-03 23`. */
function literalLabel(
   value: Malloy.LiteralValue,
   timezone?: string,
): string | undefined {
   switch (value.kind) {
      case "string_literal":
         return value.string_value;
      case "null_literal":
         return "null";
      case "date_literal": {
         const day = dateDigits(value.date_value);
         return (
            day &&
            granularLabel(
               [...day, "00", "00", "00"],
               value.granularity ?? "day",
            )
         );
      }
      case "timestamp_literal": {
         const t = wallClock(value.timestamp_value, timezone);
         return t && granularLabel(t, value.granularity ?? "second");
      }
      default:
         return literalText(value);
   }
}

type WallClock = [string, string, string, string, string, string];

/** A date's digits: a date has no zone, so they are read as written. */
function dateDigits(text: string): [string, string, string] | undefined {
   const day = /^(\d{4})-(\d{2})-(\d{2})/.exec(text);
   return day ? [day[1], day[2], day[3]] : undefined;
}

/** An instant's wall clock in `timezone`, UTC when unset. */
function wallClock(iso: string, timezone?: string): WallClock | undefined {
   const instant = new Date(iso);
   if (Number.isNaN(instant.getTime())) return undefined;
   let parts: Intl.DateTimeFormatPart[];
   try {
      parts = new Intl.DateTimeFormat("en-US", {
         timeZone: timezone ?? "UTC",
         hourCycle: "h23",
         year: "numeric",
         month: "2-digit",
         day: "2-digit",
         hour: "2-digit",
         minute: "2-digit",
         second: "2-digit",
      }).formatToParts(instant);
   } catch {
      return undefined;
   }
   const part = (type: string) =>
      parts.find((p) => p.type === type)?.value ?? "";
   return [
      part("year").padStart(4, "0"),
      part("month"),
      part("day"),
      part("hour"),
      part("minute"),
      part("second"),
   ];
}

function granularLabel([y, mo, d, h, mi, s]: WallClock, granularity: string) {
   switch (granularity) {
      case "year":
         return y;
      case "quarter":
         return `${y}-Q${Math.floor((Number(mo) - 1) / 3) + 1}`;
      case "month":
         return `${y}-${mo}`;
      case "week":
      case "day":
         return `${y}-${mo}-${d}`;
      case "hour":
         return `${y}-${mo}-${d} ${h}`;
      case "minute":
         return `${y}-${mo}-${d} ${h}:${mi}`;
      default:
         return `${y}-${mo}-${d} ${h}:${mi}:${s}`;
   }
}
