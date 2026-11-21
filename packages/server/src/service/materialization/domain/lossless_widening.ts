// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether a column may move from one type to another without changing any value
 * it holds: the one rule the bind (a table altered outside a build) and a keyed
 * load (objects against a table) judge a type move by. No I/O.
 */

/** A stored type, parsed as far as {@link losslessWidening} compares one. */
type ParsedType =
   | { kind: "int"; rank: number; unsigned: boolean }
   | { kind: "decimal"; precision: number; scale: number }
   | { kind: "float"; rank: number }
   | { kind: "text"; length: number }
   /** Fixed-length, blank-padded text (`character(n)`, `bpchar`). */
   | { kind: "char"; length: number }
   | { kind: "other"; spelling: string };

/** Each integer's width, by its spellings, with the decimal digits it holds. */
const INTEGER_WIDTHS: Record<string, { rank: number; unsigned: boolean }> = {
   tinyint: { rank: 1, unsigned: false },
   int1: { rank: 1, unsigned: false },
   smallint: { rank: 2, unsigned: false },
   int2: { rank: 2, unsigned: false },
   short: { rank: 2, unsigned: false },
   integer: { rank: 3, unsigned: false },
   int: { rank: 3, unsigned: false },
   int4: { rank: 3, unsigned: false },
   signed: { rank: 3, unsigned: false },
   bigint: { rank: 4, unsigned: false },
   int8: { rank: 4, unsigned: false },
   int64: { rank: 4, unsigned: false },
   long: { rank: 4, unsigned: false },
   hugeint: { rank: 5, unsigned: false },
   int128: { rank: 5, unsigned: false },
   utinyint: { rank: 1, unsigned: true },
   usmallint: { rank: 2, unsigned: true },
   uinteger: { rank: 3, unsigned: true },
   ubigint: { rank: 4, unsigned: true },
   uhugeint: { rank: 5, unsigned: true },
};

/** The decimal digits each integer width holds in full. */
const INTEGER_DIGITS = [0, 3, 5, 10, 19, 39];

function parsedType(type: string): ParsedType {
   const t = type.trim().replace(/\s+/g, " ").toLowerCase();
   const width = INTEGER_WIDTHS[t];
   if (width) return { kind: "int", ...width };
   const decimal =
      /^(decimal|numeric|number|dec)\s*(?:\((\d+)\s*(?:,\s*(\d+))?\))?$/.exec(
         t,
      );
   if (decimal) {
      if (decimal[2] === undefined) {
         // Unconstrained: Postgres's `numeric` holds any value exactly;
         // Snowflake's `NUMBER` is NUMBER(38,0).
         return decimal[1] === "number"
            ? { kind: "decimal", precision: 38, scale: 0 }
            : { kind: "decimal", precision: Infinity, scale: Infinity };
      }
      return {
         kind: "decimal",
         precision: Number(decimal[2]),
         scale: Number(decimal[3] ?? 0),
      };
   }
   if (t === "bignumeric") return { kind: "decimal", precision: 76, scale: 38 };
   if (/^(real|float4|float)$/.test(t)) return { kind: "float", rank: 1 };
   if (/^(double|double precision|float8|float64)$/.test(t)) {
      return { kind: "float", rank: 2 };
   }
   // Fixed-length text pads with blanks, so a move to or from it adds or
   // strips them ('ab' against 'ab '): a kind apart from varying text.
   const fixed = /^(bpchar|char|character)\s*(?:\((\d+)\))?$/.exec(t);
   if (fixed) {
      return {
         kind: "char",
         length: fixed[2] !== undefined ? Number(fixed[2]) : Infinity,
      };
   }
   const text =
      /^(varchar|text|string|character varying|char varying|nvarchar)\s*(?:\((\d+)\))?$/.exec(
         t,
      );
   if (text) {
      return {
         kind: "text",
         length: text[2] !== undefined ? Number(text[2]) : Infinity,
      };
   }
   return { kind: "other", spelling: t };
}

/**
 * Whether a column moved from `built` to `now` holds every value it held
 * exactly as before: the same type, or a widening that loses nothing (a
 * wider integer, a decimal with no fewer integer or fraction digits, an
 * integer into a decimal or float that holds all its digits, FLOAT to DOUBLE,
 * a longer varying text). A move between fixed-length text (`character(n)`)
 * and varying text, or between two fixed lengths, adds or strips padding, so
 * it is never one. Anything else inside a family may have rounded or cut a
 * value, so it is never read as is.
 */
export function losslessWidening(built: string, now: string): boolean {
   const from = parsedType(built);
   const to = parsedType(now);
   if (from.kind === "int") {
      if (to.kind === "int") {
         return from.unsigned
            ? to.rank > from.rank || (to.unsigned && to.rank === from.rank)
            : !to.unsigned && to.rank >= from.rank;
      }
      if (to.kind === "decimal") {
         return to.precision - to.scale >= INTEGER_DIGITS[from.rank];
      }
      // A DOUBLE holds every 32-bit integer exactly; a FLOAT every 16-bit one.
      if (to.kind === "float") return from.rank <= to.rank + 1;
      return false;
   }
   if (from.kind === "decimal") {
      return (
         to.kind === "decimal" &&
         to.scale >= from.scale &&
         to.precision - to.scale >= from.precision - from.scale
      );
   }
   if (from.kind === "float") {
      return to.kind === "float" && to.rank >= from.rank;
   }
   if (from.kind === "text") {
      return to.kind === "text" && to.length >= from.length;
   }
   // Another length pads differently; varying text holds no padding.
   if (from.kind === "char") {
      return to.kind === "char" && to.length === from.length;
   }
   return to.kind === "other" && to.spelling === from.spelling;
}
