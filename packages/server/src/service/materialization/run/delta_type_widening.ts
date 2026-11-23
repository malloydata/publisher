// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether an incremental table's stored columns hold the values a delta
 * brings, judged from the two native types alone.
 *
 * Malloy reads an INTEGER and a BIGINT, a FLOAT and a DOUBLE, all as
 * `number`, so an upstream column widened after a table was built changes
 * nothing the plan sees. A delta then writes the wider values into the
 * narrower stored column: a BIGINT past 2^31 fails the delta on every run,
 * and a DOUBLE is rounded into a FLOAT with no error, a wrong answer. A
 * column whose stored type does not hold its delta type losslessly makes the
 * planner rebuild the table in full instead (`shape_mismatch`).
 *
 * Numbers only: the integer, binary float and decimal types of Postgres and
 * DuckDB, spelled as Postgres's `information_schema` and `regtype` and
 * DuckDB's `duckdb_columns()` and `DESCRIBE` spell them. A spelling outside
 * them is not judged (two strings, a timestamp, an unknown type), and neither
 * is a Postgres `numeric`'s precision, which neither read reports.
 *
 * Pure.
 */

type NumericType =
   | { kind: "int"; bits: number; signed: boolean }
   | { kind: "float"; bits: 32 | 64 }
   /** Precision and scale undefined: unbounded (Postgres's `numeric`). */
   | { kind: "decimal"; precision?: number; scale?: number };

const INTEGERS: Record<string, { bits: number; signed: boolean }> = {
   tinyint: { bits: 8, signed: true },
   int1: { bits: 8, signed: true },
   smallint: { bits: 16, signed: true },
   int2: { bits: 16, signed: true },
   integer: { bits: 32, signed: true },
   int: { bits: 32, signed: true },
   int4: { bits: 32, signed: true },
   bigint: { bits: 64, signed: true },
   int8: { bits: 64, signed: true },
   hugeint: { bits: 128, signed: true },
   int128: { bits: 128, signed: true },
   utinyint: { bits: 8, signed: false },
   usmallint: { bits: 16, signed: false },
   uinteger: { bits: 32, signed: false },
   ubigint: { bits: 64, signed: false },
   uhugeint: { bits: 128, signed: false },
};

const FLOATS: Record<string, 32 | 64> = {
   real: 32,
   float: 32,
   float4: 32,
   double: 64,
   "double precision": 64,
   float8: 64,
};

/**
 * Decimal digits an integer of `bits` may need, its sign aside: a signed
 * BIGINT's 9223372036854775807 is 19, an unsigned one's
 * 18446744073709551615 is 20.
 */
function integerDigits(bits: number, signed: boolean): number {
   switch (bits) {
      case 8:
         return 3;
      case 16:
         return 5;
      case 32:
         return 10;
      case 64:
         return signed ? 19 : 20;
      default:
         return 39;
   }
}

/** `type` as a number type, or undefined when it is not one judged here. */
function numericType(type: string): NumericType | undefined {
   const t = type.trim().toLowerCase();
   if (t in INTEGERS) return { kind: "int", ...INTEGERS[t] };
   if (t in FLOATS) return { kind: "float", bits: FLOATS[t] };
   if (t === "numeric" || t === "decimal") return { kind: "decimal" };
   const m = /^(?:numeric|decimal)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)$/.exec(t);
   if (m) {
      return {
         kind: "decimal",
         precision: Number(m[1]),
         scale: m[2] === undefined ? 0 : Number(m[2]),
      };
   }
   return undefined;
}

/** Whether every value of `delta` is a value of `stored`, unchanged. */
/** The digits an integer type may need ({@link integerDigits}). */
function digitsOf(t: { bits: number; signed: boolean }): number {
   return integerDigits(t.bits, t.signed);
}

function holds(stored: NumericType, delta: NumericType): boolean {
   switch (stored.kind) {
      case "int":
         if (delta.kind !== "int") return false;
         return stored.signed
            ? delta.signed
               ? stored.bits >= delta.bits
               : stored.bits > delta.bits
            : !delta.signed && stored.bits >= delta.bits;
      case "float":
         if (delta.kind === "float") return stored.bits >= delta.bits;
         // A binary float holds an integer exactly up to its significand:
         // 2^24 for a FLOAT, 2^53 for a DOUBLE.
         if (delta.kind === "int") {
            return delta.bits <= (stored.bits === 64 ? 32 : 16);
         }
         return false;
      case "decimal": {
         if (stored.precision === undefined) return delta.kind !== "float";
         const integerDigits = stored.precision - (stored.scale ?? 0);
         if (delta.kind === "int") {
            return integerDigits >= digitsOf(delta);
         }
         if (delta.kind !== "decimal" || delta.precision === undefined) {
            return false;
         }
         return (
            (stored.scale ?? 0) >= (delta.scale ?? 0) &&
            integerDigits >= delta.precision - (delta.scale ?? 0)
         );
      }
   }
}

/**
 * Each column whose stored type does not hold its delta type losslessly, as
 * `"name" (stored → delta)` text for a seed's reason, or an empty list.
 * Columns either side lacks, and types not judged, are left out.
 */
export function widenedColumns(
   stored: ReadonlyMap<string, string>,
   delta: ReadonlyMap<string, string>,
): string[] {
   const widened: string[] = [];
   for (const [column, deltaType] of delta) {
      const storedType = stored.get(column);
      if (storedType === undefined) continue;
      if (storedType.trim().toLowerCase() === deltaType.trim().toLowerCase()) {
         continue;
      }
      const s = numericType(storedType);
      const d = numericType(deltaType);
      if (!s || !d || holds(s, d)) continue;
      widened.push(`"${column}" (${storedType} → ${deltaType})`);
   }
   return widened;
}
