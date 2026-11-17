// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether a `storage=` table read past an entry point's row-level gate holds
 * the columns that gate reads exactly as live compares them (question 14,
 * F1 and F2). A G3 table is read past a gate because the gate is grafted on
 * the answer and compares the stored values as live compares the source's.
 * That holds only when each stored value the gate reads equals live's under
 * the comparison:
 *
 * - a number live holds exactly (an integer, a big integer or a decimal)
 *   stored as a float compares by its rounded value, so `acct = $ACCT` above
 *   2^53 matches a neighbour's rows (F2);
 * - a column the served copy leaves out (no cast reads it as live's type)
 *   cannot be compared at all, so the graft would deny a caller live
 *   answers;
 * - a date or time a caller reads in another `timezone:` than the build
 *   stored it in may compare in another zone (F1).
 *
 * A table whose gate columns are not exact is judged as not G3 (its
 * `ownGateKept` and `holdsOnlyUngatedRows` cleared), so the router answers
 * live. Fail closed: a gate that reads anything but the entry point's own
 * plain columns (a computed dimension, a join path, an unreadable gate) is
 * held to every column of every table read.
 */

import type { ModelDef, SourceDef } from "@malloydata/malloy";
import { collectEntryPointGates } from "../../gate_classification";
import { malloyIdentifiers } from "../../query_text";
import type { Binding } from "../domain/binding";
import { storedTypeServesAs } from "../stored_column_types";

/** A field as the compiled source declares it. */
interface FieldLike {
   name?: unknown;
   as?: unknown;
   type?: unknown;
   numberType?: unknown;
   e?: unknown;
   join?: unknown;
   expressionType?: unknown;
}

/** A stored type that holds a number only approximately. */
const FLOAT_TYPE = /^\s*(DOUBLE|FLOAT|REAL|FLOAT4|FLOAT8)\b/i;

/** A live type a caller's `timezone:` may compare differently. */
const TEMPORAL = new Set(["date", "timestamp", "timestamptz"]);

const nameOf = (f: FieldLike) => String(f.as ?? f.name);

/** Whether `f` is a plain column: no expression, no join. */
function plainColumn(f: FieldLike): boolean {
   return (
      f.e === undefined &&
      f.join === undefined &&
      (f.expressionType === undefined || f.expressionType === "scalar")
   );
}

/**
 * The columns the entry point's row-level gates read, by name, or `"all"`
 * when a gate reads something other than its own plain columns, or the
 * gates cannot be read; `undefined` when it carries no gate.
 */
export function gateColumnsOf(
   struct: unknown,
   modelDef: unknown,
): ReadonlySet<string> | "all" | undefined {
   let exprs: string[];
   try {
      exprs = collectEntryPointGates(
         struct as SourceDef,
         modelDef as ModelDef,
         new Set(),
         true,
      ).flatMap((g) => [...g.exprs]);
   } catch {
      return "all";
   }
   if (exprs.length === 0) return undefined;
   const fields = new Map(
      (
         ((struct as { fields?: unknown[] } | undefined)?.fields ??
            []) as FieldLike[]
      ).map((f) => [nameOf(f), f] as const),
   );
   const columns = new Set<string>();
   for (const expr of exprs) {
      for (const id of malloyIdentifiers(expr)) {
         const f = fields.get(id);
         if (!f) continue;
         if (!plainColumn(f)) return "all";
         columns.add(id);
      }
   }
   return columns;
}

/**
 * Whether a stored column reads, under a gate's comparison, exactly as the
 * live field it holds: false for a float holding a number live keeps exact,
 * a left-out column, a field not found (fail closed), and, in a zoned query,
 * a date or time.
 */
function exactColumn(
   storedType: string,
   live: FieldLike | undefined,
   leftOut: boolean,
   zoned: boolean,
): boolean {
   if (leftOut || !live) return false;
   // A stored type that does not read as live's (a drifted column, which the
   // served copy leaves out): the gate cannot compare it.
   if (
      typeof live.type === "string" &&
      storedTypeServesAs(storedType, live.type) === false
   ) {
      return false;
   }
   if (live.type === "number" && live.numberType !== "float") {
      return !FLOAT_TYPE.test(storedType);
   }
   if (zoned && typeof live.type === "string" && TEMPORAL.has(live.type)) {
      return false;
   }
   return true;
}

/**
 * `tables` as the gate judgement reads them: each `storage=` table admitted
 * as G3 (`ownGateKept`, `holdsOnlyUngatedRows`) whose gate columns are not
 * exact, with both cleared. A colocated table is left as it is (its tier
 * decides), as is every table when the entry point carries no gate.
 */
export function judgeGateColumns(args: {
   tables: readonly Binding[];
   gateColumns: ReadonlySet<string> | "all" | undefined;
   /** The query sets a `timezone:` anywhere. */
   zoned: boolean;
   /** The live field a stored column holds, by the table's sources. */
   liveField: (table: Binding, column: string) => FieldLike | undefined;
   /** The columns the served copy leaves out of a table's type. */
   leftOut: (table: Binding) => readonly string[];
}): Binding[] {
   const { gateColumns } = args;
   if (gateColumns === undefined) return [...args.tables];
   return args.tables.map((t) => {
      if (
         t.tier !== "storage" ||
         (t.ownGateKept !== true && t.holdsOnlyUngatedRows !== true)
      ) {
         return t;
      }
      const leftOut = new Set(args.leftOut(t));
      const checked =
         gateColumns === "all"
            ? t.schema
            : t.schema.filter((c) => gateColumns.has(c.name));
      // A gate column the table should hold but whose type left it out.
      const missing =
         gateColumns === "all"
            ? leftOut.size > 0
            : [...gateColumns].some((c) => leftOut.has(c));
      const exact =
         !missing &&
         checked.every((c) =>
            exactColumn(
               c.type,
               args.liveField(t, c.name),
               leftOut.has(c.name),
               args.zoned,
            ),
         );
      if (exact) return t;
      const { ownGateKept: _kept, holdsOnlyUngatedRows: _ungated, ...rest } = t;
      return rest;
   });
}
