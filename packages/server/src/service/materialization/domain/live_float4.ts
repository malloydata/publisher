// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A stored answer's float4 values, as live gives them on Postgres.
 *
 * Live on Postgres reads its rows through JSON, and Postgres writes a `real`
 * (float4) as the shortest text that reads back as the same float4: a `real`
 * holding 0.1 answers `0.1`. A `storage=` build copies the column into
 * DuckLake as the same float4 (DuckDB's FLOAT), but a DuckDB read gives a
 * FLOAT as the double it widens to, so the served answer was
 * `0.10000000149011612`. The stored value is the same; only its rendering
 * differed. Rendering each value live types `real` as Postgres renders it
 * gives live's answer exactly, value and JSON form.
 *
 * Which values: the output columns whose expression Postgres types `real`,
 * read from the served query's IR. A stored column of a `storage=` table its
 * build captured as FLOAT is one, and so is `min`, `max` or `sum` of one (a
 * `sum` of `real` is `real` in Postgres; the stored answer sums the same
 * float4 values in double precision and rounds the sum once, where Postgres
 * rounds after every row, so a long sum may differ in its last digits, as two
 * live runs that add in different orders can). A unary minus, parentheses, a
 * filtered or ungrouped aggregate keep their operand's type, and `+ - * /`
 * and `coalesce` are `real` when both operands are. Anything else (an `avg`,
 * which Postgres answers as a float8 mean of the widened values, arithmetic
 * with an integer or a decimal, any function) is left as it ran, which is
 * what live gives for those. Only top-level columns: Malloy's Postgres
 * dialect casts every number of a nest to DOUBLE PRECISION, so live's nested
 * values are the widened doubles the stored read gives. A database table is
 * read in Postgres itself, so its answer is already live's; only `virtual()`
 * structs (`storage=` tables) are read here.
 *
 * Query serving calls {@link roundFloat4AsLive} on every answer a leg ran
 * whose live dialect is Postgres ({@link liveWritesFloat4AsPostgres}); it
 * writes in place. Pure but for that.
 */

import { capabilitiesOf } from "./dialect";

/**
 * Whether live writes a float4 as its shortest text (Postgres, whose JSON
 * stage reads it through `float4out`). Measured by the R4 float4 parity spec.
 */
export function liveWritesFloat4AsPostgres(dialect: unknown): boolean {
   return capabilitiesOf(dialect).liveWritesFloat4Shortest;
}

/** The stored types a float4 is captured as (DuckDB's DESCRIBE spellings). */
const FLOAT4_TYPES = new Set(["FLOAT", "FLOAT4", "REAL"]);

/** Whether a stored column type is a float4. */
function isFloat4Type(type: string): boolean {
   return FLOAT4_TYPES.has(type.trim().toUpperCase());
}

/**
 * The float4 columns of each `storage=` binding, by its `virtual()` handle,
 * lowercased (a destination may fold the case DESCRIBE reports).
 */
export function float4ColumnsByHandle(
   bindings: readonly {
      virtualHandle?: string;
      schema?: readonly { name: string; type: string }[];
   }[],
): Map<string, Set<string>> {
   const out = new Map<string, Set<string>>();
   for (const b of bindings) {
      if (!b.virtualHandle) continue;
      const columns = (b.schema ?? [])
         .filter((c) => isFloat4Type(c.type))
         .map((c) => c.name.toLowerCase());
      if (columns.length === 0) continue;
      const set = out.get(b.virtualHandle) ?? new Set<string>();
      for (const c of columns) set.add(c);
      out.set(b.virtualHandle, set);
   }
   return out;
}

/**
 * A number as live's JSON stage gives a float4: the float4 nearest it,
 * written as Postgres's `float4out` writes it, and read back as a double.
 * Not finite, or past float4's range, it is left as it is.
 *
 * `float4out` writes the shortest decimal strictly inside the float4's
 * rounding interval (a decimal exactly halfway to a neighbour is not taken,
 * though reading it back would round to this float4 by ties-to-even), and of
 * those the closest, the even last digit on a tie: 38261648 is written
 * `3.8261648e+07`, not `38261650`, and 3388560.25 as `3.3885602e+06`. Exact
 * integer arithmetic throughout, so no double rounding decides a tie.
 */
export function float4AsLive(value: number): number {
   if (!Number.isFinite(value)) return value;
   const f = Math.fround(value);
   if (!Number.isFinite(f)) return value;
   if (f === 0) return f;
   const text = shortestFloat4Text(Math.abs(f));
   return f < 0 ? -Number(text) : Number(text);
}

/** `float4out`'s digits for a positive finite float4, as `<digits>e<power>`. */
function shortestFloat4Text(f: number): string {
   const view = new DataView(new ArrayBuffer(4));
   view.setFloat32(0, f);
   const bits = view.getUint32(0);
   const biased = (bits >>> 23) & 0xff;
   const fraction = bits & 0x7fffff;
   // f = m2 * 2^e2 exactly.
   const m2 = BigInt(biased === 0 ? fraction : fraction | 0x800000);
   const e2 = biased === 0 ? -149 : biased - 150;
   // The candidates' decimal exponents: the estimate's, and one either side
   // (log10 can be off by one next to a power of ten).
   const estimate = Math.floor(Math.log10(f));
   const lowestPower = estimate - 1 - 8;
   // One integer scale for everything: 2^shift2 * 10^shift10.
   const shift2 = Math.max(0, 2 - e2);
   const shift10 = Math.max(0, -lowestPower);
   const scale = (mantissa: bigint, exp2: number, exp10: number) =>
      mantissa * 2n ** BigInt(exp2 + shift2) * 10n ** BigInt(exp10 + shift10);
   const scaled = scale(m2, e2, 0);
   // Half the gap to each neighbour: the gap below a power of two is half
   // the gap above it.
   const quarter = scale(1n, e2 - 2, 0);
   const low = scaled - (fraction === 0 && biased > 1 ? 1n : 2n) * quarter;
   const high = scaled + 2n * quarter;
   for (let digits = 1; digits <= 9; digits++) {
      let best: { m: bigint; power: number; distance: bigint } | undefined;
      for (const e10 of [estimate - 1, estimate, estimate + 1]) {
         const power = e10 - (digits - 1);
         const unit = scale(1n, 0, power);
         const floor = scaled / unit;
         for (const m of [floor, floor + 1n]) {
            if (m < 10n ** BigInt(digits - 1) || m >= 10n ** BigInt(digits)) {
               continue;
            }
            const c = m * unit;
            if (c <= low || c >= high) continue;
            const distance = c > scaled ? c - scaled : scaled - c;
            if (
               !best ||
               distance < best.distance ||
               (distance === best.distance && m % 2n === 0n)
            ) {
               best = { m, power, distance };
            }
         }
      }
      if (best) return `${best.m}e${best.power}`;
   }
   return String(f);
}

// ---- Which output columns are float4 -----------------------------------

/** An IR node, duck-typed: Malloy exports no type for the prepared IR. */
type Node = Record<string, unknown>;

/** A struct's fields as the IR holds them. */
interface StructLike {
   type?: unknown;
   name?: unknown;
   fields?: unknown;
}

/** The names of a query's top-level output columns that are float4. */
export type Float4Shape = ReadonlySet<string>;

/** Where a path is resolved: a struct, and what of its fields is float4. */
interface Scope {
   struct: StructLike;
   /** Fields of a previous stage's output that were float4. */
   outputs?: Float4Shape;
   /** A stage's own `extend:` fields, looked up first. */
   extend?: StructLike;
}

const MAX_DEPTH = 64;

function asNode(v: unknown): Node | undefined {
   return v && typeof v === "object" ? (v as Node) : undefined;
}

function fieldsOf(s: StructLike | undefined): Node[] {
   return Array.isArray(s?.fields) ? (s.fields as Node[]) : [];
}

function outputName(f: Node): string | undefined {
   const as = f.as;
   if (typeof as === "string" && as) return as;
   if (f.type === "fieldref" && Array.isArray(f.path)) {
      const last = f.path[f.path.length - 1];
      return typeof last === "string" ? last : undefined;
   }
   return typeof f.name === "string" ? f.name : undefined;
}

function findField(s: StructLike | undefined, name: string): Node | undefined {
   return fieldsOf(s).find((f) => outputName(f) === name);
}

/**
 * Resolve a path: the field, the struct that holds it, and the scope its own
 * expression reads (the stage's for a field of its input, the join's for a
 * field reached through one).
 */
function resolve(
   scope: Scope,
   path: readonly unknown[],
): { field: Node; owner: StructLike; scope: Scope } | undefined {
   if (path.length === 0 || typeof path[0] !== "string") return undefined;
   let owner: StructLike | undefined =
      scope.extend && findField(scope.extend, path[0])
         ? scope.extend
         : scope.struct;
   let inner: Scope = scope;
   for (let i = 0; i < path.length; i++) {
      const name = path[i];
      if (typeof name !== "string") return undefined;
      const field = findField(owner, name);
      if (!field) return undefined;
      if (i === path.length - 1) return { field, owner: owner!, scope: inner };
      owner = field as StructLike;
      inner = { struct: owner };
   }
   return undefined;
}

function isFloat4Column(
   field: Node,
   owner: StructLike,
   float4: ReadonlyMap<string, ReadonlySet<string>>,
): boolean {
   if (field.e !== undefined || owner.type !== "virtual") return false;
   const columns =
      typeof owner.name === "string" ? float4.get(owner.name) : undefined;
   return (
      !!columns &&
      typeof field.name === "string" &&
      columns.has(field.name.toLowerCase())
   );
}

/** Whether Postgres types an expression `real`. */
function isFloat4Expr(
   e: unknown,
   scope: Scope,
   float4: ReadonlyMap<string, ReadonlySet<string>>,
   depth: number,
): boolean {
   const node = asNode(e);
   if (!node || depth > MAX_DEPTH) return false;
   const kids = asNode(node.kids);
   const recur = (x: unknown) => isFloat4Expr(x, scope, float4, depth + 1);
   switch (node.node) {
      case "field": {
         if (!Array.isArray(node.path)) return false;
         if (scope.outputs && node.path.length === 1) {
            const name = node.path[0];
            if (typeof name === "string" && scope.outputs.has(name)) {
               return true;
            }
         }
         const hit = resolve(scope, node.path);
         if (!hit) return false;
         if (isFloat4Column(hit.field, hit.owner, float4)) return true;
         return hit.field.e !== undefined
            ? isFloat4Expr(hit.field.e, hit.scope, float4, depth + 1)
            : false;
      }
      case "aggregate":
         return (
            (node.function === "sum" ||
               node.function === "min" ||
               node.function === "max") &&
            recur(node.e)
         );
      case "()":
      case "unary-":
      case "all":
      case "exclude":
         return recur(node.e);
      case "filteredExpr":
         return recur(kids?.e);
      case "+":
      case "-":
      case "*":
      case "/":
         return recur(kids?.left) && recur(kids?.right);
      case "coalesce":
         return (
            recur(kids?.left) &&
            (recur(kids?.right) || isIntegerLiteral(kids?.right))
         );
      default:
         return false;
   }
}

function isIntegerLiteral(e: unknown): boolean {
   const node = asNode(e);
   return (
      node?.node === "numberLiteral" &&
      typeof node.literal === "string" &&
      /^-?\d+$/.test(node.literal)
   );
}

/**
 * The float4 outputs of a pipeline run over `input`: each stage's outputs are
 * the next stage's input. A nest is never one: Malloy's Postgres dialect
 * casts every number of a nest to DOUBLE PRECISION, so live's nested values
 * are the widened doubles a DuckDB read gives too.
 */
function pipelineShape(
   pipeline: readonly unknown[],
   input: Scope,
   float4: ReadonlyMap<string, ReadonlySet<string>>,
): Float4Shape {
   let scope = input;
   let shape = new Set<string>();
   for (const raw of pipeline) {
      const segment = asNode(raw);
      if (
         !segment ||
         (segment.type !== "reduce" && segment.type !== "project")
      ) {
         return new Set();
      }
      const extend = asNode(segment.extendSource) as StructLike | undefined;
      const stage: Scope = { ...scope, ...(extend ? { extend } : {}) };
      shape = new Set();
      const fields = Array.isArray(segment.queryFields)
         ? segment.queryFields
         : [];
      for (const rawField of fields) {
         const f = asNode(rawField);
         const name = f && outputName(f);
         if (f && name && isFloat4Output(f, stage, float4)) shape.add(name);
      }
      const out = asNode(segment.outputStruct) as StructLike | undefined;
      scope = { struct: out ?? {}, outputs: shape };
   }
   return shape;
}

/** Whether one output field is a float4 (a nest never is). */
function isFloat4Output(
   f: Node,
   scope: Scope,
   float4: ReadonlyMap<string, ReadonlySet<string>>,
): boolean {
   if (f.type === "turtle") return false;
   if (f.type === "fieldref" && Array.isArray(f.path)) {
      return isFloat4Expr({ node: "field", path: f.path }, scope, float4, 0);
   }
   return f.e !== undefined && isFloat4Expr(f.e, scope, float4, 0);
}

/**
 * A served query's IR, as far as this reads it: the compiled query
 * (`governance/ir`'s `PreparedQueryView.query`) and the struct it runs on.
 */
interface ServedQueryIr {
   query: unknown;
   root: unknown;
}

/**
 * The top-level output columns of a served query that Postgres would type
 * `real`. Empty when there are none or the IR cannot be read.
 */
export function float4OutputShape(
   ir: ServedQueryIr,
   float4: ReadonlyMap<string, ReadonlySet<string>>,
): Float4Shape {
   if (float4.size === 0) return new Set();
   const query = asNode(ir.query);
   const root = asNode(ir.root) as StructLike | undefined;
   if (!query || !root) return new Set();
   let pipeline = Array.isArray(query.pipeline) ? query.pipeline : [];
   const head = asNode(query.pipeHead);
   if (head && typeof head.name === "string") {
      const turtle = findField(root, head.name);
      if (!turtle || !Array.isArray(turtle.pipeline)) return new Set();
      pipeline = [...turtle.pipeline, ...pipeline];
   }
   return pipelineShape(pipeline, { struct: root }, float4);
}

/** A run's result, as far as this writes it: its rows. */
export interface ResultRows {
   _queryResult: { result: unknown[] };
}

/**
 * Write every float4 value of a served result as live's JSON stage writes
 * it, in place.
 */
export function roundFloat4AsLive(
   result: ResultRows,
   shape: Float4Shape,
): void {
   if (shape.size === 0) return;
   for (const raw of result._queryResult.result) {
      const row = asNode(raw);
      if (!row) continue;
      for (const name of shape) {
         const value = row[name];
         if (typeof value === "number") row[name] = float4AsLive(value);
      }
   }
}
