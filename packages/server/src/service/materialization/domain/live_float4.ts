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
 * build captured as FLOAT is one, and so is `min` or `max` of one, and `sum`
 * of one in a query that reads no join (a `sum` of `real` is `real` in
 * Postgres; the stored answer sums the same float4 values in double
 * precision and rounds the sum once, where Postgres rounds after every row,
 * so a sum can differ in its last digits, and so can any later arithmetic on
 * it). Where the query reads a join, in the served compile or the live one,
 * no sum is marked: Malloy's Postgres dialect may write it symmetrically,
 * through NUMERIC, which live answers as a `numeric`. A unary minus,
 * parentheses, a filtered or ungrouped aggregate keep their operand's type;
 * `+ - *` are `real` when both operands are; `coalesce` and a `pick` are
 * when their values are reals or number literals, one a real. Anything else
 * (an `avg`, which Postgres answers as a float8 mean of the widened values,
 * any `/`, which Malloy writes as `a*1.0/b`, arithmetic with an integer or a
 * decimal, any function) is left as it ran, which is
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
   const a = Math.abs(f);
   const out = shortestFloat4Fast(a) ?? Number(shortestFloat4Text(a));
   return f < 0 ? -out : out;
}

/**
 * {@link float4AsLive} by exact integer arithmetic alone, never the fast
 * path: what the fast path is checked against.
 */
export function float4AsLiveExact(value: number): number {
   if (!Number.isFinite(value)) return value;
   const f = Math.fround(value);
   if (!Number.isFinite(f)) return value;
   if (f === 0) return f;
   const out = Number(shortestFloat4Text(Math.abs(f)));
   return f < 0 ? -out : out;
}

const BITS = new DataView(new ArrayBuffer(4));

/** The float4 `steps` units in the last place from a positive float4. */
function float4Beside(f: number, steps: 1 | -1): number {
   BITS.setFloat32(0, f);
   BITS.setUint32(0, BITS.getUint32(0) + steps);
   return BITS.getFloat32(0);
}

/**
 * The common case of {@link shortestFloat4Text} without integer arithmetic,
 * or undefined where it cannot be sure, which the exact path then decides.
 * For a normal float4 that is not a power of two the rounding interval is
 * symmetric, so at each length the correctly rounded decimal
 * (`toPrecision`) is the closest one and the only one that can fall inside.
 * Unsure: a power of two (its interval is narrower below), a subnormal, a
 * decimal that reads back exactly on the interval's edge, and a possible tie
 * at that length (the next digit reads 5), where `toPrecision` rounds up and
 * `float4out` to even.
 */
function shortestFloat4Fast(a: number): number | undefined {
   BITS.setFloat32(0, a);
   const bits = BITS.getUint32(0);
   if ((bits & 0x7fffff) === 0 || bits >>> 23 === 0) return undefined;
   // Both midpoints are exact doubles (25 significant bits).
   const low = (a + float4Beside(a, -1)) / 2;
   const high = (a + float4Beside(a, 1)) / 2;
   // Inside at one length, inside at every longer one (the closest decimal
   // only gets closer), so the shortest is found by bisection.
   let lo = 1;
   let hi = 9;
   let found: number | undefined;
   while (lo <= hi) {
      const digits = (lo + hi) >> 1;
      const n = Number(a.toPrecision(digits));
      if (n === low || n === high) return undefined;
      if (n > low && n < high) {
         found = digits;
         hi = digits - 1;
      } else {
         lo = digits + 1;
      }
   }
   if (found === undefined) return undefined;
   const next = a.toExponential(found);
   if (next[next.indexOf("e") - 1] === "5") return undefined;
   return Number(a.toPrecision(found));
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

/** What the inference reads besides the IR. */
interface Context {
   /** The float4 columns of each `storage=` table, by its handle. */
   float4: ReadonlyMap<string, ReadonlySet<string>>;
   /**
    * Whether a `sum` can be a float4: false when the query reads a join,
    * where Malloy's Postgres dialect may sum symmetrically, through
    * `NUMERIC`, which live answers as a `numeric`.
    */
   sums: boolean;
}

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
   ctx: Context,
   depth: number,
): boolean {
   const node = asNode(e);
   if (!node || depth > MAX_DEPTH) return false;
   const kids = asNode(node.kids);
   const recur = (x: unknown) => isFloat4Expr(x, scope, ctx, depth + 1);
   const realOrLiteral = (x: unknown) => recur(x) || isNumberLiteral(x);
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
         if (isFloat4Column(hit.field, hit.owner, ctx.float4)) return true;
         return hit.field.e !== undefined
            ? isFloat4Expr(hit.field.e, hit.scope, ctx, depth + 1)
            : false;
      }
      case "aggregate":
         return (
            ((node.function === "sum" && ctx.sums) ||
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
      // Not `/`: Malloy's Postgres dialect writes `a*1.0/b`, a float8.
      case "+":
      case "-":
      case "*":
         return recur(kids?.left) && recur(kids?.right);
      // A real beside a number literal resolves to real.
      case "coalesce":
         return recur(kids?.left) && realOrLiteral(kids?.right);
      case "case": {
         // `pick`: real when every branch is a real or a number literal, and
         // one is a real.
         const thens = Array.isArray(kids?.caseThen) ? kids.caseThen : [];
         const branches = kids?.caseElse ? [...thens, kids.caseElse] : thens;
         return (
            branches.length > 0 &&
            branches.every(realOrLiteral) &&
            branches.some(recur)
         );
      }
      default:
         return false;
   }
}

function isNumberLiteral(e: unknown): boolean {
   return asNode(e)?.node === "numberLiteral";
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
   ctx: Context,
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
         if (f && name && isFloat4Output(f, stage, ctx)) shape.add(name);
      }
      const out = asNode(segment.outputStruct) as StructLike | undefined;
      scope = { struct: out ?? {}, outputs: shape };
   }
   return shape;
}

/** Whether one output field is a float4 (a nest never is). */
function isFloat4Output(f: Node, scope: Scope, ctx: Context): boolean {
   if (f.type === "turtle") return false;
   if (f.type === "fieldref" && Array.isArray(f.path)) {
      return isFloat4Expr({ node: "field", path: f.path }, scope, ctx, 0);
   }
   return f.e !== undefined && isFloat4Expr(f.e, scope, ctx, 0);
}

/**
 * Whether a compiled pipeline reads a join anywhere: a segment with an active
 * join, a path through a join (or a record) in a field it reads, or an
 * aggregate taken at a join. Read wide, failing toward "yes", since a sum
 * Malloy writes symmetrically is a `numeric` live.
 */
function queryReadsJoin(query: unknown, depth = 0): boolean {
   if (depth > MAX_DEPTH) return true;
   if (Array.isArray(query)) {
      return query.some((q) => queryReadsJoin(q, depth + 1));
   }
   const node = asNode(query);
   if (!node) return false;
   if (Array.isArray(node.activeJoins) && node.activeJoins.length > 0) {
      return true;
   }
   if (Array.isArray(node.structPath) && node.structPath.length > 0) {
      return true;
   }
   if (Array.isArray(node.path) && node.path.length > 1) return true;
   for (const [key, value] of Object.entries(node)) {
      if (key === "at" || key === "location") continue;
      if (
         value &&
         typeof value === "object" &&
         queryReadsJoin(value, depth + 1)
      ) {
         return true;
      }
   }
   return false;
}

/**
 * A served query's IR, as far as this reads it: the compiled query
 * (`governance/ir`'s `PreparedQueryView.query`) and the struct it runs on.
 */
interface ServedQueryIr {
   query: unknown;
   root: unknown;
   /**
    * The live compile's query, read only for whether it reads a join (a
    * served shape can answer a join from stored columns without one).
    */
   liveQuery?: unknown;
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
   const sums =
      !queryReadsJoin(pipeline) &&
      !queryReadsJoin(asNode(ir.liveQuery)?.pipeline);
   return pipelineShape(pipeline, { struct: root }, { float4, sums });
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
   // A column repeats its values (a group-by, a min over few groups).
   const written = new Map<number, number>();
   for (const raw of result._queryResult.result) {
      const row = asNode(raw);
      if (!row) continue;
      for (const name of shape) {
         const value = row[name];
         // Zero is written as it is, and -0 must not take 0's memo.
         if (typeof value !== "number" || value === 0) continue;
         let out = written.get(value);
         if (out === undefined) {
            out = float4AsLive(value);
            written.set(value, out);
         }
         row[name] = out;
      }
   }
}
