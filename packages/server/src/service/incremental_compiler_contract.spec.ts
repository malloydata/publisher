// Real-compiler contract for incremental materialization. Every fact below is
// load-bearing for the declaration resolver (incremental_declaration.ts), the
// publish gates (incremental_policy in package.ts) and the delta apply
// (incremental_apply.ts), and none of them is documented API — they are
// observed behaviors of the pinned @malloydata/malloy. Pinning them here means a
// compiler bump that changes one fails LOUDLY in this file, rather than silently
// changing what a delta writes into a serving table.
//
// Follows the real-compile pattern of materialization_eligibility.spec.ts:
// in-memory DuckDB + Runtime + getBuildPlan(). DuckDB is only the compile
// target; the dialect allowlist for actually RUNNING a delta is bigquery +
// postgres (see the publish gates).
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type {
   FixedConnectionMap,
   ModelMaterializer,
   PersistSource,
} from "@malloydata/malloy";
import {
   DatabricksDialect,
   DuckDBDialect,
   MySQLDialect,
   PostgresDialect,
   SnowflakeDialect,
   StandardSQLDialect,
   TrinoDialect,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { deriveAnnotationFields, deriveColumns } from "./build_plan";
import { renderSqlBound } from "./incremental_apply";
import {
   compilePersistSources,
   duckdbTestConnections,
} from "./incremental_test_harness";

let connections: FixedConnectionMap;
let duckdb: DuckDBConnection;

beforeAll(() => {
   ({ duckdb, connections } = duckdbTestConnections());
});

/**
 * Compile a single-file model, returning both its persist sources by name and
 * the ModelMaterializer. The delta itself is composed from a source's getSQL()
 * (fact 3); the materializer is here to pin what a compiled QUERY would do
 * instead, which is what the facts below are contrasted against.
 */
function compile(model: string): Promise<{
   sources: Record<string, PersistSource>;
   materializer: ModelMaterializer;
}> {
   return compilePersistSources(connections, model);
}

const RAW = `##! experimental.persistence
source: raw is duckdb.sql("""
  SELECT 1 AS amount, DATE '2024-01-01' AS order_date,
         TIMESTAMP '2024-01-01 00:00:00' AS ts, 'US' AS region,
         true AS flag, [1,2] AS tags, {'a': 1} AS rec
""")
`;

const ROLLUP = `source: mz is raw -> {
  group_by: order_date, region
  aggregate: revenue is amount.sum()
}`;

describe("incremental materialization: compiler contract", () => {
   // ── Fact 1: the declaration does not move the content address ─────────

   it("getSQL() is byte-identical with and without watermark=/merge_key=", async () => {
      // makeBuildId is a content address over (connection digest, getSQL()).
      // Declaring HOW a table advances must not re-address the table, or every
      // author who adds a watermark orphans their existing data and re-seeds.
      const plain = await compile(`${RAW}#@ persist name="t"\n${ROLLUP}`);
      const declared = await compile(
         `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date" merge_key="region"\n${ROLLUP}`,
      );
      expect(declared.sources.mz.getSQL()).toBe(plain.sources.mz.getSQL());
      // The declaration IS readable — it just isn't in the SQL.
      expect(deriveAnnotationFields(declared.sources.mz)).toEqual({
         name: "t",
         refresh: "incremental",
         watermark: "order_date",
         merge_key: "region",
      });
   });

   // ── Fact 2: the extension trap (canary) ───────────────────────────────

   it("CANARY: a generated source extension silently DROPS its where:", async () => {
      // `source: __d is mz extend { where: … }` compiles clean and produces SQL
      // byte-identical to the unfiltered source: the predicate vanishes. That is
      // why the delta is read off a QUERY stage (fact 3) and must NEVER be read
      // off a generated source extension — doing so would apply an UNBOUNDED
      // delta, rewriting the whole table while reporting a bounded range.
      //
      // This asserts behavior that is broken FOR US. If a compiler upgrade fixes
      // it, this test goes red: that is the point. Read the failure as "the
      // extension form is now safe", not as a regression to work around.
      //
      // Still pinned even though the delta no longer compiles Malloy at all (see
      // fact 3): the extension form is the obvious-looking way to reintroduce
      // one, and it fails silently.
      const { sources } = await compile(
         `${RAW}#@ persist name="t"\n${ROLLUP}\n
#@ persist name="d"
source: mz_ext is mz extend { where: order_date >= @2024-06-01 }`,
      );
      expect(sources.mz_ext).toBeDefined();
      expect(sources.mz_ext.getSQL()).toBe(sources.mz.getSQL());
      expect(sources.mz_ext.getSQL()).not.toContain("2024-06-01");
   });

   // ── Fact 3: getSQL() is the unfinalized SELECT the delta wraps ────────

   it("getSQL() returns a bare SELECT, wrappable as a subquery", async () => {
      // The delta is `SELECT * FROM (getSQL()) AS __d WHERE <range>`, so getSQL()
      // has to be ONE self-contained SELECT: no trailing semicolon, no second
      // statement, nothing that a subquery position rejects. Run here, not just
      // parsed, because "wrappable" is the property that matters.
      const { sources } = await compile(`${RAW}#@ persist name="t"\n${ROLLUP}`);
      const sourceSQL = sources.mz.getSQL();
      expect(sourceSQL.trim()).not.toEndWith(";");
      const wrapped = await duckdb.runSQL(
         `SELECT * FROM (${sourceSQL}) AS __d WHERE "order_date" >= DATE '2024-01-01'`,
      );
      expect(wrapped.rows).toHaveLength(1);
   });

   it("TRIPWIRE: Postgres is the only dialect that finalizes with row_to_json", () => {
      // The single most expensive fact in this file, and the reason the delta is
      // composed from getSQL() instead of from a compiled query's SQL.
      //
      // A QUERY's SQL is FINALIZED: Malloy appends the dialect's sqlFinalStage
      // wherever hasFinalStage is true. Postgres is the only such dialect, and its
      // final stage collapses the result into a single json column named `row` —
      // so `INSERT INTO t ("batch", …) SELECT "batch", … FROM (<query sql>)` fails
      // with `column "batch" does not exist`. This bit Publisher twice: once in
      // the seed's CTAS (#867, fixed upstream by malloydata/malloy#2964 forcing
      // PersistSource.getSQL() to compile unfinalized) and once in this delta.
      // getSQL() carries that fix; a query's SQL cannot.
      //
      // Two ways this goes red, both of which need a decision before shipping:
      // another dialect gains a final stage (check it before adding it to
      // INCREMENTAL_DIALECT_ALLOWLIST), or Postgres's wrapper changes shape.
      const dialects = {
         postgres: new PostgresDialect(),
         standardsql: new StandardSQLDialect(),
         duckdb: new DuckDBDialect(),
         snowflake: new SnowflakeDialect(),
         trino: new TrinoDialect(),
         mysql: new MySQLDialect(),
         databricks: new DatabricksDialect(),
      };
      expect(
         Object.entries(dialects)
            .filter(([, d]) => d.hasFinalStage)
            .map(([name]) => name),
      ).toEqual(["postgres"]);
      expect(dialects.postgres.sqlFinalStage("s", ["a"])).toContain(
         "row_to_json",
      );
   });

   it("a query stage would apply the range above the source — the road not taken", async () => {
      // Kept because it is the fact that makes the composed WHERE correct: a
      // predicate on the watermark filters the source's OUTPUT rows (above its
      // GROUP BY), which is the semantics the ledger records. The compiler agrees
      // with the wrap, so switching between them can never change WHICH rows a
      // delta covers — only whether the result has usable columns.
      const { materializer } = await compile(
         `${RAW}#@ persist name="t"\n${ROLLUP}`,
      );
      const sql = await materializer
         .loadQuery(
            `run: mz -> {
               where: order_date >= @2024-06-01 and order_date < @2024-07-01
               select: *
            }`,
         )
         .getSQL();
      expect(sql).toContain("GROUP BY");
      const whereAt = sql.indexOf("WHERE");
      expect(whereAt).toBeGreaterThan(sql.indexOf("GROUP BY"));
      expect(sql.slice(whereAt)).toContain("DATE '2024-06-01'");
      expect(sql.slice(whereAt)).toContain("DATE '2024-07-01'");
   });

   // ── Fact 4: persist-tag inheritance through an extend ─────────────────

   it("an extending child inherits the parent's whole persist tag, name= included", async () => {
      // Consequence: a child that declares nothing is a SECOND source pointed at
      // the parent's table (a collision the existing persist-target check
      // reports), and it inherits the parent's incremental declaration too. So
      // every gate must read the EFFECTIVE merged tag, never "did this source
      // declare it locally".
      const { sources } = await compile(
         `${RAW}#@ persist name="parent_t" refresh="incremental" watermark="order_date"\n${ROLLUP}\n
source: child is mz extend { }`,
      );
      expect(deriveAnnotationFields(sources.child)).toEqual({
         name: "parent_t",
         refresh: "incremental",
         watermark: "order_date",
      });
   });

   it("a child's own persist tag merges over the parent's, per key", async () => {
      const { sources } = await compile(
         `${RAW}#@ persist name="parent_t" refresh="incremental" watermark="order_date"\n${ROLLUP}\n
#@ persist watermark="region"
source: child is mz extend { }`,
      );
      // watermark is overridden; name= and refresh= still come from the parent.
      expect(deriveAnnotationFields(sources.child)).toEqual({
         name: "parent_t",
         refresh: "incremental",
         watermark: "region",
      });
   });

   it("tag negation (-watermark) strips an inherited key", async () => {
      const { sources } = await compile(
         `${RAW}#@ persist name="parent_t" refresh="incremental" watermark="order_date"\n${ROLLUP}\n
#@ persist refresh="full" -watermark
source: child is mz extend { }`,
      );
      const fields = deriveAnnotationFields(sources.child);
      expect(fields.refresh).toBe("full");
      expect(fields.watermark).toBeUndefined();
   });

   it("two extensions of one base read back different watermark= values", async () => {
      const { sources } = await compile(
         `${RAW}#@ persist name="parent_t" refresh="incremental" watermark="order_date"\n${ROLLUP}\n
#@ persist name="child_a_t" watermark="order_date"
source: child_a is mz extend { }

#@ persist name="child_b_t" watermark="region"
source: child_b is mz extend { }`,
      );
      expect(deriveAnnotationFields(sources.child_a).watermark).toBe(
         "order_date",
      );
      expect(deriveAnnotationFields(sources.child_b).watermark).toBe("region");
   });

   // ── Fact 5: an array-valued key vanishes from the scalar read ─────────

   it("watermark=[…] parses, is ABSENT from the scalar read, and is visible on the raw tag", async () => {
      // The array form is a plausible author mistake (`watermark=["a","b"]`).
      // deriveAnnotationFields drops it, so a gate reading only the flattened map
      // would diagnose it as "refresh=incremental with no watermark" — pointing
      // the author at a key they DID declare. Hence rule 5 reads the raw tag
      // entries to name the real problem.
      const { sources } = await compile(
         `${RAW}#@ persist name="t" refresh="incremental" watermark=["order_date","region"]\n${ROLLUP}`,
      );
      const fields = deriveAnnotationFields(sources.mz);
      expect(fields.refresh).toBe("incremental");
      expect(fields.watermark).toBeUndefined();

      const tag = sources.mz.annotations.parseAsTag("@").tag;
      expect(tag.has("watermark")).toBe(true);
      expect(tag.text("watermark")).toBeUndefined();
      expect(tag.textArray("watermark")).toEqual(["order_date", "region"]);
      // The raw entries are where the array is detectable key-by-key.
      const entries = Object.fromEntries(tag.entries());
      expect(entries.watermark).toBeDefined();
      expect(entries.watermark.text()).toBeUndefined();
      expect(entries.watermark.array()).toBeDefined();
   });

   // ── Output-schema resolution facts (P1 reads these) ───────────────────

   it("a derived dimension: on a table-backed source is NOT in the output schema", async () => {
      // It compiles, it is queryable, and it is NOT materialized — so naming it
      // as a watermark or merge key must fail resolution rather than produce DML
      // referencing a column the table does not have.
      const { sources } = await compile(
         `${RAW}#@ persist name="t"
source: mz_table is raw extend { dimension: derived is amount + 1 }`,
      );
      const names = deriveColumns(sources.mz_table).map((c) => c.name);
      expect(names).toContain("amount");
      expect(names).not.toContain("derived");
   });

   it("an aggregate's output column is INDISTINGUISHABLE from a dimension on the output schema", async () => {
      // isCalculation() is false for an aggregate-produced column once the query
      // is a source, so the "is this name an aggregate?" gate cannot be answered
      // from the output schema. It has to read the QUERY DEFINITION's field
      // kinds, which is what this fact pins.
      const { sources } = await compile(
         `${RAW}#@ persist name="t"
source: mz is raw -> {
  group_by: order_date
  aggregate: revenue is amount.sum()
  calculate: prev is lag(revenue)
}`,
      );
      for (const field of sources.mz._explore.intrinsicFields) {
         expect(field.isAtomicField()).toBe(true);
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect((field as any).isCalculation()).toBe(false);
      }

      // …but the query definition does distinguish them: a dimension is a
      // `fieldref`, an aggregate carries expressionType "aggregate", and a
      // calculate: (window) field carries an *_analytic expressionType.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const def = sources.mz._sourceDef as any;
      expect(def.type).toBe("query_source");
      const byKind: Record<string, string[]> = {};
      for (const segment of def.query.pipeline) {
         for (const field of segment.queryFields ?? []) {
            const kind =
               field.type === "fieldref"
                  ? "fieldref"
                  : String(field.expressionType);
            (byKind[kind] ??= []).push(
               field.type === "fieldref" ? field.path.join(".") : field.name,
            );
         }
      }
      expect(byKind.fieldref).toEqual(["order_date"]);
      expect(byKind.aggregate).toEqual(["revenue"]);
      expect(byKind.aggregate_analytic).toEqual(["prev"]);
   });

   it("struct and array columns are not atomic, so they never reach the output schema", async () => {
      // Two consequences. (a) Naming one as a watermark fails name resolution
      // (rule 8) before the orderable-type gate (rule 10) ever sees it.
      // (b) The seed CTAS DOES create such a column physically, which is why the
      // delta's MERGE column set is probed from the TARGET's catalog rather than
      // derived from the model's atomic view — deriving it here would write NULLs
      // into the columns below.
      const { sources } = await compile(
         `${RAW}#@ persist name="t"
source: mz is raw extend { }`,
      );
      const names = deriveColumns(sources.mz).map((c) => c.name);
      expect(names).toEqual(["amount", "order_date", "ts", "region", "flag"]);
      const nonAtomic = sources.mz._explore.intrinsicFields
         .filter((f) => !f.isAtomicField())
         .map((f) => f.name);
      expect(nonAtomic).toEqual(["tags", "rec"]);
   });

   it("the output schema reports the watermark's Malloy type verbatim", async () => {
      // The bounds are rendered as Malloy literals for THIS type (see the next
      // fact), and the orderable-type gate reads it.
      const { sources } = await compile(
         `${RAW}#@ persist name="t"
source: mz is raw -> { group_by: order_date, ts, region, amount, flag }`,
      );
      expect(deriveColumns(sources.mz)).toEqual([
         { name: "order_date", type: "date" },
         { name: "ts", type: "timestamp" },
         { name: "region", type: "string" },
         { name: "amount", type: "number" },
         { name: "flag", type: "boolean" },
      ]);
   });

   // ── Literal rendering must match the watermark's type ─────────────────

   it("a date column and a timestamp literal are not interchangeable", () => {
      // Why renderSqlBound is type-directed rather than emitting one temporal
      // spelling: the compiler refuses the cross-type comparison outright, and the
      // strict dialects on the allowlist refuse it in SQL too (BigQuery has no
      // implicit DATE/TIMESTAMP coercion in a comparison). The delta composes its
      // predicate in SQL now, so this is the compiler standing in for warehouses
      // this suite cannot reach — a renderer that ignored the column's type would
      // fail every date-watermarked source, at build time.
      const dateBound = renderSqlBound({
         malloyType: "date",
         value: "2024-06-01",
      });
      const timestampBound = renderSqlBound({
         malloyType: "timestamp",
         value: "2024-06-01 00:00:00",
      });
      expect(dateBound).toBe("DATE '2024-06-01'");
      expect(timestampBound).toBe("TIMESTAMP '2024-06-01 00:00:00'");
   });

   it("a date watermark compared to a TIMESTAMP literal fails to compile", async () => {
      // The compiler's own refusal, pinned as the upstream evidence for the rule
      // above.
      const { materializer } = await compile(
         `${RAW}#@ persist name="t"\n${ROLLUP}`,
      );
      await expect(
         materializer
            .loadQuery(
               `run: mz -> { where: order_date >= @2024-06-01 00:00:00; select: * }`,
            )
            .getSQL(),
      ).rejects.toThrow(/compare a date to a timestamp/i);
   });

   // ── What the delta WRITES: no row cap, and a shape that can diverge ────

   it("getSQL() carries NO row limit", async () => {
      // The delta wraps this SQL and inlines it into an INSERT ... SELECT. A
      // default row cap in it would not fail anything — it would silently write a
      // truncated range into a serving table and then record the full range as
      // covered. Nothing in the publisher can detect that afterwards, so it is
      // pinned here. (The seed's CTAS rests on the same fact.)
      const { sources } = await compile(`${RAW}#@ persist name="t"\n${ROLLUP}`);
      expect(sources.mz.getSQL().toUpperCase()).not.toContain("LIMIT");
   });

   it("a rename makes getSQL()'s columns and the source's SCHEMA disagree", async () => {
      // The delta and the seed both run getSQL(), so they always agree on rows and
      // on columns. What can still disagree is the column list the DML NAMES —
      // deriveColumns, the source's compiled output schema — because a rename
      // leaves the underlying name in the SQL (and therefore in the table the seed
      // built) while the schema reports the renamed one. Hence shapeMismatch runs
      // before any DML, and a source like this is rebuilt in full rather than
      // deltaed with a column list the table does not have.
      const { sources } = await compile(
         `${RAW}#@ persist name="t"
source: mz is raw extend {
  rename: order_day is order_date
  except: flag, tags, rec
}`,
      );
      const built = await duckdb.runSQL(sources.mz.getSQL());
      expect(Object.keys(built.rows[0])).toContain("order_date");
      expect(Object.keys(built.rows[0])).not.toContain("order_day");
      // ...while the schema the DML would name reports the renamed one.
      const schema = deriveColumns(sources.mz).map((c) => String(c.name));
      expect(schema).toContain("order_day");
      expect(schema).not.toContain("order_date");
   });

   it("a query source's SQL and its schema agree on column names", async () => {
      // The common case, and why the shape check is a fallback rather than a
      // blanket refusal: an aggregating source's schema columns ARE its SQL's
      // columns, so the DML can name them and a delta writes the seed's shape.
      const { sources } = await compile(`${RAW}#@ persist name="t"\n${ROLLUP}`);
      const built = await duckdb.runSQL(sources.mz.getSQL());
      expect(Object.keys(built.rows[0])).toEqual(
         deriveColumns(sources.mz).map((c) => String(c.name)),
      );
   });
});
