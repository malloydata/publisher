// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `sql()` source that embeds (`%{ ... }`) a gated source is refused at load
 * (`./sql_embed_gates.ts`): the embedded read is compiled into SQL text where
 * no gate is enforced.
 *
 * The first block pins the Malloy IR the check reads, the way
 * `malloy_annotation_invariants.spec.ts` pins annotation copying: if a Malloy
 * upgrade moves embeds out of `selectSegments` (or changes a segment's shape),
 * a test here goes red instead of the check silently finding nothing.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelDef,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { ModelCompilationError } from "../errors";
import {
   countFor,
   withAuthorizeModel,
} from "../test_helpers/authorize_fixture";
import {
   assertNoSqlEmbedsOfGatedSources,
   findSqlEmbedsOfGatedSources,
} from "./sql_embed_gates";

const ROOT = "file:///sql-embed-gates/";
const ROWS = `duckdb.sql("select * from (values (1, 'o1'), (2, 'o2')) as t(id, org_id)")`;

/** A DuckDB that compiles as Postgres: another connection's `sql()` spelling. */
class PostgresDialectDuckDB extends DuckDBConnection {
   override get dialectName(): string {
      return "postgres";
   }
}

const BASE = `##! experimental.givens
given: ROLES :: string[]
given: ORGS :: string[]

#(authorize) 'admin' in $ROLES
source: locked is ${ROWS} extend { primary_key: id }

#(access_filter) org_id in $ORGS
source: scoped is ${ROWS}

source: open is ${ROWS}

source: inherits_lock is locked extend { }

source: derived_from_lock is locked -> { select: * }
`;

let duckdb: DuckDBConnection;
let pg: PostgresDialectDuckDB;

beforeAll(() => {
   duckdb = new DuckDBConnection("duckdb", ":memory:");
   pg = new PostgresDialectDuckDB("pg", ":memory:");
});
afterAll(async () => {
   await duckdb.close();
   await pg.close();
});

/** Compile `entry` over `files` (plus `base.malloy`), no load checks. */
async function compile(
   entryText: string,
   extraFiles: Record<string, string> = {},
): Promise<ModelDef> {
   const files: Record<string, string> = {
      "base.malloy": BASE,
      ...extraFiles,
      "entry.malloy": `##! experimental.composite_sources\nimport "base.malloy"\n${entryText}`,
   };
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(
         new Map(Object.entries(files).map(([k, v]) => [`${ROOT}${k}`, v])),
      ),
      connections: new FixedConnectionMap(
         new Map<string, DuckDBConnection>([
            ["duckdb", duckdb],
            ["pg", pg],
         ]),
         "duckdb",
      ),
   });
   const model = await runtime
      .loadModel(new URL(`${ROOT}entry.malloy`), {
         importBaseURL: new URL(ROOT),
      })
      .getModel();
   return model._modelDef;
}

/** `embedding › embedded [routes]` for each finding, sorted. */
async function findings(entryText: string, extra?: Record<string, string>) {
   const modelDef = await compile(entryText, extra);
   return findSqlEmbedsOfGatedSources(modelDef)
      .map(
         (f) =>
            `${f.embeddingSource}${f.via ? ` (via ${f.via})` : ""} › ${f.embeddedSource} [${f.unreadable ? "unreadable" : f.routes.join(",")}]`,
      )
      .sort();
}

type Seg = Record<string, unknown>;
const segmentsOf = (modelDef: ModelDef, name: string): Seg[] =>
   (modelDef.contents[name] as unknown as { selectSegments: Seg[] })
      .selectSegments;
const labelOf = (s: unknown) =>
   (s as { as?: string; name: string }).as ?? (s as { name: string }).name;

describe("Malloy IR the sql() embed check reads", () => {
   it("an embedded query is a `query` segment whose structRef is the source def, between `sql` text segments", async () => {
      const modelDef = await compile(
         `source: e is duckdb.sql("""select * from (%{ locked -> { select: * } })""")`,
      );
      expect(modelDef.contents.e.type).toBe("sql_select");
      const segments = segmentsOf(modelDef, "e");
      expect(segments.map((s) => ("sql" in s ? "sql" : s.type))).toEqual([
         "sql",
         "query",
         "sql",
      ]);
      expect(typeof segments[1].structRef).toBe("object");
      expect(labelOf(segments[1].structRef)).toBe("locked");
   });

   it("an embedded source is the source def itself as the segment", async () => {
      const modelDef = await compile(
         `source: e is duckdb.sql("""select * from %{ inherits_lock }""")`,
      );
      const [, segment] = segmentsOf(modelDef, "e");
      expect(segment.type).toBe("sql_select");
      expect(labelOf(segment)).toBe("inherits_lock");
   });

   it("an embedded named query is inlined as a query segment over its source", async () => {
      const modelDef = await compile(
         `query: q is locked -> { select: * }
source: e is duckdb.sql("""select * from (%{ q })""")`,
      );
      const [, segment] = segmentsOf(modelDef, "e");
      expect(segment.type).toBe("query");
      expect(labelOf(segment.structRef)).toBe("locked");
   });

   it("a nested embed is a sql_select inlined inside the outer one's segment", async () => {
      const modelDef = await compile(
         `source: inner_sql is duckdb.sql("""select * from (%{ locked -> { select: * } })""")
source: outer is duckdb.sql("""select * from (%{ inner_sql -> { select: * } })""")`,
      );
      const [, segment] = segmentsOf(modelDef, "outer");
      const inner = segment.structRef as Seg;
      expect(inner.type).toBe("sql_select");
      expect(Array.isArray(inner.selectSegments)).toBe(true);
   });

   it("a query over a composite carries the resolved member on compositeResolvedSourceDef", async () => {
      const modelDef = await compile(
         `source: comp is compose(locked, open)
source: e is duckdb.sql("""select * from (%{ comp -> { select: id } })""")`,
      );
      const [, segment] = segmentsOf(modelDef, "e");
      expect((segment.structRef as Seg).type).toBe("composite");
      expect(labelOf(segment.compositeResolvedSourceDef)).toBe("locked");
   });

   it("a table source cannot be embedded at all (only sql_select / query_source)", async () => {
      await expect(
         compile(
            `source: tbl is duckdb.table("information_schema.tables")
source: e is duckdb.sql("""select * from %{ tbl }""")`,
         ),
      ).rejects.toThrow(/not persistable/);
   });
});

describe("findSqlEmbedsOfGatedSources", () => {
   it("finds an embedded query over a source's own #(authorize) and #(access_filter)", async () => {
      expect(
         await findings(
            `source: a is duckdb.sql("""select * from (%{ locked -> { select: * } })""")
source: b is duckdb.sql("""select * from (%{ scoped -> { select: * } })""")`,
         ),
      ).toEqual(["a › locked [authorize]", "b › scoped [access_filter]"]);
   });

   it("finds an embed of a source that inherits the gate, by extend or by derivation", async () => {
      expect(
         await findings(
            `source: a is duckdb.sql("""select * from %{ inherits_lock }""")
source: b is duckdb.sql("""select * from %{ derived_from_lock }""")
source: c is duckdb.sql("""select * from (%{ inherits_lock -> { select: * } })""")`,
         ),
      ).toEqual([
         "a › inherits_lock [authorize]",
         "b › derived_from_lock [authorize]",
         "c › inherits_lock [authorize]",
      ]);
   });

   it("finds a nested embed, through every source built on it", async () => {
      expect(
         await findings(
            `source: inner_sql is duckdb.sql("""select * from (%{ locked -> { select: * } })""")
source: outer is duckdb.sql("""select * from (%{ inner_sql -> { select: * } })""")
source: ext_inner is inner_sql extend { }
source: over_inner is inner_sql -> { select: * }`,
         ),
      ).toEqual([
         "ext_inner › locked [authorize]",
         "inner_sql › locked [authorize]",
         "outer (via inner_sql) › locked [authorize]",
         "over_inner (via inner_sql) › locked [authorize]",
      ]);
   });

   it("finds an embed through an import, a named query, a composite member, and an inline join", async () => {
      expect(
         await findings(
            `import "embeds.malloy"
query: q is locked -> { select: * }
source: via_query is duckdb.sql("""select * from (%{ q })""")
source: comp is compose(locked, open)
source: via_comp is duckdb.sql("""select * from (%{ comp -> { select: id } })""")
source: joins_sql is open extend {
   join_one: j is duckdb.sql("""select * from (%{ locked -> { select: * } })""") on j.id = id
}`,
            {
               "embeds.malloy": `import "base.malloy"
source: imported_embed is duckdb.sql("""select * from (%{ scoped -> { select: * } })""")
`,
            },
         ),
      ).toEqual([
         "imported_embed › scoped [access_filter]",
         "joins_sql (via j) › locked [authorize]",
         "via_comp › locked [authorize]",
         "via_query › locked [authorize]",
      ]);
   });

   it("finds an embed in another connection's sql() spelling", async () => {
      expect(
         await findings(
            `#(authorize) 'admin' in $ROLES
source: pg_locked is pg.sql("select 1 as id")
source: pg_inherits is pg_locked extend { }
source: on_pg is pg.sql("""select * from (%{ pg_locked -> { select: * } })""")
source: on_pg_source is pg.sql("""select * from %{ pg_inherits }""")
query: inline_pg is pg.sql("""select * from (%{ pg_locked -> { select: * } })""") -> { select: * }`,
         ),
      ).toEqual([
         "inline_pg (via <inline sql()>) › pg_locked [authorize]",
         "on_pg › pg_locked [authorize]",
         "on_pg_source › pg_inherits [authorize]",
      ]);
   });

   it("leaves an embed of an ungated source, and a plain sql(), alone", async () => {
      expect(
         await findings(
            `source: a is duckdb.sql("""select * from (%{ open -> { select: * } })""")
source: b is duckdb.sql("select 1 as x")
source: c is locked extend { }`,
         ),
      ).toEqual([]);
   });

   it("fails closed on a structRef that names nothing in the model", async () => {
      const modelDef = await compile(
         `source: e is duckdb.sql("""select * from (%{ open -> { select: * } })""")`,
      );
      segmentsOf(modelDef, "e")[1].structRef = "no_such_source";
      expect(findSqlEmbedsOfGatedSources(modelDef)).toEqual([
         {
            embeddingSource: "e",
            via: undefined,
            embeddedSource: "no_such_source",
            routes: [],
            unreadable: "its source reference does not resolve in the model",
         },
      ]);
      expect(() => assertNoSqlEmbedsOfGatedSources(modelDef)).toThrow(
         /"e" embeds "no_such_source", and its source reference does not resolve/,
      );
   });

   it("fails closed on a segment shape it does not read", async () => {
      const modelDef = await compile(
         `source: e is duckdb.sql("""select * from (%{ open -> { select: * } })""")`,
      );
      segmentsOf(modelDef, "e")[1] = { type: "something_new", name: "x" };
      expect(() => assertNoSqlEmbedsOfGatedSources(modelDef)).toThrow(
         ModelCompilationError,
      );
   });
});

describe("the load refuses a sql() embedding a gated source", () => {
   it("names the embedding source, the gated source, and the fix", async () => {
      await withAuthorizeModel(
         {
            "base.malloy": BASE,
            "m.malloy": `import "base.malloy"
source: reads_locked is duckdb.sql("""select * from (%{ locked -> { select: * } })""")
`,
         },
         async ({ model, error }) => {
            expect(error).toBeInstanceOf(ModelCompilationError);
            expect(error?.message).toContain(
               `"reads_locked" embeds "locked", which carries #(authorize)`,
            );
            expect(error?.message).toContain(
               "Query the gated source through Malloy instead",
            );
            expect(error?.message).toContain("or remove the gate");
            expect(model.getSources()).toBeUndefined();
         },
         "m.malloy",
      );
   });

   it("still loads an embed of an ungated source, and gates still enforce", async () => {
      await withAuthorizeModel(
         {
            "base.malloy": BASE,
            "m.malloy": `import "base.malloy"
source: reads_open is duckdb.sql("""select * from (%{ open -> { select: * } })""")
`,
         },
         async ({ model, error }) => {
            expect(error).toBeUndefined();
            expect(await countFor(model, "reads_open")).toBe(2);
         },
         "m.malloy",
      );
   });
});
