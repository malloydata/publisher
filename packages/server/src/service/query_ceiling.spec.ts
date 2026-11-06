// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(carries)` is a ceiling on what a source exposes, and a caller's own query
 * can extend a source past what its author declared. So the ceiling is also
 * checked per query, on the fields the compiled query reads.
 */
import { describe, expect, it } from "bun:test";
import { BadRequestError } from "../errors";
import { withAuthorizeModel } from "../test_helpers/authorize_fixture";
import { unrecordedAudit, type QueryAudit } from "./access_record";
import { ceilingRefusal, modelDeclaresCarries } from "./classification";
import type { Model } from "./model";

const MODEL = `##! experimental { access_modifiers composite_sources }
source: people is duckdb.table("child") include { internal: * } extend {
   primary_key: id
   #(classification) "phi.direct_identifier"
   dimension: ssn is name
   #(classification) "none"
   dimension: pid is id
   #(classification) "none"
   dimension: key is id
   #(classification) "none"
   measure: pc is count()
}
#(carries) ["none"]
source: s is duckdb.table("child") include { internal: * } extend {
   join_one: p is people on id = p.id
   #(classification) "none"
   dimension: sid is id
   #(classification) "none"
   measure: c is count()
   view: v is { aggregate: c }
}
source: hub is duckdb.table("child") include { internal: * } extend {
   join_one: r is s on id = r.sid
   #(classification) "none"
   measure: hc is count()
}
#(carries) ["phi.direct_identifier", "none"]
source: wide_q is s -> { group_by: x is p.ssn }
#(carries) ["none"]
source: ok_q is s -> { group_by: sid }
source: leak_q is hub -> { group_by: y is r.p.ssn }
source: tidy_q is hub -> { group_by: y is r.sid }
source: deep_q is leak_q -> { group_by: z is y }
source: deep_ok is tidy_q -> { group_by: z is y }
source: ca is duckdb.table("child") include { internal: * } extend {
   #(classification) "none"
   dimension: k is id
   #(classification) "none"
   measure: n is count()
}
source: cb is duckdb.table("child") include { internal: * } extend {
   #(classification) "none"
   dimension: k is id
   dimension: only_b is name
   #(classification) "none"
   measure: n is count()
}
#(carries) ["none"]
source: comp is compose(ca, cb)
#(carries) ["none"]
source: cmem is duckdb.table("child") include { internal: * } extend {
   #(classification) "none"
   dimension: k is id
   dimension: only_m is name
}
source: comp_m is compose(ca, cmem)
source: comp_q is compose(ca, cb) -> { group_by: only_b }
source: open_s is duckdb.table("child") include { internal: * } extend {
   join_one: p is people on id = p.id
   #(classification) "none"
   measure: c is count()
}
#(carries) ["none"]
source: rel is duckdb.table("child") include { internal: * } extend {
   join_one: p is people on id = p.id
   #(classification) "none"
   internal dimension: copy is p.ssn
}
source: rec_q is s extend { dimension: r is { v is p.ssn } } -> { select: r }
source: nest_q is s -> { group_by: sid nest: n is { group_by: p.ssn } }
`;

async function run(
   model: Model,
   query: string,
   audit: QueryAudit = unrecordedAudit(),
   bypassAuthorize = false,
) {
   return model.getQueryResults(
      audit,
      undefined,
      undefined,
      query,
      {},
      true,
      {},
      undefined,
      undefined,
      "full",
      bypassAuthorize,
   );
}

const DENIED: Array<[string, string, string]> = [
   [
      "an extend field that reads a classified join field",
      "run: s extend { dimension: x is p.ssn } -> { group_by: x }",
      "x, which is classified phi.direct_identifier",
   ],
   [
      "a selected extend record whose child reads a classified join field",
      "run: s extend { dimension: r is { v is p.ssn } } -> { select: r }",
      "r, which is classified phi.direct_identifier",
   ],
   [
      "a selected extend array over a classified join field",
      "run: s extend { dimension: a is [p.ssn] } -> { select: a }",
      "a, which is classified phi.direct_identifier",
   ],
   [
      "a grouped extend record over a classified join field",
      "run: s extend { dimension: r is { v is p.ssn } } -> { group_by: r }",
      "r, which is classified phi.direct_identifier",
   ],
   [
      "a grouped extend array over a classified join field",
      "run: s extend { dimension: a is [p.ssn] } -> { group_by: a }",
      "a, which is classified phi.direct_identifier",
   ],
   [
      "a nested extend record over a classified join field",
      "run: s extend { dimension: r is { v is p.ssn } } -> { aggregate: c nest: z is { group_by: r } }",
      "r, which is classified phi.direct_identifier",
   ],
   [
      "a nested extend array over a classified join field",
      "run: s extend { dimension: a is [p.ssn] } -> { aggregate: c nest: z is { group_by: a } }",
      "a, which is classified phi.direct_identifier",
   ],
   [
      "a record inside a record over a classified join field",
      "run: s extend { dimension: r is { b is { v is p.ssn } } } -> { select: r }",
      "r, which is classified phi.direct_identifier",
   ],
   [
      "an array of records over a classified join field",
      "run: s extend { dimension: a is [{ v is p.ssn }] } -> { select: a }",
      "a, which is classified phi.direct_identifier",
   ],
   [
      "a record over a raw internal column",
      "run: s extend { dimension: r is { v is name } } -> { select: r }",
      "r, which has no #(classification)",
   ],
   [
      "a record a stage's own extend declares",
      "run: s -> { extend: { dimension: r is { v is p.ssn } } select: r }",
      "r, which is classified phi.direct_identifier",
   ],
   [
      "a record a later stage's extend declares over a join it brings in",
      "run: s -> { group_by: sid } -> { extend: { join_cross: self is s dimension: z is { v is self.p.ssn } } select: z }",
      "z, which is classified phi.direct_identifier",
   ],
   [
      "a record over an internal relabel declared lower than the field it copies",
      "run: rel extend { dimension: r is { v is copy } } -> { select: r }",
      "r, which is classified phi.direct_identifier",
   ],
   [
      "a record a query source returns, read whole",
      "run: rec_q -> { select: r }",
      "r, which is classified phi.direct_identifier",
   ],
   [
      "a nest a query source returns, read whole",
      "run: nest_q -> { select: n }",
      "n, which is classified phi.direct_identifier",
   ],
   [
      "a raw internal column read through extend",
      "run: s extend { dimension: x is name } -> { group_by: x }",
      "no #(classification)",
   ],
   [
      "a join the caller writes whose on reads a classified field",
      'run: s extend { join_one: q is people on sid = q.key and p.ssn = "north" } -> { aggregate: q.pc }',
      "p.ssn, which is classified phi.direct_identifier",
   ],
   [
      "a source the caller declares over a carried one",
      "source: mine is s extend { dimension: x is p.ssn }\nrun: mine -> { group_by: x }",
      "x, which is classified phi.direct_identifier",
   ],
   [
      "a filter on a classified field",
      'run: s extend { dimension: x is p.ssn } -> { where: x = "north" aggregate: c }',
      "x, which is classified phi.direct_identifier",
   ],
   [
      "a nest that reads a classified field",
      "run: s extend { dimension: x is p.ssn } -> { aggregate: c nest: z is { group_by: x } }",
      "x, which is classified phi.direct_identifier",
   ],
   [
      "a join the caller writes whose on reads an internal column",
      'run: s extend { join_one: q is people on sid = q.key and name = "north" } -> { aggregate: q.pc }',
      "name, which has no #(classification)",
   ],
   [
      "a filter on an internal column read through extend",
      'run: s extend { dimension: x is name } -> { where: x = "north" aggregate: c }',
      "no #(classification)",
   ],
   [
      "a nest that reads an internal column through extend",
      "run: s extend { dimension: x is name } -> { aggregate: c nest: z is { group_by: x } }",
      "no #(classification)",
   ],
   [
      "an index over an internal column",
      "run: s extend { dimension: x is name } -> { index: x }",
      "cannot see completely",
   ],
   [
      "a join a later stage extends in, which reads an internal column",
      "run: s -> { group_by: sid } -> { extend: { join_cross: self is s extend { dimension: x is name } } group_by: self.x }",
      "no #(classification)",
   ],
   [
      "a later-stage dimension over a join the same extend brings in",
      "run: s -> { group_by: sid } -> { extend: { join_cross: self is s extend { dimension: x is name } dimension: z is self.x } group_by: z }",
      "no #(classification)",
   ],
   [
      "a later-stage dimension over a join that shares a name with an earlier output",
      "run: s -> { group_by: self is sid } -> { extend: { join_cross: other is s extend { dimension: x is name } dimension: z is other.x } group_by: z }",
      "no #(classification)",
   ],
   [
      "a nest over a join a later stage extends in",
      "run: s -> { group_by: sid } -> { extend: { join_cross: self is s extend { dimension: x is name } } nest: n is { group_by: self.x } }",
      "no #(classification)",
   ],
   [
      "a composite member that carries a ceiling and reads an internal column",
      "run: comp_m -> { group_by: only_m }",
      "source cmem carries [none]",
   ],
   [
      "an index over a classified field",
      "run: s extend { dimension: x is p.ssn } -> { index: x }",
      "cannot see completely",
   ],
   [
      "a dotted read through a join whose name a group_by output also takes",
      'run: s -> { group_by: p is sid where: p.ssn = "x" }',
      "p.ssn, which is classified phi.direct_identifier",
   ],
   [
      "a dotted read through a join whose name an aggregate output also takes",
      'run: s -> { aggregate: p is count() where: p.ssn = "x" }',
      "p.ssn, which is classified phi.direct_identifier",
   ],
];

describe("#(carries) is checked per query", () => {
   for (const [name, query, message] of DENIED) {
      it(`refuses ${name}`, async () => {
         await withAuthorizeModel(MODEL, async ({ model }) => {
            const err = await run(model, query).catch((e) => e);
            expect(err).toBeInstanceOf(BadRequestError);
            expect((err as Error).message).toContain("carries [none]");
            expect((err as Error).message).toContain(message);
         });
      });
   }

   it("applies the ceiling of a carried source reached through a join from an open one", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         for (const query of [
            "run: hub -> { group_by: r.p.ssn }",
            "run: open_s extend { join_one: r is s on id = r.sid } -> { group_by: r.p.ssn }",
         ]) {
            const err = await run(model, query).catch((e) => e);
            expect(err).toBeInstanceOf(BadRequestError);
            expect((err as Error).message).toContain("source r carries [none]");
            expect((err as Error).message).toContain("r.p.ssn");
         }
         for (const query of [
            "run: hub -> { aggregate: r.c }",
            "run: open_s extend { join_one: r is s on id = r.sid } -> { aggregate: r.c }",
         ]) {
            expect((await run(model, query)).compactResult.length).toBe(1);
         }
      });
   });

   it("follows a read to where it comes from: through a caller extend, and out of a query source's base", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         for (const query of [
            "run: open_s extend { join_one: r is s on id = r.sid\n dimension: x is r.p.ssn } -> { group_by: x }",
            "run: leak_q -> { group_by: y }",
            "run: deep_q -> { group_by: z }",
         ]) {
            const err = await run(model, query).catch((e) => e);
            expect(err).toBeInstanceOf(BadRequestError);
            expect((err as Error).message).toContain("carries [none]");
         }
         for (const query of [
            "run: open_s extend { join_one: r is s on id = r.sid\n dimension: x is r.sid } -> { group_by: x }",
            "run: tidy_q -> { group_by: y }",
            "run: deep_ok -> { group_by: z }",
         ]) {
            expect(
               (await run(model, query)).compactResult.length,
            ).toBeGreaterThan(0);
         }
      });
   });

   it("refuses a shape it cannot read once the model declares a ceiling, and serves a traceable read of an open source", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const err = await run(model, "run: hub -> { index: r.sid }").catch(
            (e) => e,
         );
         expect(err).toBeInstanceOf(BadRequestError);
         expect((err as Error).message).toContain("cannot see completely");
         expect(
            (await run(model, "run: open_s -> { group_by: p.pid }"))
               .compactResult.length,
         ).toBeGreaterThan(0);
      });
   });

   it("holds a query source to the ceiling of the source it runs on", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const err = await run(model, "run: wide_q -> { group_by: x }").catch(
            (e) => e,
         );
         expect(err).toBeInstanceOf(BadRequestError);
         expect((err as Error).message).toContain("source s carries [none]");
         expect(
            (await run(model, "run: ok_q -> { group_by: sid }")).compactResult
               .length,
         ).toBeGreaterThan(0);
      });
   });

   it("serves the twins: a later-stage join within the list, output-space names, a composite within its ceiling", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         for (const query of [
            "run: s -> { group_by: sid } -> { extend: { join_cross: self is s extend { dimension: x is sid } } group_by: self.x }",
            "run: s -> { group_by: sid } -> { extend: { dimension: y is sid } group_by: y }",
            "run: s -> { group_by: sid aggregate: n is count() order_by: n desc }",
            "run: s -> { group_by: sid aggregate: c calculate: prev is lag(c) }",
            "run: s -> { group_by: sid aggregate: n is count() calculate: prev is lag(n) }",
            "run: s -> { group_by: sid aggregate: n is count() order_by: n desc }",
            "run: s -> { group_by: p is sid order_by: p }",
            "run: s -> { group_by: p is sid where: p.pid is not null }",
            "run: comp -> { group_by: k }",
            "run: comp_m -> { group_by: k }",
            "run: comp_q -> { group_by: only_b }",
         ]) {
            expect(
               (await run(model, query)).compactResult.length,
            ).toBeGreaterThan(0);
         }
      });
   });

   it("refuses a composite member's unclassified field under the composite's ceiling", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const err = await run(
            model,
            "run: comp -> { group_by: only_b }",
         ).catch((e) => e);
         expect(err).toBeInstanceOf(BadRequestError);
         expect((err as Error).message).toContain("only_b");
      });
   });

   it("refuses a caller relabel and a caller-written ceiling", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         for (const note of [
            '#(classification) "none"',
            '#(carries) ["phi.direct_identifier"]',
         ]) {
            const err = await run(
               model,
               `run: s extend {\n${note}\n dimension: x is concat(p.ssn, '') } -> { group_by: x }`,
            ).catch((e) => e);
            expect(err).toBeInstanceOf(BadRequestError);
            expect((err as Error).message).toContain("not permitted");
         }
      });
   });

   it("records a refusal with what it read", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const audit = unrecordedAudit();
         await run(
            model,
            "run: s extend { dimension: x is p.ssn } -> { group_by: x }",
            audit,
         ).catch(() => undefined);
         expect(audit.summary?.source).toBe("s");
         expect(audit.summary?.fieldsRead.map((f) => f.path)).toContain(
            "p.ssn",
         );
      });
   });

   it("serves what stays on the list", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         for (const query of [
            "run: s -> { aggregate: c }",
            "run: s -> { aggregate: n is count() }",
            "run: s -> { group_by: p.pid aggregate: p.pc }",
            "run: s -> v",
            "run: s extend { dimension: k is p.pid } -> { group_by: k }",
            "run: s extend { join_one: q is people on sid = q.key } -> { aggregate: q.pc }",
            "run: s -> { aggregate: c nest: z is { group_by: p.pid } }",
            "source: mine is s extend { dimension: k is p.pid }\nrun: mine -> { group_by: k }",
         ]) {
            const audit = unrecordedAudit();
            const result = await run(model, query, audit);
            expect(result.compactResult.length).toBeGreaterThan(0);
         }
      });
   });

   it("costs a source without #(carries) nothing, and the data-management bypass skips it", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const query =
            "run: open_s extend { dimension: x is p.ssn } -> { group_by: x }";
         expect((await run(model, query)).compactResult.length).toBeGreaterThan(
            0,
         );
         const carried =
            "run: s extend { dimension: x is p.ssn } -> { group_by: x }";
         expect(
            (await run(model, carried, unrecordedAudit(), true)).compactResult
               .length,
         ).toBeGreaterThan(0);
      });
   });

   it("holds a record on a carried source joined into an open one to the joined source's ceiling", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         for (const query of [
            "run: open_s extend { join_one: rr is s extend { dimension: r is { v is p.ssn } } on id = rr.sid } -> { select: rr.r }",
            "run: open_s extend { join_one: rr is s extend { dimension: a is [p.ssn] } on id = rr.sid } -> { select: rr.a }",
         ]) {
            const err = await run(model, query).catch((e) => e);
            expect(err).toBeInstanceOf(BadRequestError);
            expect((err as Error).message).toContain(
               "source rr carries [none]",
            );
            expect((err as Error).message).toContain(
               "which is classified phi.direct_identifier",
            );
         }
      });
   });

   it("refuses a record whose child it cannot follow", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const err = await run(
            model,
            "run: s -> { aggregate: c nest: n is { extend: { dimension: r is { v is p.ssn } } select: r } }",
         ).catch((e) => e);
         expect(err).toBeInstanceOf(BadRequestError);
         expect((err as Error).message).toContain(
            "source s carries [none]; this query reads r, which Publisher cannot see completely",
         );
      });
   });

   it("twin: serves a record or array built from fields on the list, a real join named like one, and a record on an open source", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         for (const query of [
            "run: s extend { dimension: r is { v is sid } } -> { select: r }",
            "run: s extend { dimension: a is [sid] } -> { select: a }",
            "run: s extend { dimension: r is { b is { v is sid, w is p.pid } } } -> { select: r }",
            "run: s extend { dimension: a is [{ v is sid }] } -> { select: a }",
            "run: s extend { dimension: r is { v is sid } } -> { group_by: r }",
            "run: s extend { dimension: r is { v is sid } } -> { aggregate: c nest: z is { group_by: r } }",
            "run: s extend { join_one: r is people on sid = r.key } -> { group_by: r.pid }",
            "run: open_s extend { dimension: r is { v is p.ssn } } -> { select: r }",
            "run: open_s extend { join_one: rr is s extend { dimension: r is { v is sid } } on id = rr.sid } -> { select: rr.r }",
         ]) {
            expect(
               (await run(model, query)).compactResult.length,
            ).toBeGreaterThan(0);
         }
      });
   });

   it("records a record's reads in fieldsRead, refused or served", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const refused = unrecordedAudit();
         await run(
            model,
            "run: s extend { dimension: r is { v is p.ssn } } -> { select: r }",
            refused,
         ).catch(() => undefined);
         expect(refused.summary?.fieldsRead.map((f) => f.path)).toEqual([
            "p.ssn",
            "r",
         ]);
         expect(refused.summary?.fieldsReadComplete).toBe(true);
         const served = unrecordedAudit();
         await run(
            model,
            "run: s extend { dimension: a is [sid] } -> { select: a }",
            served,
         );
         expect(served.summary?.fieldsRead.map((f) => f.path)).toEqual([
            "a",
            "sid",
         ]);
         expect(served.summary?.fieldsReadComplete).toBe(true);
      });
   });
});

describe("a ceiling the model reaches only through an import", () => {
   const BASE = `##! experimental { access_modifiers composite_sources }
source: people is duckdb.table("child") include { internal: * } extend {
   primary_key: id
   #(classification) "phi.direct_identifier"
   dimension: ssn is name
}
#(carries) ["none"]
source: s is duckdb.table("child") include { internal: * } extend {
   join_one: p is people on id = p.id
   #(classification) "none"
   dimension: sid is id
   #(classification) "none"
   measure: c is count()
}
source: ca is duckdb.table("child") include { internal: * } extend {
   #(classification) "none"
   dimension: k is id
}
#(carries) ["none"]
source: cmem is duckdb.table("child") include { internal: * } extend {
   #(classification) "none"
   dimension: k is id
   dimension: only_m is name
}
`;
   const FILES = {
      "base.malloy": BASE,
      "hub.malloy": `##! experimental { access_modifiers composite_sources }
import { s, ca, cmem } from "base.malloy"
source: hub is duckdb.table("child") include { internal: * } extend {
   join_one: r is s on id = r.sid
   #(classification) "none"
   measure: hc is count()
}
source: comp_m is compose(ca, cmem)
`,
      "m.malloy": `##! experimental { access_modifiers composite_sources }
import { hub, comp_m } from "hub.malloy"
`,
   };

   it("refuses a read past the ceiling of a carried source joined or composed in another file", async () => {
      await withAuthorizeModel(FILES, async ({ model, error }) => {
         expect(error).toBeUndefined();
         for (const [query, source, field] of [
            ["run: hub -> { group_by: r.p.ssn }", "source r", "r.p.ssn"],
            [
               "run: hub extend { dimension: x is r.name } -> { group_by: x }",
               "source r",
               "x, which has no #(classification)",
            ],
            ["run: comp_m -> { group_by: only_m }", "source cmem", "only_m"],
         ]) {
            const err = await run(model, query).catch((e) => e);
            expect(err).toBeInstanceOf(BadRequestError);
            expect((err as Error).message).toContain(
               `${source} carries [none]`,
            );
            expect((err as Error).message).toContain(field);
         }
      });
   });

   it("twin: serves a read within that ceiling, and a model reaching no ceiling is not checked", async () => {
      await withAuthorizeModel(FILES, async ({ model }) => {
         for (const query of [
            "run: hub -> { aggregate: r.c }",
            "run: comp_m -> { group_by: k }",
         ]) {
            expect(
               (await run(model, query)).compactResult.length,
            ).toBeGreaterThan(0);
         }
      });
      const open = {
         ...FILES,
         "base.malloy": BASE.replaceAll('#(carries) ["none"]\n', ""),
      };
      await withAuthorizeModel(open, async ({ model }) => {
         expect(
            modelDeclaresCarries(
               (model as unknown as { modelDef: never }).modelDef,
            ),
         ).toBe(false);
         expect(
            (await run(model, "run: hub -> { group_by: r.p.ssn }"))
               .compactResult.length,
         ).toBeGreaterThan(0);
      });
   });
});

describe("a filter an earlier stage of a query source applies", () => {
   const ROWS = `duckdb.sql("select * from (values (1,'east','999-11-2222'),(2,'west','999-33-4444')) as t(id, region, ssn)")`;
   const model = (carries: string) => `##! experimental.access_modifiers
source: people is ${ROWS} include { internal: id, region, ssn } extend {
   #(classification) "pii.direct_identifier"
   dimension: social is ssn
   #(classification) "none"
   dimension: area is region
   #(classification) "none"
   dimension: pid is id
}
#(carries) ${carries}
source: two is people -> { where: social = "999-11-2222"; group_by: area, pid } -> { group_by: area, pid }
#(carries) ${carries}
source: one is people -> { where: social = "999-11-2222"; group_by: area, pid }
source: hub is duckdb.sql("select 1 as id") extend {
   join_one: rr is two on id = rr.pid
   join_one: r1 is one on id = r1.pid
}
`;

   it("charges a read through a join every stage's filter, so two stages refuse as one does", async () => {
      await withAuthorizeModel(model('["none"]'), async ({ model }) => {
         for (const [query, path] of [
            ["run: hub -> { group_by: rr.area }", "rr.area"],
            ["run: hub -> { group_by: r1.area }", "r1.area"],
         ]) {
            const err = await run(model, query).catch((e) => e);
            expect(err).toBeInstanceOf(BadRequestError);
            expect((err as Error).message).toContain(
               `${path}, which filters on social (pii.direct_identifier)`,
            );
         }
         expect(
            (await run(model, "run: hub -> { aggregate: c is count() }"))
               .compactResult.length,
         ).toBe(1);
      });
   });

   it("twin: serves the two-stage source under a ceiling that lists the filter's class", async () => {
      await withAuthorizeModel(
         model('["none", "pii.direct_identifier"]'),
         async ({ model }) => {
            expect(
               (await run(model, "run: two -> { group_by: area }"))
                  .compactResult,
            ).toEqual([{ area: "east" }]);
         },
      );
   });
});

describe("a plain copy declared lower in a carried source an open one joins", () => {
   const model = (yr: string) => `##! experimental.access_modifiers
#(carries) ["none"]
source: s is duckdb.table("child") include { internal: * } extend {
   #(classification) "none"
   dimension: sid is id
   #(classification) "pii.direct_identifier"
   internal dimension: social is name
   #(classification) "none"
   dimension: yr is ${yr}
}
source: hub is duckdb.table("child") include { internal: * } extend {
   join_one: r is s on id = r.sid
}
`;
   const QUERIES = [
      "run: hub -> { group_by: r.yr }",
      "run: hub extend { dimension: x is r.yr } -> { group_by: x }",
      "run: hub -> { aggregate: m is count() { where: r.yr = 'north' } }",
   ];

   it("charges a read of the copy the class it copies", async () => {
      await withAuthorizeModel(model("social"), async ({ model }) => {
         for (const query of QUERIES) {
            const err = await run(model, query).catch((e) => e);
            expect(err).toBeInstanceOf(BadRequestError);
            expect((err as Error).message).toContain("source r carries [none]");
            expect((err as Error).message).toContain(
               "which is classified pii.direct_identifier",
            );
         }
      });
   });

   it("twin: serves a computed field declared lower, the author's word for it", async () => {
      await withAuthorizeModel(
         model("concat(social, '')"),
         async ({ model }) => {
            for (const query of QUERIES) {
               expect((await run(model, query)).compactResult.length).toBe(
                  query.includes("count()") ? 1 : 2,
               );
            }
         },
      );
   });
});

describe("an unreadable chain on a joined struct", () => {
   const carried = {
      name: "carried",
      type: "table",
      annotations: { blockNotes: [{ text: '#(carries) ["none"]\n' }] },
      fields: [],
   };
   const modelDefWith = (joinQuery: unknown) => ({
      contents: {
         carried,
         open: {
            name: "open",
            type: "table",
            fields: [
               {
                  name: "j",
                  join: "one",
                  type: "table",
                  fields: [{ name: "v", type: "string" }],
                  ...(joinQuery ? { query: joinQuery } : {}),
               },
            ],
         },
      },
   });
   const query = {
      structRef: "open",
      pipeline: [
         {
            type: "reduce",
            queryFields: [{ type: "fieldref", path: ["j", "v"] }],
            refSummary: { fieldUsage: [{ path: ["j", "v"] }] },
         },
      ],
   };

   it("refuses a read through it when the model declares #(carries) anywhere", () => {
      const refusal = ceilingRefusal(
         modelDefWith({ structRef: "missing", pipeline: [] }) as never,
         query,
      );
      expect(refusal).toContain("cannot see completely");
   });

   it("twin: the same read through a join with no unreadable chain serves", () => {
      expect(ceilingRefusal(modelDefWith(undefined) as never, query)).toBe(
         undefined,
      );
   });
});

describe("a read whose head shadows a name the first stage outputs", () => {
   const modelDef = {
      contents: {
         s: {
            name: "s",
            type: "table",
            annotations: { blockNotes: [{ text: '#(carries) ["none"]\n' }] },
            fields: [
               {
                  name: "a",
                  type: "string",
                  annotations: {
                     blockNotes: [{ text: '#(classification) "none"\n' }],
                  },
               },
               { name: "n", join: "one", type: "table", fields: [] },
            ],
         },
      },
   };
   const queryReading = (output: string, read: string[]) => ({
      structRef: "s",
      pipeline: [
         {
            type: "reduce",
            queryFields: [{ type: "fieldref", path: ["a"], as: output }],
            refSummary: { fieldUsage: [{ path: ["a"] }, { path: read }] },
         },
      ],
   });

   it("holds a dotted path the lineage cannot resolve to the ceiling", () => {
      const refusal = ceilingRefusal(
         modelDef as never,
         queryReading("n", ["n", "x"]),
      );
      expect(refusal).toContain("n.x");
      expect(refusal).toContain("cannot see completely");
   });

   it("twin: a bare output name, as in `order_by: n`, is not a read of the source", () => {
      expect(ceilingRefusal(modelDef as never, queryReading("m", ["m"]))).toBe(
         undefined,
      );
   });
});

describe("the per-request model def is not rescanned for #(carries)", () => {
   it("skips the scan when the caller already knows the model declares one", () => {
      const scans = (declared?: boolean): number => {
         let count = 0;
         const contents = new Proxy(
            {
               s: {
                  name: "s",
                  type: "table",
                  annotations: {
                     blockNotes: [{ text: '#(carries) ["none"]\n' }],
                  },
                  fields: [{ name: "a", type: "string" }],
               },
            },
            {
               ownKeys(target) {
                  count++;
                  return Reflect.ownKeys(target);
               },
            },
         );
         const query = {
            structRef: "s",
            pipeline: [
               {
                  type: "reduce",
                  queryFields: [{ type: "fieldref", path: ["a"] }],
                  refSummary: { fieldUsage: [{ path: ["a"] }] },
               },
            ],
         };
         ceilingRefusal({ contents } as never, query, [], declared);
         return count;
      };
      expect(scans(true)).toBeLessThan(scans(undefined));
   });
});
