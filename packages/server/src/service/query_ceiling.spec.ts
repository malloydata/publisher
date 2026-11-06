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
            expect((err as Error).message).toContain(`${source} carries [none]`);
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
