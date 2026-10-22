// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * End-to-end and comparator coverage for the filter-binding guard over query
 * sources: a `where:` that reads a field joined from one (served when
 * untouched, in any translation), a misbind under a query source's base or in
 * a join a query stage adds (refused), and the joined shapes the comparator
 * must tell apart (parameter bindings, join types, composites).
 *
 * Every case runs through `serve`/`compare`, whose outcome must equal
 * `EXPECTED[id]`; a refusal only counts as DENIED when the comparator's reason
 * is on the debug spy. Data: deals 1,2,3 (3 deleted); deal_first_close.close_type
 * is null for 1 and 3, so `is_open` admits 1 and 3 through the close join (n=2
 * for the honest A shapes) and 1 and 2 through `not is_deleted` (n=2 for the
 * honest B shapes); a misbound filter admits all three (n=3).
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type FilterCondition,
   type ModelDef,
   type GivenValue,
   type SourceDef,
} from "@malloydata/malloy";
import { afterAll, describe, expect, it, spyOn } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { AccessDeniedError } from "../../src/errors";
import { logger } from "../../src/logger";
import { Environment } from "../../src/service/environment";
import {
   assertFilterConditionBindsToDeclaringSource,
   assertInheritedSourceFiltersBind,
   isSameDeclaration,
} from "../../src/service/filter_binding_guard";
import type { Package } from "../../src/service/package";

const DENY_MESSAGE = "Inherited source filter binding check failed; denying";
/** The guard could not find where a filter was declared, so it refuses rather than guess. */
const UNRESOLVED_REASON = "declaring source could not be resolved";
const COMPARATOR_REASON =
   "resolves to a different field on the executed source";

interface Outcome {
   kind: "SERVED" | "DENIED" | "REFUSED" | "INVALID";
   detail: string;
}

type Kind = Outcome["kind"];

/** Honest row count of a served id (see the header); a served control with any other count answered wrong rows. */
const HONEST_COUNT_OVERRIDES: Record<string, string> = {
   "B7-nest-honest": 'row={"v":{"n":2}}',
   "B7-query-source-honest": 'row={"m":3}',
   "C-givens": "n=3",
   "G1-honest": "n=1",
   "B2b-op-same": "n=3",
};

function checkRows(id: string, outcome: Outcome): void {
   if (outcome.kind !== "SERVED") return;
   expect({ id, detail: outcome.detail }).toEqual({
      id,
      detail: HONEST_COUNT_OVERRIDES[id] ?? "n=2",
   });
}

/** Assert `outcome` is what `EXPECTED` says for `id`; the detail rides along for the failure diff. */
function check(id: string, outcome: Outcome): Outcome {
   expect({ id, kind: outcome.kind, detail: outcome.detail }).toMatchObject({
      id,
      kind: EXPECTED[id],
   });
   return outcome;
}

/** A denial is DENIED (comparator reason) or REFUSED (unresolvable origin); anything else means the probe is invalid. */
function classifyDenial(reasons: string[]): Outcome {
   const comparator = reasons.find((r) => r.includes(COMPARATOR_REASON));
   if (comparator) return { kind: "DENIED", detail: comparator };
   const unresolved = reasons.find((r) => r.includes(UNRESOLVED_REASON));
   if (unresolved) return { kind: "REFUSED", detail: unresolved };
   return {
      kind: "INVALID",
      detail: `denied for another reason: ${reasons.join(" | ") || "<no guard debug reason>"}`,
   };
}

function invalid(e: unknown): Outcome {
   const err = e as Error;
   return {
      kind: "INVALID",
      detail: `${err.constructor?.name}: ${err.message}`
         .replace(/\s+/g, " ")
         .slice(0, 400),
   };
}

/** Run `act` with the guard's debug reasons captured. */
async function withDenialSpy(act: () => Promise<Outcome>): Promise<Outcome> {
   const debug = spyOn(logger, "debug");
   try {
      return await act();
   } catch (e) {
      if (e instanceof AccessDeniedError) {
         return classifyDenial(
            debug.mock.calls
               .filter((c) => String(c[0]) === DENY_MESSAGE)
               .map((c) =>
                  String(
                     ((c as unknown[])[1] as { error?: string } | undefined)
                        ?.error,
                  ),
               ),
         );
      }
      return invalid(e);
   } finally {
      debug.mockRestore();
   }
}

function firstValue(rows: Record<string, unknown>[]): string {
   const row = rows[0] ?? {};
   return "n" in row ? `n=${String(row.n)}` : `row=${JSON.stringify(row)}`;
}

function firstCellValue(resultJson: string): string {
   const cell = JSON.parse(resultJson)?.data?.array_value?.[0]?.record_value;
   const v = cell?.[0];
   return `n=${String(v?.number_value ?? v?.string_value ?? JSON.stringify(v))}`;
}

const roots: string[] = [];
afterAll(async () => {
   for (const root of roots)
      await fs.rm(root, { recursive: true, force: true });
});

interface Served {
   env: Environment;
   pkg: Package;
}

/**
 * Install `files` as package `pkg` (compiled by the real load path) and run
 * `act`. `outside` files sit beside the package, imported as `OUTSIDE/<name>`.
 */
async function withPackage<T>(
   files: Record<string, string>,
   act: (served: Served) => Promise<T>,
   outside: Record<string, string> = {},
): Promise<T> {
   const root = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-probe-"));
   roots.push(root);
   const outDir = path.join(root, "outside");
   await fs.mkdir(outDir, { recursive: true });
   for (const [name, text] of Object.entries(outside)) {
      await fs.writeFile(path.join(outDir, name), text);
   }
   const envPath = path.join(root, "env");
   await fs.mkdir(envPath, { recursive: true });
   const env = await Environment.create("probeEnv", envPath, []);
   const pkg = await env.installPackage("pkg", async (staging) => {
      await fs.mkdir(staging, { recursive: true });
      await fs.writeFile(
         path.join(staging, "publisher.json"),
         JSON.stringify({ name: "pkg" }),
      );
      for (const [name, text] of Object.entries(files)) {
         await fs.writeFile(
            path.join(staging, name),
            text.replaceAll("OUTSIDE/", `file://${outDir}/`),
         );
      }
   });
   return act({ env, pkg });
}

/**
 * Serve `query` over `files` on the query route and on `/compile` (append
 * scope), checking `<id>` against `EXPECTED` and `/compile` against the query route.
 */
async function serve(
   id: string,
   files: Record<string, string>,
   query: string,
   givens: Record<string, GivenValue> = {},
   outside: Record<string, string> = {},
): Promise<{ query: Outcome; compile: Outcome }> {
   let result: { query: Outcome; compile: Outcome };
   try {
      result = await withPackage(
         files,
         async ({ env, pkg }) => {
            const ran = await withDenialSpy(async () => {
               const r = await pkg
                  .getModel("index.malloy")!
                  .getQueryResults(
                     undefined,
                     undefined,
                     query,
                     {},
                     true,
                     givens,
                  );
               return {
                  kind: "SERVED",
                  detail: firstValue(
                     r.compactResult as unknown as Record<string, unknown>[],
                  ),
               };
            });
            const compiled = await withDenialSpy(async () => {
               const { problems } = await env.compileSource(
                  "pkg",
                  "index.malloy",
                  query,
                  false,
                  givens,
                  "append",
               );
               if (problems.length > 0) {
                  return invalid(
                     new Error(problems.map((p) => p.message).join("; ")),
                  );
               }
               return { kind: "SERVED", detail: "compile-ok" };
            });
            return { query: ran, compile: compiled };
         },
         outside,
      );
   } catch (e) {
      result = {
         query: invalid(e),
         compile: { kind: "INVALID", detail: "package did not load" },
      };
   }
   check(id, result.query);
   checkRows(id, result.query);
   // `/compile` runs nothing, so it must never be more permissive than the query route.
   if (!(id in COMPILE_DIVERGENCE_ALLOWED)) {
      expect({ id, compile: result.compile }).toMatchObject({
         id,
         compile: { kind: result.query.kind },
      });
   }
   return result;
}

/** Run notebook cell 1 of `nb.malloynb` (cells compile unrestricted). */
async function serveCell(id: string, notebook: string): Promise<Outcome> {
   let outcome: Outcome;
   try {
      outcome = await withPackage(
         { "nb.malloynb": notebook },
         async ({ pkg }) =>
            withDenialSpy(async () => {
               const r = await pkg
                  .getModel("nb.malloynb")!
                  .executeNotebookCell(1, {}, false, {});
               return { kind: "SERVED", detail: firstCellValue(r.result!) };
            }),
      );
   } catch (e) {
      outcome = invalid(e);
   }
   check(id, outcome);
   checkRows(id, outcome);
   return outcome;
}

// ---------------------------------------------------------------- fixtures

const DEAL_ROWS = `select 1 as deal_id, false as is_deleted union all select 2, false union all select 3, true`;
const CLOSE_ROWS = `select 1 as deal_id, cast(null as integer) as close_type union all select 2, 7 union all select 3, cast(null as integer)`;
const NULL_ROWS = `select 1 as deal_id, cast(null as integer) as close_type union all select 2, cast(null as integer) union all select 3, cast(null as integer)`;
const DEAL = `duckdb.sql("${DEAL_ROWS}")`;
const CLOSE = `duckdb.sql("${CLOSE_ROWS}")`;
const NULLS = `duckdb.sql("${NULL_ROWS}")`;
const COMPOSITES = "##! experimental { composite_sources }";

const RUN_PIPELINE = "run: pipeline_now -> { aggregate: n is count() }";
const INDEX = `import "deals.malloy"\nimport "pipeline.malloy"\nexport { pipeline_now, deals }`;

/**
 * deals.malloy joins `alias` (declared in `decls`, entry file `entry`) and
 * reads it in `is_open`; `deals` filters on it. pipeline.malloy derives
 * `pipeline_now` from `deals` in a separate file, so the executed struct and
 * the declaring struct come from different translations. The twin re-binds
 * `alias` to `twinDecl`'s `twin_src` (all-null close rows, so a wrong serve
 * counts 3).
 */
interface Family {
   decls: Record<string, string>;
   entry: string;
   alias: string;
   twinDecl: string;
   header?: string;
   // Imports do not chain, so a given declared in a decl file needs its own import in index.
   indexImports?: string[];
}

function familyFiles(f: Family, twin: boolean): Record<string, string> {
   const header = f.header ?? "";
   return {
      ...f.decls,
      "deals.malloy": `${header}
import "${f.entry}"
source: deals_incl is ${DEAL} extend {
   join_one: ${f.alias} on deal_id = ${f.alias}.deal_id
   dimension: is_open is ${f.alias}.close_type is null
   measure: n is count()
}
source: deals is deals_incl extend { where: is_open }
`,
      "pipeline.malloy": twin
         ? `${header}
import "deals.malloy"
${f.twinDecl}
source: pipeline_now is deals extend { rename: old_${f.alias} is ${f.alias} } extend {
   join_one: ${f.alias} is twin_src on deal_id = ${f.alias}.deal_id
}
`
         : `${header}\nimport "deals.malloy"\nsource: pipeline_now is deals extend { dimension: x is 1 }`,
      "index.malloy": `${header}\n${(f.indexImports ?? []).map((i) => `import "${i}"`).join("\n")}\n${INDEX}`,
   };
}

const DFC = `${CLOSE} -> { group_by: deal_id, close_type } extend { primary_key: deal_id }`;
const NULL_DFC = `${NULLS} -> { group_by: deal_id, close_type } extend { primary_key: deal_id }`;

const A1: Family = {
   decls: {
      "q1.malloy": `source: q1 is ${CLOSE} -> { group_by: deal_id, close_type }`,
      "q2.malloy": `import "q1.malloy"\nsource: q2 is q1 -> { group_by: deal_id, close_type } extend { primary_key: deal_id }`,
   },
   entry: "q2.malloy",
   alias: "q2",
   twinDecl: `source: tq1 is ${NULLS} -> { group_by: deal_id, close_type }
source: twin_src is tq1 -> { group_by: deal_id, close_type } extend { primary_key: deal_id }`,
};

const A2: Family = {
   decls: {
      "b.malloy": `source: dfc is ${DFC}
source: base is ${DEAL} extend { join_one: dfc on deal_id = dfc.deal_id }`,
      "q.malloy": `import "b.malloy"
source: dq is base -> { group_by: deal_id; group_by: close_type is dfc.close_type } extend { primary_key: deal_id }`,
   },
   entry: "q.malloy",
   alias: "dq",
   twinDecl: `source: tdfc is ${NULL_DFC}
source: tbase is ${DEAL} extend { join_one: tdfc on deal_id = tdfc.deal_id }
source: twin_src is tbase -> { group_by: deal_id; group_by: close_type is tdfc.close_type } extend { primary_key: deal_id }`,
};

const A3: Family = {
   decls: {
      "c.malloy": `source: dfc0 is ${DFC}`,
      "q.malloy": `import "c.malloy"
source: dq is ${DEAL} -> {
   extend: { join_one: dfc0 on deal_id = dfc0.deal_id }
   group_by: deal_id
   group_by: close_type is dfc0.close_type
} extend { primary_key: deal_id }`,
   },
   entry: "q.malloy",
   alias: "dq",
   twinDecl: `source: tdfc0 is ${NULL_DFC}
source: twin_src is ${DEAL} -> {
   extend: { join_one: tdfc0 on deal_id = tdfc0.deal_id }
   group_by: deal_id
   group_by: close_type is tdfc0.close_type
} extend { primary_key: deal_id }`,
};

const DFC_PLAIN: Family = {
   decls: { "close.malloy": `source: dfc is ${DFC}` },
   entry: "close.malloy",
   alias: "dfc",
   twinDecl: `source: twin_src is ${NULL_DFC}`,
};

// ------------------------------------------------------------ calibration

describe("calibration", () => {
   it("CAL-C: no query-source join, inherited where, cross-file derived source serves", async () => {
      await serve(
         "CAL-C",
         {
            "deals.malloy": `
source: deals_incl is ${DEAL} extend { dimension: is_open is not is_deleted  measure: n is count() }
source: deals is deals_incl extend { where: is_open }
`,
            "pipeline.malloy": `import "deals.malloy"\nsource: pipeline_now is deals extend { dimension: x is 1 }`,
            "index.malloy": INDEX,
         },
         RUN_PIPELINE,
      );
   });
   it("CAL-G: query-source join, served directly, serves", async () => {
      await serve(
         "CAL-G",
         familyFiles(DFC_PLAIN, false),
         "run: deals -> { aggregate: n is count() }",
      );
   });
});

// ------------------------------------------------ A. false denials (+ twins)

const FAMILIES: [string, Family][] = [
   ["A0", DFC_PLAIN],
   ["A1", A1],
   ["A2", A2],
   ["A3", A3],
];

describe("A: query-source joins compiled in another translation", () => {
   for (const [id, family] of FAMILIES) {
      it(`${id}: control (identical declaration, other translation)`, async () => {
         await serve(id, familyFiles(family, false), RUN_PIPELINE);
      });
      it(`${id}-twin: alias re-bound to a different query source`, async () => {
         await serve(`${id}-twin`, familyFiles(family, true), RUN_PIPELINE);
      });
   }
});

describe("A4: composite with a query-source member", () => {
   const memberFiles = (member: string, extraDecl: string, twin: boolean) => ({
      "cmp.malloy": `${COMPOSITES}
${extraDecl}
source: comp is compose(${member}, ${DEAL} extend { dimension: other is 1 }) extend { measure: n is count() }
`,
      "deals.malloy": `${COMPOSITES}\nimport "cmp.malloy"\nsource: deals is comp extend { where: is_open }`,
      "pipeline.malloy": twin
         ? `${COMPOSITES}\nimport "deals.malloy"\nsource: pipeline_now is deals extend { rename: raw_open is is_open } extend { dimension: is_open is true }`
         : `${COMPOSITES}\nimport "deals.malloy"\nsource: pipeline_now is deals extend { dimension: x is 1 }`,
      "index.malloy": `${COMPOSITES}\n${INDEX}`,
   });
   const qMember = `qmem`;
   const qDecl = `source: qmem is ${DEAL} -> { group_by: deal_id, is_deleted } extend { primary_key: deal_id; dimension: is_open is not is_deleted }`;
   const jMember = `jmem`;
   const jDecl = `source: dfc is ${DFC}
source: jmem is ${DEAL} extend { join_one: dfc on deal_id = dfc.deal_id; dimension: is_open is dfc.close_type is null }`;

   it("A4a: member is a query source (control)", async () => {
      await serve("A4a", memberFiles(qMember, qDecl, false), RUN_PIPELINE);
   });
   it("A4a-twin: top-level is_open re-bound", async () => {
      await serve("A4a-twin", memberFiles(qMember, qDecl, true), RUN_PIPELINE);
   });
   it("A4b: member joins a query source (control)", async () => {
      await serve("A4b", memberFiles(jMember, jDecl, false), RUN_PIPELINE);
   });
   it("A4b-twin: top-level is_open re-bound", async () => {
      await serve("A4b-twin", memberFiles(jMember, jDecl, true), RUN_PIPELINE);
   });
});

describe("A5/B2: a joined composite", () => {
   const MEMBERS = `source: m1 is ${CLOSE} extend { primary_key: deal_id }
source: m2 is ${CLOSE} extend { primary_key: deal_id; dimension: other is 1 }
source: cmp is compose(m1, m2)`;
   const NULL_MEMBERS = `source: tm1 is ${NULLS} extend { primary_key: deal_id }
source: tm2 is ${NULLS} extend { primary_key: deal_id; dimension: other is 1 }
source: twin_src is compose(tm1, tm2)`;
   const family: Family = {
      header: COMPOSITES,
      decls: { "close.malloy": `${COMPOSITES}\n${MEMBERS}` },
      entry: "close.malloy",
      alias: "cmp",
      twinDecl: NULL_MEMBERS,
   };
   it("A5-direct: declared source served directly through a joined composite", async () => {
      await serve(
         "A5-direct",
         familyFiles(family, false),
         "run: deals -> { aggregate: n is count() }",
      );
   });
   it("A5: derived cross-file through a joined composite (control)", async () => {
      await serve("A5", familyFiles(family, false), RUN_PIPELINE);
   });
   it("A5-samefile: derived in the same file as deals, through a joined composite", async () => {
      const files = familyFiles(family, false);
      await serve(
         "A5-samefile",
         {
            "close.malloy": files["close.malloy"],
            "deals.malloy": `${files["deals.malloy"]}\nsource: pipeline_now is deals extend { dimension: x is 1 }`,
            "pipeline.malloy": `${COMPOSITES}`,
            "index.malloy": files["index.malloy"],
         },
         RUN_PIPELINE,
      );
   });
   it("B2: caller-side alias re-joined to a different composite (end to end)", async () => {
      await serve("B2", familyFiles(family, true), RUN_PIPELINE);
   });
});

// ------------------- B2b: a caller rejoins a composite alias to a member Malloy would not pick

describe("B2b: rejoining a joined composite alias", () => {
   const rejoin = (to: string, op = "") =>
      `run: deals extend { rename: old_cmp is cmp } extend { join_one: cmp is ${to} ${op} on deal_id = cmp.deal_id } -> { aggregate: n is count() }`;
   const HONEST = "run: deals -> { aggregate: n is count() }";

   describe("members in another order, split across files", () => {
      const close = `${COMPOSITES}
source: m1 is ${CLOSE} extend { primary_key: deal_id }
source: m2 is ${NULLS} extend { primary_key: deal_id }
source: cmp is compose(m1, m2)
source: cmp_rev is compose(m2, m1)
`;
      const files = {
         "close.malloy": close,
         "deals.malloy": `${COMPOSITES}
import "close.malloy"
source: deals_incl is ${DEAL} extend {
   join_one: cmp on deal_id = cmp.deal_id
   dimension: is_open is cmp.close_type is null
   measure: n is count()
}
source: deals is deals_incl extend { where: is_open }
`,
         "index.malloy": `${COMPOSITES}\nimport "close.malloy"\nimport "deals.malloy"\nexport { deals, cmp, cmp_rev }`,
      };
      it("B2b-split-honest: untouched", async () => {
         await serve("B2b-split-honest", files, HONEST);
      });
      it("B2b-split-same: re-joined to the same composite", async () => {
         await serve("B2b-split-same", files, rejoin("cmp"));
      });
      it("B2b-split-reordered: re-joined to a composite of the same members in another order", async () => {
         await serve("B2b-split-reordered", files, rejoin("cmp_rev"));
      });
   });

   describe("a member with the same fields", () => {
      const files = {
         "index.malloy": `${COMPOSITES}
source: m1 is ${CLOSE} extend { primary_key: deal_id }
source: m2 is ${NULLS} extend { primary_key: deal_id }
source: cmp is compose(m1, m2)
source: deals_incl is ${DEAL} extend {
   join_one: cmp on deal_id = cmp.deal_id
   dimension: is_open is cmp.close_type is null
   measure: n is count()
}
source: deals is deals_incl extend { where: is_open }
export { deals, cmp, m1, m2 }`,
      };
      it("B2b-member-m2: re-joined directly to a member that reads other rows", async () => {
         await serve("B2b-member-m2", files, rejoin("m2"));
      });
      it("B2b-member-m1: re-joined directly to a member, even the one Malloy would pick", async () => {
         await serve("B2b-member-m1", files, rejoin("m1"));
      });
   });

   describe("another join type", () => {
      const H = "##! experimental { composite_sources, join_types }";
      const DEAL4 = `duckdb.sql("select 1 as deal_id, false as is_deleted union all select 2, false union all select 3, true union all select 4, false")`;
      const CLOSE5 = `duckdb.sql("select 1 as deal_id, cast(null as integer) as close_type union all select 2, 7 union all select 3, cast(null as integer) union all select 5, cast(null as integer)")`;
      const files = {
         "deals.malloy": `${H}
source: m1 is ${CLOSE5} extend { primary_key: deal_id }
source: m2 is ${CLOSE5} extend { primary_key: deal_id; dimension: other is 1 }
source: cmp is compose(m1, m2)
source: deals_incl is ${DEAL4} extend {
   join_one: cmp on deal_id = cmp.deal_id
   dimension: is_open is cmp.close_type is null
   measure: n is count()
}
source: deals is deals_incl extend { where: is_open }
`,
         "pipeline.malloy": `${H}\nimport "deals.malloy"\nsource: pipeline_now is deals extend { dimension: x is 1 }`,
         "index.malloy": `${H}\nimport "deals.malloy"\nimport "pipeline.malloy"\nexport { deals, pipeline_now, cmp, m1, m2 }`,
      };
      const run = (to: string, op: string) =>
         `run: pipeline_now extend { rename: old_cmp is cmp } extend { join_one: cmp is ${to} ${op} on deal_id = cmp.deal_id } -> { aggregate: n is count() }`;
      it("B2b-op-same: composite re-joined with its own join type", async () => {
         await serve("B2b-op-same", files, run("cmp", ""));
      });
      for (const [to, op] of [
         ["cmp", "full"],
         ["cmp", "inner"],
         ["m1", "full"],
         ["m1", "inner"],
         ["m1", "right"],
      ]) {
         it(`B2b-op-${to}-${op}`, async () => {
            await serve(`B2b-op-${to}-${op}`, files, run(to, op));
         });
      }
   });
});

// ------------------------------- X: the declaring file sits outside the package

describe("X: a source derived from a file outside the package", () => {
   const SHARED = `
source: deals_incl is ${DEAL} extend { dimension: is_open is not is_deleted  measure: n is count() }
source: deals is deals_incl extend { where: is_open }
`;
   const MIS = "except: is_open } extend { dimension: is_open is true";
   const RUN = "run: pipeline_now -> { aggregate: n is count() }";
   const cases: [string, string][] = [
      [
         "X1-misbound",
         `import "OUTSIDE/shared.malloy"\nsource: pipeline_now is deals extend { ${MIS} }`,
      ],
      [
         "X3-query-source-misbound",
         `import "OUTSIDE/shared.malloy"
source: deals_x is deals extend { ${MIS} }
source: pipeline_now is deals_x -> { group_by: deal_id } extend { primary_key: deal_id }`,
      ],
   ];
   for (const [id, pipeline] of cases) {
      it(`${id}: refused on both routes`, async () => {
         await serve(
            id,
            {
               "pipeline.malloy": pipeline,
               "index.malloy": `import "pipeline.malloy"\nexport { pipeline_now }`,
            },
            RUN,
            {},
            { "shared.malloy": SHARED },
         );
      });
   }
});

// ------------------------------------------------------ A6: query-source target

describe("A6: a declared query source serving its own where:", () => {
   const QS = (pre: string) => `${pre}
source: qs is ${DEAL} -> { group_by: deal_id, is_deleted } extend {
   join_one: dfc on deal_id = dfc.deal_id
   dimension: is_open is dfc.close_type is null
   where: is_open
   measure: n is count()
}`;
   const DFC_FILE = `source: dfc is ${DFC}`;
   const RUN_QS = "run: qs -> { aggregate: n is count() }";
   it("A6a: declared in the serving file, served directly", async () => {
      await serve(
         "A6a",
         { "index.malloy": `${DFC_FILE}\n${QS("")}\nexport { qs }` },
         RUN_QS,
      );
   });
   it("A6b: derived from it in the serving file (inheritance)", async () => {
      await serve(
         "A6b",
         {
            "index.malloy": `${DFC_FILE}\n${QS("")}\nsource: qs2 is qs extend { dimension: x is 1 }\nexport { qs, qs2 }`,
         },
         "run: qs2 -> { aggregate: n is count() }",
      );
   });
   it("A6c: declared in an imported file, served directly", async () => {
      await serve(
         "A6c",
         {
            "close.malloy": DFC_FILE,
            "qs.malloy": `import "close.malloy"\n${QS("")}`,
            "index.malloy": `import "qs.malloy"\nexport { qs }`,
         },
         RUN_QS,
      );
   });
   it("A6e: query source built on a base whose inherited where: reads a query-source join", async () => {
      await serve(
         "A6e",
         {
            "index.malloy": `${DFC_FILE}
source: deals_incl is ${DEAL} extend {
   join_one: dfc on deal_id = dfc.deal_id
   dimension: is_open is dfc.close_type is null
}
source: deals is deals_incl extend { where: is_open }
source: qs is deals -> { group_by: deal_id }
export { qs }`,
         },
         RUN_QS,
      );
   });
   it("A6f: the same, base imported from another file", async () => {
      await serve(
         "A6f",
         {
            "close.malloy": DFC_FILE,
            "deals.malloy": `import "close.malloy"
source: deals_incl is ${DEAL} extend {
   join_one: dfc on deal_id = dfc.deal_id
   dimension: is_open is dfc.close_type is null
}
source: deals is deals_incl extend { where: is_open }`,
            "qs.malloy": `import "deals.malloy"\nsource: qs is deals -> { group_by: deal_id }`,
            "index.malloy": `import "qs.malloy"\nexport { qs }`,
         },
         RUN_QS,
      );
   });
   it("A6g: a source derived (another file) from a query source that serves its own where:", async () => {
      await serve(
         "A6g",
         {
            "close.malloy": DFC_FILE,
            "qs.malloy": `import "close.malloy"\n${QS("")}`,
            "pipeline.malloy": `import "qs.malloy"\nsource: pipeline_qs is qs extend { dimension: x is 1 }`,
            "index.malloy": `import "pipeline.malloy"\nexport { pipeline_qs }`,
         },
         "run: pipeline_qs -> { aggregate: n is count() }",
      );
   });
   it("A6d: control, no query-source join in the query source", async () => {
      await serve(
         "A6d",
         {
            "index.malloy": `source: qs is ${DEAL} -> { group_by: deal_id, is_deleted } extend {
   dimension: is_open is not is_deleted
   where: is_open
   measure: n is count()
}
export { qs }`,
         },
         RUN_QS,
      );
   });
});

// ------------------------------------------------ B1: misbind inside a query source

describe("B1: a plain where: misbound under a query source's base", () => {
   const BASE = (derive: string) => `
source: deals_incl is ${DEAL} extend { dimension: is_open is not is_deleted  measure: n is count() }
source: deals is deals_incl extend { where: is_open }
source: deals_x is deals extend { ${derive} }
source: ids is ${DEAL} extend { primary_key: deal_id }
`;
   const MISBOUND = "except: is_open";
   const REBOUND = `${MISBOUND} } extend { dimension: is_open is true`;
   const HONEST = "dimension: tag is 1";
   const QS = `source: qs is deals_x -> { group_by: deal_id } extend { primary_key: deal_id }`;
   const variants: [string, string][] = [
      ["misbound", REBOUND],
      ["honest", HONEST],
   ];

   it("B1-direct: the misbind itself is refused when deals_x is served directly", async () => {
      await serve(
         "B1-direct",
         { "index.malloy": `${BASE(REBOUND)}\nexport { deals_x }` },
         "run: deals_x -> { aggregate: n is count() }",
      );
   });

   for (const [label, derive] of variants) {
      it(`B1-run-${label}: query source as the run target`, async () => {
         await serve(
            `B1-run-${label}`,
            { "index.malloy": `${BASE(derive)}\n${QS}\nexport { qs }` },
            "run: qs -> { aggregate: n is count() }",
         );
      });
      it(`B1-join-${label}: query source as a caller join`, async () => {
         await serve(
            `B1-join-${label}`,
            { "index.malloy": `${BASE(derive)}\n${QS}\nexport { qs, ids }` },
            "run: ids extend { join_one: q is qs on deal_id = q.deal_id } -> { aggregate: n is q.count() }",
         );
      });
      it(`B1-nested-${label}: q2 over q1 in another file`, async () => {
         await serve(
            `B1-nested-${label}`,
            {
               "q1.malloy": `${BASE(derive)}\nsource: q1 is deals_x -> { group_by: deal_id }`,
               "q2.malloy": `import "q1.malloy"\nsource: q2 is q1 -> { group_by: deal_id } extend { primary_key: deal_id }`,
               "index.malloy": `import "q2.malloy"\nexport { q2 }`,
            },
            "run: q2 -> { aggregate: n is count() }",
         );
      });
      it(`B1-composite-${label}: query source as a composite member`, async () => {
         await serve(
            `B1-composite-${label}`,
            {
               "index.malloy": `${COMPOSITES}
${BASE(derive)}
${QS}
source: comp is compose(qs, ${DEAL} extend { dimension: other is 1 }) extend { measure: n is count() }
export { comp }`,
            },
            "run: comp -> { aggregate: n is count() }",
         );
      });
   }
});

// ---------------------------------- B3: caller re-joins an alias

describe("B3: a caller re-joins the alias to another package-declared query source", () => {
   const rejoin = (source: string, on = "deal_id = dfc.deal_id") =>
      `run: deals extend { rename: old_dfc is dfc } extend { join_one: dfc is ${source} on ${on} } -> { aggregate: n is count() }`;
   const ALTS = `
source: ctl_q is ${DFC}
source: alt_q is ${NULL_DFC}
source: alt_w is ${CLOSE} -> { group_by: deal_id, close_type; where: deal_id > 5 } extend { primary_key: deal_id }
`;
   const DEALS = `
source: deals_incl is ${DEAL} extend {
   join_one: dfc on deal_id = dfc.deal_id
   dimension: is_open is dfc.close_type is null
   measure: n is count()
}
source: deals is deals_incl extend { where: is_open }
`;
   // `dfc` must be exported too: a caller `rename:` of an unexported join is refused as off-surface.
   const EXPORTS = "export { deals, dfc, ctl_q, alt_q, alt_w }";
   const layouts: [string, Record<string, string>][] = [
      [
         "same",
         {
            "index.malloy": `source: dfc is ${DFC}\n${DEALS}\n${ALTS}\n${EXPORTS}`,
         },
      ],
      [
         "split",
         {
            "close.malloy": `source: dfc is ${DFC}`,
            "deals.malloy": `import "close.malloy"\n${DEALS}`,
            "alts.malloy": ALTS,
            "index.malloy": `import "close.malloy"\nimport "deals.malloy"\nimport "alts.malloy"\n${EXPORTS}`,
         },
      ],
   ];
   const cases: [string, string][] = [
      ["ctl", rejoin("ctl_q")],
      ["U2", rejoin("alt_q")],
      ["U3", rejoin("alt_w")],
      ["U4", rejoin("ctl_q", "deal_id + 0 = dfc.deal_id")],
      [
         "J-inline",
         rejoin(
            "deals -> { group_by: deal_id; group_by: close_type is deal_id * 0 } extend { primary_key: deal_id }",
         ),
      ],
   ];
   for (const [layout, files] of layouts) {
      for (const [label, query] of cases) {
         it(`B3-${layout}-${label}`, async () => {
            await serve(`B3-${layout}-${label}`, files, query);
         });
      }
   }
});

// ------------------------------------ B7: a join added inside a query stage

describe("B7: a misbound source joined inside a query stage", () => {
   const SOURCES = `
source: deals_incl is ${DEAL} extend { dimension: is_open is not is_deleted  measure: n is count() }
source: deals is deals_incl extend { where: is_open }
source: pipeline_now is deals extend { dimension: x is 1 }
`;
   const JOIN = (src: string) => `join_one: d is ${src} on deal_id = d.deal_id`;
   const MIS =
      "pipeline_now extend { except: is_open } extend { dimension: is_open is true }";
   const shapes: [string, (src: string) => string][] = [
      [
         "stage",
         (s) =>
            `run: deals_incl -> { extend: { ${JOIN(s)} }; aggregate: n is d.count() }`,
      ],
      [
         "nest",
         (s) =>
            `run: deals_incl -> { extend: { ${JOIN(s)} }; nest: v is { aggregate: n is d.count() } }`,
      ],
      [
         "view",
         (s) =>
            `run: deals_incl extend { view: v is { extend: { ${JOIN(s)} }; aggregate: n is d.count() } } -> v`,
      ],
      [
         "stage2",
         (s) =>
            `run: deals_incl -> { group_by: deal_id } -> { extend: { ${JOIN(s)} }; aggregate: n is d.count() }`,
      ],
   ];
   shapes.push([
      "query-source",
      (s) =>
         `run: deals_incl extend { join_one: q is deals_incl -> { extend: { ${JOIN(s)} }; group_by: deal_id; aggregate: n is d.count() } on deal_id = q.deal_id } -> { aggregate: m is q.count() }`,
   ]);
   for (const [shape, text] of shapes) {
      for (const [label, src] of [
         ["misbound", MIS],
         ["honest", "pipeline_now"],
      ]) {
         it(`B7-${shape}-${label}`, async () => {
            await serve(
               `B7-${shape}-${label}`,
               {
                  "index.malloy": `${SOURCES}\nexport { deals_incl, pipeline_now }`,
               },
               text(src),
            );
         });
      }
   }
});

// ---------------------------- B8: parameterized base (end to end, then comparator)

describe("B8: a query source over a parameterized base", () => {
   const PARAM = "##! experimental { parameters }";
   const BASE = `${PARAM}
source: base(accessModifier::number) is ${CLOSE} extend { where: deal_id >= accessModifier }`;
   const DEALS = `
source: deals_incl is ${DEAL} extend {
   join_one: dfc on deal_id = dfc.deal_id
   dimension: is_open is dfc.close_type is null
   measure: n is count()
}
source: deals is deals_incl extend { where: is_open }
`;
   const rejoin = (source: string) =>
      `run: deals extend { rename: old_dfc is dfc } extend { join_one: dfc is ${source} on deal_id = dfc.deal_id } -> { aggregate: n is count() }`;
   const files = {
      "base.malloy": BASE,
      "close.malloy": `${PARAM}\nimport "base.malloy"\nsource: dfc is base(accessModifier is 1) -> { group_by: deal_id, close_type } extend { primary_key: deal_id }`,
      "alts.malloy": `${PARAM}
import "base.malloy"
source: dfc_same is base(accessModifier is 1) -> { group_by: deal_id, close_type } extend { primary_key: deal_id }
source: dfc_other is base(accessModifier is 3) -> { group_by: deal_id, close_type } extend { primary_key: deal_id }
`,
      "deals.malloy": `${PARAM}\nimport "close.malloy"\n${DEALS}`,
      "index.malloy": `${PARAM}\nimport "close.malloy"\nimport "deals.malloy"\nimport "alts.malloy"\nexport { deals, dfc, dfc_same, dfc_other }`,
   };
   it("B8-e2e-ctl: re-join to an identical parameter binding", async () => {
      await serve("B8-e2e-ctl", files, rejoin("dfc_same"));
   });
   it("B8-e2e-twin: re-join to a different parameter binding", async () => {
      await serve("B8-e2e-twin", files, rejoin("dfc_other"));
   });
});

// ------------------------------------------------ B9: joined composite member where

describe("B9: a joined composite member's own where: misbound", () => {
   const body = (member: string) => `${COMPOSITES}
source: m1 is ${CLOSE} extend { primary_key: deal_id; dimension: live is close_type is null; where: live }
source: m1x is m1 extend { ${member} }
source: m2 is ${CLOSE} extend { primary_key: deal_id; dimension: other is 1 }
source: cmp is compose(m1x, m2)
source: ids is ${DEAL} extend { join_one: cmp on deal_id = cmp.deal_id }
export { ids }`;
   const query = "run: ids -> { aggregate: n is cmp.count() }";
   for (const [label, member] of [
      ["misbound", "except: live } extend { dimension: live is true"],
      ["honest", "dimension: tag is 1"],
   ]) {
      it(`B9-${label}`, async () => {
         await serve(`B9-${label}`, { "index.malloy": body(member) }, query);
      });
   }
});

// ---------------------------------------------------- C: notebook-cell entry

describe("C-notebook: cells compile unrestricted", () => {
   const SOURCES = `
source: deals_incl is ${DEAL} extend { dimension: is_open is not is_deleted  measure: n is count() }
source: deals is deals_incl extend { where: is_open }
source: pipeline_now is deals extend { dimension: x is 1 }
`;
   const nb = (cell0: string, cell1: string) =>
      `>>>malloy\n${cell0}\n\n>>>malloy\n${cell1}\n`;
   for (const [label, derive] of [
      ["misbound", "except: is_open } extend { dimension: is_open is true"],
      ["honest", "dimension: tag is 1"],
   ]) {
      it(`C-nb-B1-${label}: query source as the run target`, async () => {
         await serveCell(
            `C-nb-B1-${label}`,
            nb(
               `${SOURCES}\nsource: deals_x is deals extend { ${derive} }\nsource: qs is deals_x -> { group_by: deal_id } extend { primary_key: deal_id }`,
               "run: qs -> { aggregate: n is count() }",
            ),
         );
      });
      it(`C-nb-B7-${label}: join added inside a stage`, async () => {
         const src =
            label === "honest"
               ? "pipeline_now"
               : "pipeline_now extend { except: is_open } extend { dimension: is_open is true }";
         await serveCell(
            `C-nb-B7-${label}`,
            nb(
               SOURCES,
               `run: deals_incl -> { extend: { join_one: d is ${src} on deal_id = d.deal_id }; aggregate: n is d.count() }`,
            ),
         );
      });
   }
   it("C-nb-direct: the misbind itself is refused in a cell", async () => {
      await serveCell(
         "C-nb-direct",
         nb(
            `${SOURCES}\nsource: deals_x is deals extend { except: is_open } extend { dimension: is_open is true }`,
            "run: deals_x -> { aggregate: n is count() }",
         ),
      );
   });
});

// ----------------------------------------------- C: givens, referencedAt

describe("C-givens: a given read by a query source's pipeline where", () => {
   const header = "##! experimental { givens }";
   const family: Family = {
      header,
      decls: {
         "close.malloy": `${header}
given:
  LIM :: number
source: dfc is ${CLOSE} -> { where: deal_id = $LIM; group_by: deal_id, close_type } extend { primary_key: deal_id }`,
      },
      entry: "close.malloy",
      alias: "dfc",
      twinDecl: `source: twin_src is ${NULL_DFC}`,
      indexImports: ["close.malloy"],
   };
   it("C-givens: cross-translation control with a given in the pipeline", async () => {
      await serve("C-givens", familyFiles(family, false), RUN_PIPELINE, {
         LIM: 3,
      });
   });
   it("C-givens-twin: alias re-bound", async () => {
      await serve("C-givens-twin", familyFiles(family, true), RUN_PIPELINE, {
         LIM: 3,
      });
   });
});

describe("C-referencedAt: `src -> view` query sources", () => {
   const CLOSE_BASE = `source: close_base is ${CLOSE} extend { primary_key: deal_id; view: by_deal is { group_by: deal_id, close_type }; view: by_deal_all is { group_by: deal_id; group_by: close_type is 7 } }`;
   const family: Family = {
      decls: {
         "close.malloy": `${CLOSE_BASE}\nsource: dfc is close_base -> by_deal extend { primary_key: deal_id }`,
      },
      entry: "close.malloy",
      alias: "dfc",
      twinDecl: `source: twin_src is ${NULL_DFC}`,
   };
   it("C-ref-same: same declaration, other translation", async () => {
      await serve("C-ref-same", familyFiles(family, false), RUN_PIPELINE);
   });
   const rejoinMoved = (id: string, view: string) =>
      serve(
         id,
         {
            "close.malloy": `${CLOSE_BASE}\nsource: dfc is close_base -> by_deal extend { primary_key: deal_id }`,
            "deals.malloy": `import "close.malloy"
source: deals_incl is ${DEAL} extend {
   join_one: dfc on deal_id = dfc.deal_id
   dimension: is_open is dfc.close_type is null
   measure: n is count()
}
source: deals is deals_incl extend { where: is_open }
source: moved is close_base -> ${view} extend { primary_key: deal_id }
`,
            "index.malloy": `import "close.malloy"\nimport "deals.malloy"\nexport { deals, dfc, moved }`,
         },
         "run: deals extend { rename: old_dfc is dfc } extend { join_one: dfc is moved on deal_id = dfc.deal_id } -> { aggregate: n is count() }",
      );
   it("C-ref-moved: caller re-joins an identical src -> view written elsewhere", async () => {
      await rejoinMoved("C-ref-moved", "by_deal");
   });
   it("C-ref-moved-twin: caller re-joins a different view of the same source", async () => {
      await rejoinMoved("C-ref-moved-twin", "by_deal_all");
   });
});

// ------------------------------------------------------- comparator tier

function runtimeOver(
   files: Record<string, string>,
   conn: DuckDBConnection,
): Runtime {
   return new Runtime({
      urlReader: new InMemoryURLReader(
         new Map(Object.entries(files).map(([k, v]) => [`file:///${k}`, v])),
      ),
      connections: new FixedConnectionMap(
         new Map([["duckdb", conn]]),
         "duckdb",
      ),
   });
}

async function structs(
   files: Record<string, string>,
   entry: string,
): Promise<Record<string, SourceDef>> {
   const conn = new DuckDBConnection("duckdb", ":memory:");
   try {
      const model = await runtimeOver(files, conn).getModel(
         new URL(`file:///${entry}`),
      );
      return (
         model as unknown as {
            _modelDef: { contents: Record<string, SourceDef> };
         }
      )._modelDef.contents;
   } finally {
      await conn.close();
   }
}

/** `name` as the executed run target, after Malloy resolves a composite to one member. */
async function resolvedTarget(
   files: Record<string, string>,
   entry: string,
   name: string,
): Promise<{ declared: SourceDef; resolved: SourceDef | undefined }> {
   const conn = new DuckDBConnection("duckdb", ":memory:");
   try {
      const materializer = runtimeOver(files, conn).loadModel(
         new URL(`file:///${entry}`),
      );
      const declared = (
         (await materializer.getModel()) as unknown as {
            _modelDef: { contents: Record<string, SourceDef> };
         }
      )._modelDef.contents[name];
      const prepared = (await materializer
         .loadQuery(`run: ${name} -> { aggregate: n is count() }`)
         .getPreparedQuery()) as unknown as {
         _query?: { compositeResolvedSourceDef?: SourceDef };
      };
      return {
         declared,
         resolved: prepared._query?.compositeResolvedSourceDef,
      };
   } finally {
      await conn.close();
   }
}

/** `name` from two compiles, compared as declared vs executed. */
async function compare(
   id: string,
   declared: Record<string, string>,
   executed: Record<string, string>,
   entry = "m.malloy",
   name = "deals",
): Promise<Outcome> {
   const d = (await structs(declared, entry))[name];
   const e = (await structs(executed, entry))[name];
   return compareStructs(id, d, e);
}

/** The comparator throws a plain Error, so classify its message directly. */
function compareStructs(
   id: string,
   d: SourceDef,
   e: SourceDef,
   unresolved?: SourceDef,
): Outcome {
   let outcome: Outcome;
   try {
      assertFilterConditionBindsToDeclaringSource(
         d,
         e,
         d.filterList![0] as FilterCondition,
         unresolved,
      );
      outcome = { kind: "SERVED", detail: "BINDS" };
   } catch (err) {
      const message = (err as Error).message;
      outcome = message.includes(COMPARATOR_REASON)
         ? { kind: "DENIED", detail: message }
         : invalid(err);
   }
   return check(id, outcome);
}

describe("comparator: U1-U4 (query-source join, one compile vs another)", () => {
   const text = (sql: string, pipe: string, on = "deal_id = dfc.deal_id") => ({
      "m.malloy": `
source: dfc is duckdb.sql("${sql}") -> { ${pipe} } extend { primary_key: deal_id }
source: deals_incl is duckdb.sql("select 1 as deal_id") extend {
   join_one: dfc on ${on}
   dimension: is_open is dfc.close_type is null
}
source: deals is deals_incl extend { where: is_open }
`,
   });
   const ONE = "select 1 as deal_id, cast(null as integer) as close_type";
   const BASELINE = text(ONE, "group_by: deal_id, close_type");
   it("U1: identical text, fresh compile (control)", async () => {
      await compare("U1", BASELINE, BASELINE);
   });
   it("U2: different inner SQL", async () => {
      await compare(
         "U2",
         BASELINE,
         text(
            `${ONE} union all select 2, cast(null as integer)`,
            "group_by: deal_id, close_type",
         ),
      );
   });
   it("U3: different pipeline where", async () => {
      await compare(
         "U3",
         BASELINE,
         text(ONE, "group_by: deal_id, close_type; where: deal_id > 5"),
      );
   });
   it("U4: different ON", async () => {
      await compare(
         "U4",
         BASELINE,
         text(
            ONE,
            "group_by: deal_id, close_type",
            "deal_id + 0 = dfc.deal_id",
         ),
      );
   });
});

describe("G: a row-gated source with a misbound where:, on both routes", () => {
   const HEADER = "##! experimental { givens }";
   const body = (derive: string) => `${HEADER}
given:
  LIM :: number
source: deals_incl is ${DEAL} extend { dimension: is_open is not is_deleted  measure: n is count() }
#(access_filter) deal_id = $LIM
source: deals is deals_incl extend { where: is_open }
source: deals_x is deals extend { ${derive} }
export { deals_x }`;
   const RUN = "run: deals_x -> { aggregate: n is count() }";
   it("G1-misbound: the gate admits the caller and the where: is rebound", async () => {
      await serve(
         "G1-misbound",
         {
            "index.malloy": body(
               "except: is_open } extend { dimension: is_open is true",
            ),
         },
         RUN,
         { LIM: 1 },
      );
   });
   it("G1-honest: the same gated source, untouched", async () => {
      await serve(
         "G1-honest",
         { "index.malloy": body("dimension: tag is 1") },
         RUN,
         { LIM: 1 },
      );
   });
});

describe("unsaved model text at scope file and package", () => {
   const model = (derive: string) => `${COMPOSITES.replace(/.*/, "")}
source: dfc is ${DFC}
source: deals_incl is ${DEAL} extend {
   join_one: dfc on deal_id = dfc.deal_id
   dimension: is_open is dfc.close_type is null
   measure: n is count()
}
source: deals is deals_incl extend { where: is_open }
source: deals_x is deals extend { ${derive} }
export { deals_x }
run: deals_x -> { aggregate: n is count() }
`;
   const compileProblems = (text: string, scope: "file" | "package") =>
      withPackage(
         // The saved file already exports the target, so only the unsaved text differs.
         {
            "index.malloy": model("dimension: tag is 1").replace(
               /\nrun:.*\n/,
               "\n",
            ),
         },
         async ({ env }) =>
            (
               await env.compileSource(
                  "pkg",
                  "index.malloy",
                  text,
                  false,
                  {},
                  scope,
               )
            ).problems,
      );
   for (const scope of ["file", "package"] as const) {
      it(`${scope}: an untouched derivation in unsaved text is not refused`, async () => {
         expect(
            await compileProblems(model("dimension: tag is 1"), scope),
         ).toEqual([]);
      });
      it(`${scope}: a rebound derivation in unsaved text is refused`, async () => {
         const outcome = await withDenialSpy(async () => {
            await compileProblems(
               model("except: is_open } extend { dimension: is_open is true"),
               scope,
            );
            return { kind: "SERVED", detail: "compile-ok" };
         });
         expect(outcome.kind).toBe("DENIED");
      });
   }
});

describe("isSameDeclaration", () => {
   const files = {
      "m.malloy": `
source: base is ${DEAL} extend { dimension: is_open is not is_deleted; where: is_open }
source: alias is base
source: renamed is base extend { rename: open_flag is is_open }
source: widened is base extend { dimension: x is 1 }
`,
   };
   it("holds for a copy of the same source, not for an alias or any modification", async () => {
      const c = await structs(files, "m.malloy");
      expect(isSameDeclaration({ ...c.base }, c.base)).toBe(true);
      for (const other of ["alias", "renamed", "widened"]) {
         expect(isSameDeclaration(c[other], c.base)).toBe(false);
         expect(isSameDeclaration({ ...c[other] }, c.base)).toBe(false);
      }
   });
});

describe("copy invariance", () => {
   const FIXTURES: [string, Record<string, string>][] = [
      ...FAMILIES.flatMap(([id, family]) =>
         [false, true].map((twin): [string, Record<string, string>] => [
            `${id}${twin ? "-twin" : ""}`,
            familyFiles(family, twin),
         ]),
      ),
      [
         "B1",
         {
            "index.malloy": `
source: deals_incl is ${DEAL} extend { dimension: is_open is not is_deleted  measure: n is count() }
source: deals is deals_incl extend { where: is_open }
source: deals_x is deals extend { except: is_open } extend { dimension: is_open is true }
source: qs is deals_x -> { group_by: deal_id } extend { primary_key: deal_id }
`,
         },
      ],
   ];
   const verdict = (s: SourceDef, modelDef: ModelDef) => {
      try {
         assertInheritedSourceFiltersBind(s, modelDef);
         return "ok";
      } catch (e) {
         return (e as Error).message;
      }
   };
   for (const [id, files] of FIXTURES) {
      it(`${id}: a registry-style copy of a struct gets the same verdict`, async () => {
         const conn = new DuckDBConnection("duckdb", ":memory:");
         try {
            const model = (await runtimeOver(files, conn).getModel(
               new URL("file:///index.malloy"),
            )) as unknown as { _modelDef: ModelDef };
            const modelDef = model._modelDef;
            for (const [name, s] of Object.entries(modelDef.contents)) {
               if (!("fields" in s)) continue;
               const source = s as SourceDef;
               expect({
                  name,
                  verdict: verdict({ ...source }, modelDef),
               }).toEqual({
                  name,
                  verdict: verdict(source, modelDef),
               });
            }
         } finally {
            await conn.close();
         }
      });
   }
});

describe("minted query-source names", () => {
   it("two compiles of one query source still mint different names", async () => {
      const text = {
         "m.malloy": `source: dfc is duckdb.sql("${CLOSE_ROWS}") -> { group_by: deal_id } extend { primary_key: deal_id }
source: deals is duckdb.sql("${DEAL_ROWS}") extend { join_one: dfc on deal_id = dfc.deal_id }
`,
      };
      const nameOf = async () =>
         (
            (await structs(text, "m.malloy")).deals.fields.find(
               (f) => (f as { as?: string }).as === "dfc",
            ) as { name: string }
         ).name;
      const [first, second] = [await nameOf(), await nameOf()];
      expect(first).toMatch(/^QuerySource-[0-9a-f-]{36}$/);
      expect(second).not.toBe(first);
   });
});

describe("comparator: B2 joined composites under one alias", () => {
   const text = (rows: string) => ({
      "m.malloy": `${COMPOSITES}
source: m1 is duckdb.sql("${rows}") extend { primary_key: deal_id }
source: m2 is duckdb.sql("${rows}") extend { primary_key: deal_id; dimension: other is 1 }
source: cmp is compose(m1, m2)
source: deals_incl is duckdb.sql("select 1 as deal_id") extend {
   join_one: cmp on deal_id = cmp.deal_id
   dimension: is_open is cmp.close_type is null
}
source: deals is deals_incl extend { where: is_open }
`,
   });
   it("B2-cmp-ctl: identical composite, fresh compile", async () => {
      await compare("B2-cmp-ctl", text(CLOSE_ROWS), text(CLOSE_ROWS));
   });
   it("B2-cmp-twin: composite members read different rows", async () => {
      await compare("B2-cmp-twin", text(CLOSE_ROWS), text(NULL_ROWS));
   });
   const joinedType = (s: SourceDef | undefined) =>
      (
         s?.fields.find((x) => (x as { as?: string }).as === "cmp") as
            | { type?: string }
            | undefined
      )?.type;
   it("A5-resolved-ctl: the declared joined composite vs the composite-resolved run target", async () => {
      const { declared, resolved } = await resolvedTarget(
         text(CLOSE_ROWS),
         "m.malloy",
         "deals",
      );
      expect(joinedType(declared)).toBe("composite");
      expect(joinedType(resolved)).not.toBe("composite");
      compareStructs("A5-resolved-ctl", declared, resolved!, declared);
   });
});

describe("comparator: B8 parameter bindings", () => {
   const PARAM = "##! experimental { parameters }";
   const files = (
      param: string,
      arg: number,
      split: boolean,
   ): Record<string, string> => {
      const base = `${PARAM}
source: base(${param}::number) is duckdb.sql("${CLOSE_ROWS}") extend { where: deal_id >= ${param} }`;
      const body = `${PARAM}
source: dfc is base(${param} is ${arg}) -> { group_by: deal_id, close_type } extend { primary_key: deal_id }
source: deals_incl is duckdb.sql("select 1 as deal_id") extend {
   join_one: dfc on deal_id = dfc.deal_id
   dimension: is_open is dfc.close_type is null
}
source: deals is deals_incl extend { where: is_open }
`;
      return split
         ? {
              "base.malloy": base,
              "m.malloy": `${PARAM}\nimport "base.malloy"\n${body}`,
           }
         : { "m.malloy": `${base}\n${body}` };
   };
   for (const param of ["accessModifier", "annotations"]) {
      for (const split of [false, true]) {
         const tag = `${param}-${split ? "split" : "same"}`;
         it(`B8-ctl-${tag}: same binding`, async () => {
            await compare(
               `B8-ctl-${tag}`,
               files(param, 1, split),
               files(param, 1, split),
            );
         });
         it(`B8-twin-${tag}: different binding`, async () => {
            await compare(
               `B8-twin-${tag}`,
               files(param, 1, split),
               files(param, 3, split),
            );
         });
      }
   }
});

describe("comparator: named arguments are unordered", () => {
   const PARAM = "##! experimental { parameters }";
   const files = (args: string): Record<string, string> => ({
      "m.malloy": `${PARAM}
source: base(a::number, b::number) is duckdb.sql("${CLOSE_ROWS}") extend { where: deal_id >= a and deal_id <= b }
source: dfc is base(${args}) -> { group_by: deal_id, close_type } extend { primary_key: deal_id }
source: deals_incl is duckdb.sql("select 1 as deal_id") extend {
   join_one: dfc on deal_id = dfc.deal_id
   dimension: is_open is dfc.close_type is null
}
source: deals is deals_incl extend { where: is_open }
`,
   });
   it("B8-order-reordered: the same arguments in another order", async () => {
      await compare(
         "B8-order-reordered",
         files("a is 1, b is 2"),
         files("b is 2, a is 1"),
      );
   });
   it("B8-order-swapped: the same values bound to the other names", async () => {
      await compare(
         "B8-order-swapped",
         files("a is 1, b is 2"),
         files("a is 2, b is 1"),
      );
   });
});

describe("comparator: matrixOperation (experimental join_types)", () => {
   const text = (mop: string) => ({
      "m.malloy": `##! experimental { join_types }
source: dfc is duckdb.sql("${CLOSE_ROWS}") -> { group_by: deal_id, close_type } extend { primary_key: deal_id }
source: deals_incl is duckdb.sql("select 1 as deal_id union all select 9") extend {
   join_one: dfc ${mop} on deal_id = dfc.deal_id
   dimension: is_open is dfc.close_type is null
}
source: deals is deals_incl extend { where: is_open }
`,
   });
   it("C-matrix-ctl: same join type", async () => {
      await compare("C-matrix-ctl", text("left"), text("left"));
   });
   it("C-matrix-twin: left vs inner", async () => {
      await compare("C-matrix-twin", text("left"), text("inner"));
   });
   it("the compiled join carries the operation the comparator reads", async () => {
      const op = async (mop: string) =>
         (
            (await structs(text(mop), "m.malloy")).deals_incl.fields.find(
               (f) => (f as { as?: string }).as === "dfc" || f.name === "dfc",
            ) as unknown as { matrixOperation?: string }
         ).matrixOperation;
      expect([await op("left"), await op("inner")]).toEqual(["left", "inner"]);
   });
});

/** Ids whose `/compile` verdict may differ from the query route, each with why. */
const COMPILE_DIVERGENCE_ALLOWED: Record<string, string> = {};

const EXPECTED: Record<string, Kind> = {
   "B8-order-reordered": "SERVED",
   "B8-order-swapped": "DENIED",
   "B2b-split-honest": "SERVED",
   "B2b-split-same": "SERVED",
   "B2b-split-reordered": "DENIED",
   "B2b-member-m2": "DENIED",
   "B2b-member-m1": "DENIED",
   "B2b-op-same": "SERVED",
   "B2b-op-cmp-full": "DENIED",
   "B2b-op-cmp-inner": "DENIED",
   "B2b-op-m1-full": "DENIED",
   "B2b-op-m1-inner": "DENIED",
   "B2b-op-m1-right": "DENIED",
   "X1-misbound": "REFUSED",
   "X3-query-source-misbound": "REFUSED",
   "G1-misbound": "DENIED",
   "G1-honest": "SERVED",
   "CAL-C": "SERVED",
   "CAL-G": "SERVED",
   A0: "SERVED",
   "A0-twin": "DENIED",
   A1: "SERVED",
   "A1-twin": "DENIED",
   A2: "SERVED",
   "A2-twin": "DENIED",
   A3: "SERVED",
   "A3-twin": "DENIED",
   A4a: "SERVED",
   "A4a-twin": "DENIED",
   A4b: "SERVED",
   "A4b-twin": "DENIED",
   "A5-direct": "SERVED",
   A5: "SERVED",
   "A5-samefile": "SERVED",
   B2: "DENIED",
   A6a: "SERVED",
   A6b: "SERVED",
   A6c: "SERVED",
   A6e: "SERVED",
   A6f: "SERVED",
   A6g: "SERVED",
   A6d: "SERVED",
   "B1-direct": "DENIED",
   "B1-run-misbound": "DENIED",
   "B1-join-misbound": "DENIED",
   "B1-nested-misbound": "DENIED",
   "B1-composite-misbound": "DENIED",
   "B1-run-honest": "SERVED",
   "B1-join-honest": "SERVED",
   "B1-nested-honest": "SERVED",
   "B1-composite-honest": "SERVED",
   "B3-same-ctl": "SERVED",
   "B3-same-U2": "DENIED",
   "B3-same-U3": "DENIED",
   "B3-same-U4": "DENIED",
   "B3-same-J-inline": "DENIED",
   "B3-split-ctl": "SERVED",
   "B3-split-U2": "DENIED",
   "B3-split-U3": "DENIED",
   "B3-split-U4": "DENIED",
   "B3-split-J-inline": "DENIED",
   "B7-stage-misbound": "DENIED",
   "B7-stage-honest": "SERVED",
   "B7-nest-misbound": "DENIED",
   "B7-nest-honest": "SERVED",
   "B7-view-misbound": "DENIED",
   "B7-view-honest": "SERVED",
   "B7-stage2-misbound": "DENIED",
   "B7-stage2-honest": "SERVED",
   "B7-query-source-misbound": "DENIED",
   "B7-query-source-honest": "SERVED",
   "B8-e2e-ctl": "SERVED",
   "B8-e2e-twin": "DENIED",
   "B9-misbound": "DENIED",
   "B9-honest": "SERVED",
   "C-nb-B1-misbound": "DENIED",
   "C-nb-B7-misbound": "DENIED",
   "C-nb-B1-honest": "SERVED",
   "C-nb-B7-honest": "SERVED",
   "C-nb-direct": "DENIED",
   "C-givens": "SERVED",
   "C-givens-twin": "DENIED",
   "C-ref-same": "SERVED",
   "C-ref-moved": "SERVED",
   "C-ref-moved-twin": "DENIED",
   U1: "SERVED",
   U2: "DENIED",
   U3: "DENIED",
   U4: "DENIED",
   "B2-cmp-ctl": "SERVED",
   "B2-cmp-twin": "DENIED",
   "A5-resolved-ctl": "SERVED",
   "B8-ctl-accessModifier-same": "SERVED",
   "B8-twin-accessModifier-same": "DENIED",
   "B8-ctl-accessModifier-split": "SERVED",
   "B8-twin-accessModifier-split": "DENIED",
   "B8-ctl-annotations-same": "SERVED",
   "B8-twin-annotations-same": "DENIED",
   "B8-ctl-annotations-split": "SERVED",
   "B8-twin-annotations-split": "DENIED",
   "C-matrix-ctl": "SERVED",
   "C-matrix-twin": "DENIED",
};
