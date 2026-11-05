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
import type { Model } from "./model";

const MODEL = `##! experimental { access_modifiers }
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
      "an index over a classified field",
      "run: s extend { dimension: x is p.ssn } -> { index: x }",
      "cannot see completely",
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
