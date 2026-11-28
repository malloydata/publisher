// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An index query (`queryClass: index`) on a source that declares a stored
 * table of its own is answered from that table or refused, never run live:
 *
 *  (a) a stored answer is unchanged;
 *  (c) no ready table: 409 `INDEX_SOURCE_UNBUILT`, with `unbuiltReason`
 *      (`unbuilt`, or `refused` for a declaration the plan refused);
 *  (b) HELD: a ready table is bound and the served copy declines it, which
 *      is still answered live;
 *  (d) a source with no declaration of its own (an extension that only
 *      inherits one, a plain source), or a query of another class, is
 *      unchanged.
 *
 * Governance decides first: an off-surface source or file answers its 404,
 * and a gate its 403, exactly as an interactive query does, so the refusal
 * never reveals a source the caller may not query, or that it has a table.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import type { GivenValue } from "@malloydata/malloy";
import {
   IndexSourceUnbuiltError,
   internalErrorToHttpError,
} from "../../../src/errors";
import { unrecordedAudit } from "../../../src/service/access_record";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import type { QueryClass } from "../../../src/service/query_metadata";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
} from "../../../src/test_helpers/conformance/package_runner";

const EV =
   "id,kind,ts\n1,a,2024-03-15 01:45:10\n2,b,2024-03-15 03:05:00\n3,a,2024-11-03 05:30:00\n";

const FILES: Record<string, string> = {
   "ev.csv": EV,
   "base.malloy": `source: ev_base is duckdb.table('ev.csv') extend {
  dimension: ts_day is ts.day
}
`,
   "m.malloy": `##! experimental.givens
import "base.malloy"

given: ROLE :: string
given: KINDS :: string[]
given: ORG :: string

#(materialize) storage=lake
source: mm is ev_base extend { measure: n is count() }

source: mm_ext is mm extend { measure: k is count() }

source: plain is duckdb.table('ev.csv') extend { measure: n is count() }

#@ persist name="p_t"
source: p is duckdb.table('ev.csv') -> { group_by: kind; aggregate: c is count() }

#(materialize) storage=lake
source: secret is duckdb.table('ev.csv') extend { measure: n is count() }

#(authorize) 'analyst' = $ROLE
#(materialize) storage=lake
source: locked is duckdb.table('ev.csv') extend { measure: n is count() }

#(access_filter) kind in $KINDS
#(materialize) storage=lake
source: scoped is duckdb.table('ev.csv') extend { measure: n is count() }

source: org_rows is duckdb.table('ev.csv') extend { where: kind = $ORG }

// Query-shaped over a base whose where: reads a given: the plan refuses it
// (reads_given), so no table of it is ever built here.
#(materialize) storage=lake
source: org_kinds is org_rows -> { group_by: id, kind }
`,
   "index.malloy": `##! experimental.givens
import "m.malloy"
import { pub is secret } from "m.malloy"
export { mm, mm_ext, plain, p, locked, scoped, org_kinds, pub }
`,
};

type Run = { servedBy: string; rows: string } | { error: Error };

/** The rows as a sorted list of JSON lines: an index query's rows come in no order. */
function rowsOf(rows: unknown): string {
   const line = (row: unknown) =>
      JSON.stringify(row, (_k, v: unknown) =>
         typeof v === "bigint" ? Number(v) : v,
      );
   return JSON.stringify(
      (Array.isArray(rows) ? rows : [rows]).map(line).sort(),
   );
}

/** The HTTP answer a run's error maps to, or the run's own shape. */
function http(run: Run) {
   if (!("error" in run)) return run;
   const { status, json } = internalErrorToHttpError(run.error, { log: false });
   return { status, json };
}

describe("index queries on a source that declares a stored table", () => {
   let restore: () => void;
   let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
   let fixture: LoadedFixture | undefined;

   async function run(
      malloy: string,
      queryClass?: QueryClass,
      options: {
         model?: string;
         givens?: Record<string, GivenValue>;
      } = {},
   ): Promise<Run> {
      const model = loader!.pkg.getModel(options.model ?? "index.malloy");
      if (!model) throw new Error("model did not load");
      try {
         const result = await model.runQuery(
            unrecordedAudit(),
            {
               query: malloy,
               ...(options.givens ? { givens: options.givens } : {}),
               ...(queryClass ? { queryMetadataInput: { queryClass } } : {}),
            },
            QueryPrivileges.none,
         );
         return {
            servedBy:
               result.path?.servedBy?.via ??
               result.path?.servedBy?.kind ??
               "live",
            rows: rowsOf(result.compactResult),
         };
      } catch (error) {
         return { error: error as Error };
      }
   }

   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await freshLoadPool();
      loader = await loadFixture(FILES);
   }, 600_000);

   afterAll(async () => {
      await fixture?.dispose();
      await loader?.dispose().catch(() => {});
      restore?.();
      await freshLoadPool();
   });

   describe("before anything is built", () => {
      it("(c) refuses an index query on a #(materialize) source: 409 INDEX_SOURCE_UNBUILT, unbuilt", async () => {
         const answer = await run("run: mm -> { index: * }", "index");
         expect("error" in answer && answer.error).toBeInstanceOf(
            IndexSourceUnbuiltError,
         );
         expect(http(answer)).toMatchObject({
            status: 409,
            json: {
               code: 409,
               reason: "INDEX_SOURCE_UNBUILT",
               unbuiltReason: "unbuilt",
            },
         });
      });

      it("names the source the caller queried, and no table, path or connection", async () => {
         const answer = await run("run: mm -> { index: * }", "index");
         const message = "error" in answer ? answer.error.message : "";
         expect(message).toContain('Source "mm"');
         for (const leak of ["lake", "file:", "ev.csv", "duckdb", "__"]) {
            expect(message).not.toContain(leak);
         }
      });

      it("(c) refuses an index query on a source's own #@ persist", async () => {
         expect(
            http(await run("run: p -> { index: * }", "index")),
         ).toMatchObject({
            status: 409,
            json: { reason: "INDEX_SOURCE_UNBUILT", unbuiltReason: "unbuilt" },
         });
      });

      it("(c) refuses a named view or a filtered index query alike", async () => {
         expect(
            http(
               await run(
                  "run: mm -> { where: kind = 'a'; index: kind }",
                  "index",
               ),
            ),
         ).toMatchObject({ status: 409 });
      });

      it("(d) answers an extension that only inherits the declaration live", async () => {
         const answer = await run("run: mm_ext -> { index: * }", "index");
         expect(answer).toMatchObject({ servedBy: "live" });
      });

      it("(d) answers a source with no declaration live", async () => {
         const answer = await run("run: plain -> { index: * }", "index");
         expect(answer).toMatchObject({ servedBy: "live" });
      });

      it("(d) answers the same query of any other class live", async () => {
         for (const queryClass of [undefined, "interactive", "ops"] as const) {
            const answer = await run("run: mm -> { index: * }", queryClass);
            expect(answer).toMatchObject({ servedBy: "live" });
         }
      });

      it("names an import rename as the caller queried it, never the off-surface source it renames", async () => {
         // `pub` renames `secret`, which is stored and off the surface, and
         // keeps its sourceID: the refusal must say `pub`.
         expect(await run("run: pub -> { group_by: kind }")).toMatchObject({
            servedBy: "live",
         });
         const answer = await run("run: pub -> { index: * }", "index");
         expect(http(answer)).toMatchObject({
            status: 409,
            json: { reason: "INDEX_SOURCE_UNBUILT", unbuiltReason: "unbuilt" },
         });
         const message = "error" in answer ? answer.error.message : "";
         expect(message).toContain('Source "pub"');
         expect(message).not.toContain("secret");
      });

      it("governance first: an off-surface source answers its 404, not the 409", async () => {
         const interactive = http(await run("run: secret -> { index: * }"));
         const index = http(await run("run: secret -> { index: * }", "index"));
         expect(interactive).toMatchObject({ status: 404 });
         expect(index).toEqual(interactive);
      });

      it("governance first: a file off the surface answers its 404, not the 409", async () => {
         const interactive = http(
            await run("run: mm -> { index: * }", undefined, {
               model: "m.malloy",
            }),
         );
         const index = http(
            await run("run: mm -> { index: * }", "index", {
               model: "m.malloy",
            }),
         );
         expect(interactive).toMatchObject({ status: 404 });
         expect(index).toEqual(interactive);
      });

      it("governance first: an #(authorize) lock that does not admit the caller answers its 403", async () => {
         for (const givens of [undefined, { ROLE: "viewer" }]) {
            const interactive = http(
               await run("run: locked -> { index: * }", undefined, { givens }),
            );
            const index = http(
               await run("run: locked -> { index: * }", "index", { givens }),
            );
            expect(interactive).toMatchObject({ status: 403 });
            expect(index).toEqual(interactive);
         }
      });

      it("governance first: a row-level gate whose given is missing answers its 403", async () => {
         const interactive = http(await run("run: scoped -> { index: * }"));
         const index = http(await run("run: scoped -> { index: * }", "index"));
         expect(interactive).toMatchObject({ status: 403 });
         expect(index).toEqual(interactive);
      });

      it("(c) refuses a caller the gates admit, on a gated source", async () => {
         const locked = http(
            await run("run: locked -> { index: * }", "index", {
               givens: { ROLE: "analyst" },
            }),
         );
         expect(locked).toMatchObject({
            status: 409,
            json: { reason: "INDEX_SOURCE_UNBUILT" },
         });
         expect(
            (locked as { json: { unbuiltReason?: string } }).json.unbuiltReason,
         ).toBe("unbuilt");
         expect(
            http(
               await run("run: scoped -> { index: * }", "index", {
                  givens: { KINDS: ["a"] },
               }),
            ),
         ).toMatchObject({
            status: 409,
            json: { reason: "INDEX_SOURCE_UNBUILT" },
         });
      });
   });

   it("(c) says refused for a declaration the plan refused", async () => {
      expect(
         Object.keys(
            loader!.pkg.materialization().getBuildPlan()?.refusedSources ?? {},
         ).filter((id) => id.startsWith("org_kinds@")),
      ).toHaveLength(1);
      expect(
         http(
            await run("run: org_kinds -> { index: * }", "index", {
               givens: { ORG: "a" },
            }),
         ),
      ).toMatchObject({
         status: 409,
         json: { reason: "INDEX_SOURCE_UNBUILT", unbuiltReason: "refused" },
      });
   });

   describe("once built and bound", () => {
      beforeAll(async () => {
         fixture = await loader!.bind();
      }, 600_000);

      it("(a) answers an index query on a #(materialize) source from its table, as before", async () => {
         const live = await run("run: mm -> { index: * }");
         const index = await run("run: mm -> { index: * }", "index");
         expect(index).toMatchObject({ servedBy: "served_copy" });
         expect(index).toEqual(live);
      });

      it("(a) answers an index query on a source's own #@ persist from its table", async () => {
         const index = await run("run: p -> { index: * }", "index");
         expect("servedBy" in index && index.servedBy).not.toBe("live");
      });

      it("(b) HELD: a bound table the served copy declines still answers live", async () => {
         // The query computes in another zone than the table was built in,
         // so the served copy declines it. Not refused: case (b) is held,
         // and keeps today's live answer.
         const malloy =
            "run: mm -> { timezone: 'Etc/UTC'; group_by: ts_day; aggregate: n }";
         const interactive = await run(malloy);
         const index = await run(malloy, "index");
         expect(index).toMatchObject({ servedBy: "live" });
         expect(index).toEqual(interactive);
      });

      it("(c) still refuses the refused declaration, which no build binds", async () => {
         expect(
            http(
               await run("run: org_kinds -> { index: * }", "index", {
                  givens: { ORG: "a" },
               }),
            ),
         ).toMatchObject({ status: 409, json: { unbuiltReason: "refused" } });
      });

      it("(d) still answers an inheriting extension and a plain source as before", async () => {
         expect(await run("run: plain -> { index: * }", "index")).toMatchObject(
            { servedBy: "live" },
         );
         const ext = await run("run: mm_ext -> { index: * }", "index");
         expect("error" in ext).toBe(false);
      });
   });
});

describe("index queries with materialization v2 off", () => {
   let restore: () => void;
   let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;

   beforeAll(async () => {
      // PUBLISHER_MATERIALIZE=false reads `#(materialize)` as not written,
      // and the plan is what it was without it: nothing is declared.
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "false" },
      }).restore;
      await freshLoadPool();
      loader = await loadFixture({
         "ev.csv": EV,
         "m.malloy": `#(materialize) storage=lake
source: mm is duckdb.table('ev.csv') extend { measure: n is count() }
`,
      });
   }, 600_000);

   afterAll(async () => {
      await loader?.dispose().catch(() => {});
      restore?.();
      await freshLoadPool();
   });

   it("(d) answers an index query on a #(materialize) source live, as before", async () => {
      const result = await loader!.pkg.getModel("m.malloy")!.runQuery(
         unrecordedAudit(),
         {
            query: "run: mm -> { index: * }",
            queryMetadataInput: { queryClass: "index" },
         },
         QueryPrivileges.none,
      );
      expect(result.path?.servedBy?.kind ?? "live").toBe("live");
   });
});
