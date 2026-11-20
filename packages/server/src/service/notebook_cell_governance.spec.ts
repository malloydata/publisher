// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The gates a linear report's query cell runs under: a row filter grafted
 * onto the cell, a lock that admits or denies it, and a cell that reads no
 * gate. Pins each cell's rows, its denial, and the access summary the cell
 * run records, over a model compiled from files. A cell runs as its report's
 * viewer runs it, through `runQuery` on the report's path.
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { AccessDeniedError, BadRequestError } from "../errors";
import { unrecordedAudit } from "./access_record";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { Connection } from "@malloydata/malloy";
import { Model, ModelPolicy } from "./model";
import { seededDuckdb, writeTempPackage } from "../test_helpers/temp_package";

const SEED_SQL = `
CREATE OR REPLACE TABLE orgtable (id INTEGER, org_id INTEGER, val VARCHAR);
INSERT INTO orgtable VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 2, 'c'), (4, 2, 'd');
`;

const MODEL = `##! experimental.givens

#(secure)
given:
  GROUPS :: number[]

#(secure)
given:
  ROLE :: string

#(access_filter) org_id in $GROUPS
source: gated is duckdb.table('orgtable') extend {
  measure: c is count()
}

#(authorize) 'admin' = $ROLE
source: locked is duckdb.table('orgtable') extend {
  measure: c is count()
}

#(authorize) 'admin' = $ROLE
#(access_filter) org_id in $GROUPS
source: both is duckdb.table('orgtable') extend {
  measure: c is count()
}

#(authorize) 'admin' = $ROLE
#(filter) name=org dimension=org_id type=equal implicit required
source: refined is duckdb.table('orgtable') extend {
  measure: c is count()
}

source: plain is duckdb.table('orgtable') extend {
  measure: c is count()
}
`;

const REPORT = `## artifact { layout=linear title="Cells" tiles=["gated -> { aggregate: c }", "locked -> { aggregate: c }", "both -> { aggregate: c }", "plain -> { aggregate: c }", "refined -> { aggregate: c }"] }
import "m.malloy"
`;

/** The single count a cell's aggregate returned. */
function countOf(cell: { compactResult?: unknown }): number {
   const rows = (cell.compactResult ?? []) as { c: number }[];
   expect(rows.length).toBe(1);
   return Number(rows[0].c);
}

describe("a linear report's query cells under the model's gates", () => {
   let model: Model;
   let duckdb: DuckDBConnection;
   let removeDir: () => void;
   let cellTexts: string[] = [];
   const cells = {
      gated: -1,
      locked: -1,
      both: -1,
      plain: -1,
      refined: -1,
   };

   beforeAll(async () => {
      const written = writeTempPackage({
         "m.malloy": MODEL,
         "report.malloy": REPORT,
      });
      removeDir = written.cleanup;
      duckdb = await seededDuckdb(SEED_SQL);
      model = await Model.create(
         "cells-pkg",
         written.dir,
         "report.malloy",
         new Map<string, Connection>([["duckdb", duckdb]]),
         {
            policy: ModelPolicy.fixed({
               queryBoundary: {
                  mode: "all",
                  exploresDeclared: false,
                  isQueryEntryPoint: true,
                  layout: "linear",
               },
            }),
         },
      );
      expect(
         model.attachServedNotebookCells(model.getCompiledSourceText()!),
      ).toBe("ok");
      const listed = (await model.getNotebook()).notebookCells ?? [];
      cellTexts = listed.map((cell) => cell.text ?? "");
      for (const name of Object.keys(cells) as (keyof typeof cells)[]) {
         cells[name] = listed.findIndex(
            (cell) =>
               cell.kind === "query" &&
               cell.text === `run: ${name} -> { aggregate: c }`,
         );
         expect(cells[name]).toBeGreaterThanOrEqual(0);
      }
   });

   afterAll(async () => {
      await duckdb?.close();
      removeDir?.();
   });

   const run = (
      index: number,
      givens?: Record<string, unknown>,
      audit = unrecordedAudit(),
      filterParams?: Record<string, unknown>,
   ) =>
      model.runQuery(audit, {
         query: cellTexts[index],
         givens: givens as never,
         filterParams: filterParams as never,
      });

   describe("a row-filtered cell", () => {
      it("grafts the caller's groups onto the cell", async () => {
         // Org 1 holds one row and org 2 three, so each count names the org.
         expect(countOf(await run(cells.gated, { GROUPS: [1] }))).toBe(1);
         expect(countOf(await run(cells.gated, { GROUPS: [2] }))).toBe(3);
         expect(countOf(await run(cells.gated, { GROUPS: [1, 2] }))).toBe(4);
         expect(countOf(await run(cells.gated, { GROUPS: [] }))).toBe(0);
      });

      it("denies a caller who supplies no groups with the opaque 403 the query path gives, naming no given", async () => {
         const refused = await run(cells.gated).then(
            () => undefined,
            (err: unknown) => err,
         );
         expect(refused).toBeInstanceOf(AccessDeniedError);
         expect((refused as Error).message).not.toContain("GROUPS");
         expect((refused as Error).message).toBe(
            AccessDeniedError.forSource("gated").message,
         );
      });

      it("records nothing for a cell denied an unsupplied given", async () => {
         const audit = unrecordedAudit();
         await expect(
            run(cells.gated, undefined, audit),
         ).rejects.toBeInstanceOf(AccessDeniedError);
         expect(audit.summary).toBeUndefined();
      });

      it("records the graft as a filtered decision, naming the filter and the given it read", async () => {
         const audit = unrecordedAudit();
         await run(cells.gated, { GROUPS: [1] }, audit);
         expect(audit.summary).toMatchObject({
            source: "gated",
            decision: "filtered",
            gates: { authorize: [], accessFilter: ["org_id in $GROUPS"] },
            gateGivenNames: ["GROUPS"],
            givenNames: ["GROUPS"],
            secureGivens: { GROUPS: [1] },
            rowCount: 1,
         });
      });
   });

   describe("a locked cell", () => {
      it("runs for a caller the lock admits", async () => {
         expect(countOf(await run(cells.locked, { ROLE: "admin" }))).toBe(4);
      });

      it("denies a caller the lock refuses, or one who supplies no role", async () => {
         await expect(
            run(cells.locked, { ROLE: "guest" }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
         await expect(run(cells.locked)).rejects.toBeInstanceOf(
            AccessDeniedError,
         );
      });

      it("records an admitted decision with no graft", async () => {
         const audit = unrecordedAudit();
         await run(cells.locked, { ROLE: "admin" }, audit);
         expect(audit.summary).toMatchObject({
            source: "locked",
            decision: "admitted",
            gates: { authorize: ["'admin' = $ROLE"], accessFilter: [] },
            gateGivenNames: [],
         });
      });

      it("records nothing for a denied cell", async () => {
         const audit = unrecordedAudit();
         await expect(
            run(cells.locked, { ROLE: "guest" }, audit),
         ).rejects.toBeInstanceOf(AccessDeniedError);
         expect(audit.summary).toBeUndefined();
      });
   });

   describe("a cell under a lock and a row filter", () => {
      it("admits only past the lock, then needs the groups its filter reads", async () => {
         expect(
            countOf(await run(cells.both, { ROLE: "admin", GROUPS: [2] })),
         ).toBe(3);
         expect(
            countOf(await run(cells.both, { ROLE: "admin", GROUPS: [1] })),
         ).toBe(1);
         await expect(
            run(cells.both, { ROLE: "guest", GROUPS: [1, 2] }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
         // Past the lock, an unsupplied filter given is denied as the query
         // path denies it.
         await expect(
            run(cells.both, { ROLE: "admin" }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      });

      it("records a filtered decision carrying both gates", async () => {
         const audit = unrecordedAudit();
         await run(cells.both, { ROLE: "admin", GROUPS: [2] }, audit);
         expect(audit.summary).toMatchObject({
            source: "both",
            decision: "filtered",
            gates: {
               authorize: ["'admin' = $ROLE"],
               accessFilter: ["org_id in $GROUPS"],
            },
            gateGivenNames: ["GROUPS"],
         });
      });
   });

   describe("an ungated cell", () => {
      it("runs with no givens and records an open decision", async () => {
         const audit = unrecordedAudit();
         expect(countOf(await run(cells.plain, undefined, audit))).toBe(4);
         expect(audit.summary).toMatchObject({
            source: "plain",
            decision: "open",
            gates: { authorize: [], accessFilter: [] },
         });
      });
   });

   describe("a locked cell its source's #(filter) refines", () => {
      it("applies the caller's filter value for a caller the lock admits", async () => {
         expect(
            countOf(
               await run(cells.refined, { ROLE: "admin" }, undefined, {
                  org: "2",
               }),
            ),
         ).toBe(3);
         expect(
            countOf(
               await run(cells.refined, { ROLE: "admin" }, undefined, {
                  org: "1",
               }),
            ),
         ).toBe(1);
      });

      it("denies a refused caller with a 403 before it validates the filter values", async () => {
         await expect(
            run(cells.refined, { ROLE: "guest" }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
         await expect(
            run(cells.refined, { ROLE: "guest" }, undefined, { org: "2" }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      });

      it("refuses a numeric filter value with a bad request naming the filter and the type it takes", async () => {
         const refused = await run(
            cells.refined,
            { ROLE: "admin" },
            undefined,
            {
               org: 2,
            },
         ).then(
            () => undefined,
            (err: unknown) => err,
         );
         expect(refused).toBeInstanceOf(BadRequestError);
         expect((refused as Error).message).toBe(
            'Filter "org" takes a string or an array of strings, not a number',
         );
      });

      it("denies a refused caller a 403 before it validates a numeric filter value", async () => {
         await expect(
            run(cells.refined, { ROLE: "guest" }, undefined, { org: 2 }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      });

      it("answers an admitted caller missing a required filter with a bad request", async () => {
         await expect(
            run(cells.refined, { ROLE: "admin" }),
         ).rejects.toBeInstanceOf(BadRequestError);
      });
   });
});
