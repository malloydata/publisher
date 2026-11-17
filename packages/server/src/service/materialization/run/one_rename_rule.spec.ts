// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { logger } from "../../../logger";
import type { Materialization } from "../../../storage/DatabaseInterface";
import type { BuildPlan, ManifestEntry } from "../domain/types";
import {
   keptTableWarnings,
   TABLE_KEPT_AFTER_RENAME_CODE,
} from "../package_warnings";
import {
   KEPT_TABLES_KEY,
   keptTablesResolverFor,
   Reclaimer,
   RENAMED_TABLES_KEY,
} from "./reclaimer";
import type { KeptTable } from "../domain/types";
import type { RenamedTable } from "./run_instructions";

/**
 * The one rename rule, decided by Reclaimer.reclaimRenamedTables over a fake
 * store: an auto-run reclaims a table it replaced only when it can prove the
 * table is its own (a Publisher suffix, this package's own auto-run built it
 * there, nothing else names it, the replacement committed and is served),
 * keeps and lists one at an author's exact `name=` or in a destination no
 * longer configured, and never drops anything else. What the derive offers is
 * self_table_naming.spec.ts's; the drops on a real warehouse are
 * one_rename_rule_duckdb.spec.ts's and the Postgres integration spec's.
 */

const CONN = "warehouse";

const entry = (
   table: string,
   sourceEntityId = "a1",
   storageDestinationName?: string,
): ManifestEntry =>
   ({
      sourceEntityId,
      physicalTableName: table,
      connectionName: CONN,
      ...(storageDestinationName ? { storageDestinationName } : {}),
   }) as ManifestEntry;

const renamed = (
   previousName: string,
   physicalTableName: string,
   extra: Partial<RenamedTable> = {},
): RenamedTable => ({
   sourceEntityId: "a1",
   sourceName: "summary",
   previousName,
   physicalTableName,
   connectionName: CONN,
   ...extra,
});

let clock = 0;
function run(
   id: string,
   entries: Record<string, ManifestEntry>,
   options: {
      status?: Materialization["status"];
      metadata?: Record<string, unknown>;
      version?: string | null;
   } = {},
): Materialization {
   clock += 1000;
   return {
      id,
      status: options.status ?? "MANIFEST_FILE_READY",
      version: options.version ?? null,
      metadata: options.metadata ?? { mode: "auto" },
      manifest: { entries } as never,
      createdAt: new Date(clock),
      completedAt: new Date(clock),
   } as unknown as Materialization;
}

const recording = (...tables: RenamedTable[]) => ({
   mode: "auto",
   [RENAMED_TABLES_KEY]: tables,
});

/** Reclaim as `runId` over `runs`: what was dropped, kept, and recorded. */
async function reclaim(
   runs: Materialization[],
   runId: string,
   options: {
      reader?: { versionId?: string; owned?: string };
      /** The storage destinations configured; every other one throws. */
      destinations?: string[];
      /** Tables the drop leaves in place (another package's mark). */
      marked?: string[];
      bound?: boolean;
   } = {},
) {
   const dropped: string[] = [];
   const records = new Map(runs.map((m) => [m.id, m.metadata ?? null]));
   const repository = {
      getEnvironmentByName: async () => ({ id: "env-1" }),
      listMaterializations: async () =>
         runs.map((m) => ({ ...m, metadata: records.get(m.id) ?? null })),
      updateMaterialization: async (
         id: string,
         update: { metadata?: Record<string, unknown> },
      ) => {
         records.set(id, update.metadata ?? null);
         return {} as Materialization;
      },
      deleteIncrementalLedgerEntry: async () => undefined,
   };
   const configured = new Set(options.destinations ?? []);
   const reclaimer = new Reclaimer({
      repository: () => repository as never,
      host: {
         getEnvironment: async () =>
            ({
               getStorageDestination: (name: string) => {
                  if (!configured.has(name)) throw new Error("not configured");
                  return { name };
               },
               getPackage: async () => ({
                  getMalloyConnection: async () => undefined,
               }),
               peekVersion: () => ({
                  getMalloyConnection: async () => undefined,
               }),
            }) as never,
      } as never,
      lifecycle: {} as never,
      dropManifestEntries: async (
         _e,
         _en,
         _p,
         _id,
         occasion,
         entries,
         _ref,
         _conn,
         _views,
         skipped,
      ) => {
         expect(occasion).toBe("reclaim");
         for (const e of entries) {
            // The ownership mark: a table another package marked is left.
            if (options.marked?.includes(e.physicalTableName)) {
               skipped?.push(e);
               continue;
            }
            dropped.push(
               `${e.storageDestinationName ?? CONN}:${e.physicalTableName}`,
            );
         }
         return [];
      },
   });
   const kept = await reclaimer.reclaimRenamedTables(
      "e1",
      "pa",
      runId,
      options.reader ?? {},
      async () => options.bound ?? true,
   );
   return {
      dropped,
      kept,
      renamedOn: (id: string) =>
         (records.get(id)?.[RENAMED_TABLES_KEY] as
            | RenamedTable[]
            | undefined) ?? [],
      keptOn: (id: string) =>
         records.get(id)?.[KEPT_TABLES_KEY] as KeptTable[] | undefined,
   };
}

let guards: string[];
let infoSpy: ReturnType<typeof spyOn>;
beforeEach(() => {
   guards = [];
   infoSpy = spyOn(logger, "info").mockImplementation(((
      message: string,
      fields?: { guard?: string },
   ) => {
      if (message === "Keeping a table a readable name replaced") {
         guards.push(String(fields?.guard));
      }
   }) as never);
});
afterEach(() => infoSpy.mockRestore());

describe("an auto-run reclaims a renamed table it can prove is its own", () => {
   const cases: [string, string, string, Partial<RenamedTable>?][] = [
      [
         "a name= prefix changed under scope: version",
         "old_t__v1_2_0__p0123abcd",
         "named_t__v1_2_0__p4567cdef",
      ],
      [
         "a name= container changed under scope: version",
         "staging.q_t__v1_2_0__p0123abcd",
         "analytics.q_t__v1_2_0__p4567cdef",
      ],
      [
         "a readable-name table replaced when the author adds name=",
         "pa__sales__summary__p0123abcd",
         "summary_t",
      ],
      [
         "a rollup moved to another container",
         "old_ns.pa__sales__orders_by_category__p0123abcd",
         "ns.pa__sales__orders_by_category__p0123abcd",
      ],
   ];
   for (const [label, before, after] of cases) {
      it(label, async () => {
         const version = label.includes("scope: version") ? "1.2.0" : null;
         const reader = version ? { versionId: "1.2.0", owned: "1.2.0" } : {};
         const meta = (m: Record<string, unknown>) =>
            version ? { ...m, scope: "version" } : m;
         const old = run(
            "old",
            { a1: entry(before) },
            { version, metadata: meta({ mode: "auto" }) },
         );
         const now = run(
            "now",
            { a1: entry(after) },
            { version, metadata: meta(recording(renamed(before, after))) },
         );
         const r = await reclaim([old, now], "now", { reader });
         expect(r.dropped).toEqual([`${CONN}:${before}`]);
         expect(r.renamedOn("now")).toEqual([]);
         expect(r.kept).toEqual([]);
      });
   }

   it("a table left in an old storage= destination that is still configured", async () => {
      // lake -> lake2, lake -> the source's own database, and back.
      for (const [from, to] of [
         ["lake", "lake2"],
         ["lake", undefined],
         [undefined, "lake"],
      ] as const) {
         const name = "pa__sales__summary__p0123abcd";
         const old = run("old", { a1: entry(name, "a1", from) });
         const now = run(
            "now",
            { a1: entry(name, "a1", to) },
            {
               metadata: recording(
                  renamed(name, name, {
                     ...(to ? { storageDestinationName: to } : {}),
                     previousPlace: from
                        ? { storageDestinationName: from }
                        : {},
                  }),
               ),
            },
         );
         const r = await reclaim([old, now], "now", {
            destinations: ["lake", "lake2"],
         });
         expect(r.dropped).toEqual([`${from ?? CONN}:${name}`]);
         expect(r.kept).toEqual([]);
      }
   });
});

describe("a table at an author's exact name= is kept and listed", () => {
   for (const [label, after] of [
      ["the name= changed", "named_new"],
      ["the name= removed", "pa__sales__summary__p0123abcd"],
   ] as const) {
      it(`${label}: never dropped, listed as kept, logged author_name`, async () => {
         const old = run("old", { a1: entry("named_t") });
         const now = run(
            "now",
            { a1: entry(after) },
            { metadata: recording(renamed("named_t", after)) },
         );
         const r = await reclaim([old, now], "now");
         expect(r.dropped).toEqual([]);
         const kept: KeptTable = {
            sourceEntityId: "a1",
            sourceName: "summary",
            physicalTableName: "named_t",
            connectionName: CONN,
            reason: "author_name",
         };
         expect(r.kept).toEqual([kept]);
         expect(r.keptOn("now")).toEqual([kept]);
         // Decided for good: no longer in the rename record.
         expect(r.renamedOn("now")).toEqual([]);
         expect(guards).toContain("author_name");
      });
   }

   it("carries the list forward until the name is reused, or no run names the table", async () => {
      const old = run("old", { a1: entry("named_t") });
      const now = run(
         "now",
         { a1: entry("named_new") },
         { metadata: recording(renamed("named_t", "named_new")) },
      );
      const first = await reclaim([old, now], "now");
      const listed = first.keptOn("now")!;
      // The next run carries the source and lists the same table.
      const next = run(
         "next",
         { a1: entry("named_new") },
         { metadata: { mode: "auto", [KEPT_TABLES_KEY]: undefined } },
      );
      const withList = {
         ...now,
         metadata: { mode: "auto", keptTables: listed },
      };
      const carried = await reclaim([old, withList, next], "next");
      expect(carried.kept).toEqual(listed);
      // The name is taken again (name= changed back): no longer listed, and
      // the empty list is written once so it is not carried further.
      const back = run("back", { a1: entry("named_t") });
      const reused = await reclaim([old, withList, back], "back");
      expect(reused.kept).toEqual([]);
      expect(reused.keptOn("back")).toEqual([]);
      // The runs that built it deleted with their tables: dropped, gone.
      const after = run("after", { a1: entry("named_new") });
      const gone = await reclaim([withList, after], "after");
      expect(gone.kept).toEqual([]);
   });

   it("a table in a destination no longer configured: kept, listed destination_missing, never dropped", async () => {
      const name = "pa__sales__summary__p0123abcd";
      const old = run("old", { a1: entry(name, "a1", "gone_lake") });
      const now = run(
         "now",
         { a1: entry(name, "a1", "lake") },
         {
            metadata: recording(
               renamed(name, name, {
                  storageDestinationName: "lake",
                  previousPlace: { storageDestinationName: "gone_lake" },
               }),
            ),
         },
      );
      const r = await reclaim([old, now], "now", { destinations: ["lake"] });
      expect(r.dropped).toEqual([]);
      expect(r.kept).toEqual([
         {
            sourceEntityId: "a1",
            physicalTableName: name,
            connectionName: CONN,
            storageDestinationName: "gone_lake",
            reason: "destination_missing",
         },
      ]);
      expect(r.renamedOn("now")).toEqual([]);
      expect(guards).toContain("destination_missing");
   });
});

describe("nothing else is dropped", () => {
   const OLD = "pa__sales__summary__p0123abcd";
   const NEW = "summary_t";
   const withRecord = (entries: Record<string, ManifestEntry>) =>
      run("now", entries, { metadata: recording(renamed(OLD, NEW)) });

   it("a table another package has marked as its own (the ownership mark)", async () => {
      const r = await reclaim(
         [run("old", { a1: entry(OLD) }), withRecord({ a1: entry(NEW) })],
         "now",
         { marked: [OLD] },
      );
      expect(r.dropped).toEqual([]);
      // Left in place by the drop: recorded, for a later run.
      expect(r.renamedOn("now")).toHaveLength(1);
   });

   it("while the replacement has not committed", async () => {
      const r = await reclaim(
         [
            run("old", { a1: entry(OLD) }),
            withRecord({ a1: entry("something_else") }),
         ],
         "now",
      );
      expect(r.dropped).toEqual([]);
      expect(guards).toEqual(["replacement-not-committed"]);
      expect(r.renamedOn("now")).toHaveLength(1);
   });

   it("while the replacement is not what the package serves", async () => {
      const r = await reclaim(
         [
            run("old", { a1: entry(OLD) }),
            withRecord({ a1: entry(NEW) }),
            run("newer", { a1: entry(OLD) }),
         ],
         "now",
      );
      expect(r.dropped).toEqual([]);
      expect(guards).toEqual(["not-serving-replacement"]);
      expect(r.renamedOn("now")).toHaveLength(1);
   });

   it("while the package is not bound to the run, though author names are still listed", async () => {
      const old = run("old", { a1: entry(OLD), b2: entry("b_old", "b2") });
      const now = run(
         "now",
         { a1: entry(NEW), b2: entry("b_new", "b2") },
         {
            metadata: recording(
               renamed(OLD, NEW),
               renamed("b_old", "b_new", { sourceEntityId: "b2" }),
            ),
         },
      );
      const r = await reclaim([old, now], "now", { bound: false });
      expect(r.dropped).toEqual([]);
      expect(r.kept?.map((t) => t.physicalTableName)).toEqual(["b_old"]);
      expect(r.renamedOn("now").map((t) => t.previousName)).toEqual([OLD]);
   });

   it("a table only a host-instructed run built", async () => {
      const r = await reclaim(
         [
            run(
               "host",
               { a1: entry(OLD) },
               { metadata: { mode: "orchestrated" } },
            ),
            withRecord({ a1: entry(NEW) }),
         ],
         "now",
      );
      expect(r.dropped).toEqual([]);
      expect(guards).toEqual(["not-in-package-records"]);
      // The host owns it, so it is not listed as kept either.
      expect(r.kept).toEqual([]);
   });

   it("a table of another package's name this package's records never built", async () => {
      // A record that names another package's table (its own runs built it,
      // and listMaterializations reads only this package's).
      const theirs = "pb__sales__summary__p89abcdef";
      const r = await reclaim(
         [
            run("old", { a1: entry(OLD) }),
            run(
               "now",
               { a1: entry(NEW) },
               {
                  metadata: recording(renamed(theirs, NEW)),
               },
            ),
         ],
         "now",
      );
      expect(r.dropped).toEqual([]);
      expect(guards).toEqual(["not-in-package-records"]);
   });

   it("a table another source of the package now serves", async () => {
      const r = await reclaim(
         [
            run("old", { a1: entry(OLD) }),
            withRecord({ a1: entry(NEW), b2: entry(OLD, "b2") }),
         ],
         "now",
      );
      expect(r.dropped).toEqual([]);
      expect(guards).toEqual(["still-served"]);
   });

   it("a table the replacement may be, under a name that meets it", async () => {
      // Moved from the default schema to `main`: on DuckDB one table.
      const r = await reclaim(
         [
            run("old", { a1: entry(OLD) }),
            run(
               "now",
               { a1: entry(`main.${OLD}`) },
               {
                  metadata: recording(renamed(OLD, `main.${OLD}`)),
               },
            ),
         ],
         "now",
      );
      expect(r.dropped).toEqual([]);
      expect(guards).toEqual(["still-served"]);
   });
});

describe("the kept list a loading package reads", () => {
   it("is the newest serving run's, whatever a newer run that lists none says", async () => {
      const table = (name: string): KeptTable => ({
         sourceEntityId: "a1",
         physicalTableName: name,
         connectionName: CONN,
         reason: "author_name",
      });
      const runs = [
         run(
            "older",
            {},
            { metadata: { mode: "auto", keptTables: [table("x")] } },
         ),
         run(
            "newer",
            {},
            { metadata: { mode: "auto", keptTables: [table("y")] } },
         ),
         // Committed after, its reclaim not yet run: lists nothing.
         run("newest", {}),
         // Another version's own run is not this reader's.
         run(
            "theirs",
            {},
            {
               version: "9.0.0",
               metadata: {
                  mode: "auto",
                  scope: "version",
                  keptTables: [table("z")],
               },
            },
         ),
      ];
      const resolve = keptTablesResolverFor(
         { listMaterializations: async () => runs } as never,
         "env-1",
      );
      expect(await resolve("pa", {})).toEqual([table("y")]);
   });
});

describe("the table-kept-after-rename warning", () => {
   it("names the table, where it is and why, with the source as its subject", () => {
      const plan = {
         sources: {
            "summary@m": {
               name: "summary",
               sourceEntityId: "a1",
               modelPath: "models/sales.malloy",
            },
         },
      } as unknown as BuildPlan;
      const warnings = keptTableWarnings(plan, [
         {
            sourceEntityId: "a1",
            physicalTableName: "named_t",
            connectionName: CONN,
            reason: "author_name",
         },
         {
            sourceEntityId: "gone",
            sourceName: "orders",
            physicalTableName: "orders__p0123abcd",
            connectionName: CONN,
            storageDestinationName: "old_lake",
            reason: "destination_missing",
         },
      ]);
      expect(warnings).toEqual([
         {
            model: "models/sales.malloy",
            subject: "summary",
            code: TABLE_KEPT_AFTER_RENAME_CODE,
            severity: "warn",
            message: expect.stringContaining(
               "table `named_t` on connection `warehouse` is no longer read",
            ),
         },
         {
            subject: "orders",
            code: TABLE_KEPT_AFTER_RENAME_CODE,
            severity: "warn",
            message: expect.stringContaining(
               "in storage destination `old_lake`",
            ),
         },
      ]);
      expect(warnings[0].message).toContain("never drops");
      expect(warnings[1].message).toContain("no longer configured");
      expect(keptTableWarnings(plan, [])).toEqual([]);
   });
});
