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
import { WRITES_TABLES_KEY } from "../../versions/materialization_scope";

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
      /** Destination names configured as another's DuckLake. */
      aliases?: Record<string, string>;
      /** The warehouse connection's type (postgres by default). */
      connectionType?: string;
      /** Runs that commit while the reclaim waits for the run lock. */
      commitWhileLocking?: Materialization[];
      /** Destination schemas, by destination name. */
      schemas?: Record<string, string>;
      /**
       * Whether a table is the served one, as the identity probe on the
       * table itself would answer; by default, one exactly when both are
       * in the same DuckLake (aliases).
       */
      sameTable?: (old: string, served: string) => boolean | undefined;
   } = {},
) {
   const dropped: string[] = [];
   let locks = 0;
   let probes = 0;
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
               // Each configured destination its own DuckLake: its own
               // catalog database and data path, unless `aliases` names
               // two for one.
               getStorageDestination: (name: string) => {
                  if (!configured.has(name)) throw new Error("not configured");
                  const lake = options.aliases?.[name] ?? name;
                  return {
                     name,
                     type: "ducklake",
                     ducklakeConnection: {
                        catalog: {
                           postgresConnection: {
                              host: "pg",
                              port: 5432,
                              databaseName: `catalog_${lake}`,
                           },
                        },
                        storage: { bucketUrl: `/data/${lake}` },
                        ...(options.schemas?.[name]
                           ? { destinationSchema: options.schemas[name] }
                           : {}),
                     },
                  };
               },
               getApiConnection: (name: string) => ({
                  name,
                  type: options.connectionType ?? "postgres",
               }),
               getPackage: async () => ({
                  getMalloyConnection: async () => undefined,
               }),
               peekVersion: () => ({
                  getMalloyConnection: async () => undefined,
               }),
            }) as never,
      } as never,
      lifecycle: {} as never,
      sameTable: async (old, served) => {
         probes += 1;
         const lakeOf = (t: { storageDestinationName?: string }) =>
            t.storageDestinationName === undefined
               ? `own:${CONN}`
               : (options.aliases?.[t.storageDestinationName] ??
                 t.storageDestinationName);
         return options.sameTable
            ? options.sameTable(
                 old.storageDestinationName ?? CONN,
                 served.storageDestinationName ?? CONN,
              )
            : lakeOf(old) === lakeOf(served);
      },
      withRunLock: async (_e, _i, _p, _v, fn) => {
         locks += 1;
         for (const m of options.commitWhileLocking ?? []) {
            runs.push(m);
            records.set(m.id, m.metadata ?? null);
         }
         return fn();
      },
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
      locks,
      probes,
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
      // Renamed as it moved: the old table cannot be the new one.
      const before = "pa__sales__old_summary__p0123abcd";
      const after = "pa__sales__summary__p4567cdef";
      const old = run("old", { a1: entry(before, "a1", "gone_lake") });
      const now = run(
         "now",
         { a1: entry(after, "a1", "lake") },
         {
            metadata: recording(
               renamed(before, after, {
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
            physicalTableName: before,
            connectionName: CONN,
            storageDestinationName: "gone_lake",
            reason: "destination_missing",
         },
      ]);
      expect(r.renamedOn("now")).toEqual([]);
      expect(guards).toContain("destination_missing");
   });

   it("a destination no longer configured, its table under the same name: may be the one served, so neither dropped nor listed", async () => {
      // `lake` may be `gone_lake` renamed: nothing proves them apart.
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
      expect(r.kept).toEqual([]);
      expect(guards).toEqual(["still-served"]);
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

   it("while the run committed no table for the source", async () => {
      const r = await reclaim(
         [
            run("old", { a1: entry(OLD) }),
            withRecord({ b2: entry("b_t", "b2") }),
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

describe("the run lock", () => {
   const OLD = "pa__sales__summary__p0123abcd";
   it("decides under it, from the runs as they are then: a run that committed meanwhile decides instead", async () => {
      const old = run("old", { a1: entry(OLD) });
      const now = run(
         "now",
         { a1: entry("summary_t") },
         { metadata: recording(renamed(OLD, "summary_t")) },
      );
      // Reverted: the newer run builds the old name again.
      const newer = run("newer", { a1: entry(OLD) });
      const r = await reclaim([old, now], "now", {
         commitWhileLocking: [newer],
      });
      expect(r.locks).toBe(1);
      expect(r.dropped).toEqual([]);
      // Nothing decided: left for the newer run.
      expect(r.kept).toBeUndefined();
      expect(r.renamedOn("now")).toHaveLength(1);
   });

   it("decides nothing while a run of the package is active", async () => {
      const old = run("old", { a1: entry(OLD) });
      const now = run(
         "now",
         { a1: entry("summary_t") },
         { metadata: recording(renamed(OLD, "summary_t")) },
      );
      const active = run("active", {}, { status: "PENDING" });
      const r = await reclaim([old, now], "now", {
         commitWhileLocking: [active],
      });
      expect(r.dropped).toEqual([]);
      expect(r.kept).toBeUndefined();
   });
});

describe("a move between places that may be one", () => {
   const NAME = "pa__sales__summary__p0123abcd";
   const move = (from: string | undefined, to: string | undefined) => [
      run("old", { a1: entry(NAME, "a1", from) }),
      run(
         "now",
         { a1: entry(NAME, "a1", to) },
         {
            metadata: recording(
               renamed(NAME, NAME, {
                  ...(to ? { storageDestinationName: to } : {}),
                  previousPlace: from ? { storageDestinationName: from } : {},
               }),
            ),
         },
      ),
   ];

   it("two destination names for one DuckLake: the table moved to is never dropped", async () => {
      const r = await reclaim(move("lake", "lake2"), "now", {
         destinations: ["lake", "lake2"],
         aliases: { lake2: "lake" },
      });
      expect(r.dropped).toEqual([]);
      expect(r.kept).toEqual([]);
      expect(guards).toEqual(["still-served"]);
   });

   it("a connection that may reach the destination: never dropped, either way", async () => {
      for (const [from, to] of [
         ["lake", undefined],
         [undefined, "lake"],
      ] as const) {
         guards = [];
         const r = await reclaim(move(from, to), "now", {
            destinations: ["lake"],
            // A MotherDuck connection may attach the lake.
            connectionType: "motherduck",
         });
         expect(r.dropped).toEqual([]);
         expect(guards).toEqual(["still-served"]);
      }
   });
});

describe("two names for one DuckLake the config does not tell apart", () => {
   const move = (name: string, from: string, to: string) => [
      run("old", { a1: entry(name, "a1", from) }),
      run(
         "now",
         { a1: entry(name, "a1", to) },
         {
            metadata: recording(
               renamed(name, name, {
                  storageDestinationName: to,
                  previousPlace: { storageDestinationName: from },
               }),
            ),
         },
      ),
   ];

   it("a destination schema spelled in another case is the same schema", async () => {
      const r = await reclaim(
         move("pa__sales__summary__p0123abcd", "lake", "lake2"),
         "now",
         {
            destinations: ["lake", "lake2"],
            aliases: { lake2: "lake" },
            schemas: { lake2: "MAIN" },
            sameTable: () => undefined,
         },
      );
      expect(r.dropped).toEqual([]);
      expect(guards).toEqual(["still-served"]);
   });

   it("a name with its own container is one table in one lake, whatever the schemas", async () => {
      const r = await reclaim(
         move(
            "analytics.pa__sales__summary__v1_0_0__p0123abcd",
            "lake",
            "lake2",
         ),
         "now",
         {
            destinations: ["lake", "lake2"],
            aliases: { lake2: "lake" },
            schemas: { lake2: "s2" },
            sameTable: () => undefined,
         },
      );
      expect(r.dropped).toEqual([]);
      expect(guards).toEqual(["still-served"]);
   });

   it("one the config calls apart is still tested on the table itself: kept when it is the served table", async () => {
      // The configs differ (a catalog spelled another way, say), but the
      // table at the old place is the one the package serves.
      const r = await reclaim(
         move("pa__sales__summary__p0123abcd", "lake", "lake2"),
         "now",
         { destinations: ["lake", "lake2"], sameTable: () => true },
      );
      expect(r.probes).toBe(1);
      expect(r.dropped).toEqual([]);
      expect(guards).toEqual(["still-served"]);
      expect(r.renamedOn("now")).toEqual([]);
   });

   it("kept, and tried again, when the table cannot be asked", async () => {
      const r = await reclaim(
         move("pa__sales__summary__p0123abcd", "lake", "lake2"),
         "now",
         { destinations: ["lake", "lake2"], sameTable: () => undefined },
      );
      expect(r.dropped).toEqual([]);
      expect(guards).toEqual(["identity-unknown"]);
      expect(r.renamedOn("now")).toHaveLength(1);
   });
});

describe("an active run outside the serving line", () => {
   it("that has claimed the old name to write keeps it", async () => {
      const OLD = "pa__sales__summary__p0123abcd";
      const runs = [
         run("old", { a1: entry(OLD) }, { version: "1.0.0" }),
         run(
            "now",
            { a1: entry("summary_t") },
            {
               version: "2.0.0",
               metadata: recording(renamed(OLD, "summary_t")),
            },
         ),
         run(
            "orch",
            {},
            {
               version: "3.0.0",
               status: "MANIFEST_ROWS_READY",
               metadata: { mode: "orchestrated", [WRITES_TABLES_KEY]: [OLD] },
            },
         ),
      ];
      const r = await reclaim(runs, "now", { reader: { versionId: "2.0.0" } });
      expect(r.dropped).toEqual([]);
      expect(guards).toEqual(["still-referenced"]);
      expect(r.renamedOn("now")).toHaveLength(1);
   });
});

describe("a source renamed again before its old table went (A to B to C)", () => {
   it("is judged against what the package serves now: the oldest table goes", async () => {
      const A = "pa__sales__summary__p0123abcd";
      const runs = [
         run("r1", { a1: entry(A) }),
         // Renamed to B; the drop of A left it (another package's mark,
         // since gone), so it stays recorded.
         run(
            "r2",
            { a1: entry("b_t") },
            { metadata: recording(renamed(A, "b_t")) },
         ),
         // Renamed to C: B is an author's name, kept.
         run(
            "r3",
            { a1: entry("c_t") },
            { metadata: recording(renamed("b_t", "c_t")) },
         ),
      ];
      const r = await reclaim(runs, "r3");
      expect(r.dropped).toEqual([`${CONN}:${A}`]);
      expect(r.kept?.map((t) => t.physicalTableName)).toEqual(["b_t"]);
      expect(r.renamedOn("r2")).toEqual([]);
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
