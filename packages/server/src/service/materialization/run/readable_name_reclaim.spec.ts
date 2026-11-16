// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { Materialization } from "../../../storage/DatabaseInterface";
import type { ManifestEntry } from "../domain/types";
import { Reclaimer, RENAMED_TABLES_KEY } from "./reclaimer";
import type { RenamedTable } from "./run_instructions";

/**
 * Reclaimer.reclaimRenamedTables's guards, one at a time, over a fake store:
 * which old tables reach the drop, which are kept and why, and what the runs'
 * records say afterwards. The drop itself (and guard 4, a table another
 * package marked) is dropManifestEntries', and runs end to end in
 * readable_name_migration.spec.ts.
 */

const OLD = "summary__p1a2b3c4d";
const NEW = "pa__model__summary__p9f8e7d6c";

const entry = (table: string, sourceEntityId = "a1"): ManifestEntry =>
   ({
      sourceEntityId,
      physicalTableName: table,
      connectionName: "warehouse",
   }) as ManifestEntry;

const renamed: RenamedTable = {
   sourceEntityId: "a1",
   previousName: OLD,
   physicalTableName: NEW,
   connectionName: "warehouse",
};

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

/** Reclaim as run `runId`, over `runs`; what was dropped, and the records. */
async function reclaim(
   runs: Materialization[],
   runId: string,
   owe: string[] = [],
   options: { bound?: boolean; skip?: string[] } = {},
): Promise<{
   dropped: string[];
   forgotten: string[];
   records: Map<string, Record<string, unknown> | null>;
   rebinds: number;
   occasions: string[];
}> {
   const occasions: string[] = [];
   let rebinds = 0;
   const dropped: string[] = [];
   const forgotten: string[] = [];
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
      deleteIncrementalLedgerEntry: async (
         _env: string,
         table: { physicalTableName: string },
      ) => {
         forgotten.push(table.physicalTableName);
      },
   };
   const reclaimer = new Reclaimer({
      repository: () => repository as never,
      host: {
         getEnvironment: async () =>
            ({
               getPackage: async () => ({
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
         occasions.push(occasion);
         for (const e of entries) {
            if (options.skip?.includes(e.physicalTableName)) {
               skipped?.push(e);
            } else {
               dropped.push(e.physicalTableName);
            }
         }
         return entries.filter((e) => owe.includes(e.physicalTableName));
      },
   });
   await reclaimer.reclaimRenamedTables("e1", "pa", runId, {}, async () => {
      rebinds += 1;
      return options.bound ?? true;
   });
   return { dropped, forgotten, records, rebinds, occasions };
}

const withRenamed = { mode: "auto", [RENAMED_TABLES_KEY]: [renamed] };

describe("reclaiming a table a readable name replaced", () => {
   it("drops it when every guard holds, forgets its boundary and its record", async () => {
      const old = run("old", { a1: entry(OLD) });
      const now = run("now", { a1: entry(NEW) }, { metadata: withRenamed });
      const r = await reclaim([old, now], "now");
      expect(r.dropped).toEqual([OLD]);
      expect(r.occasions).toEqual(["reclaim"]);
      expect(r.forgotten).toEqual([OLD]);
      expect(r.records.get("now")?.[RENAMED_TABLES_KEY]).toBeUndefined();
   });

   it("keeps it when the replacement did not commit under its new name (guard 2)", async () => {
      const old = run("old", { a1: entry(OLD) });
      // The run committed the source elsewhere (a failed build, say).
      const now = run("now", { a1: entry("other") }, { metadata: withRenamed });
      const r = await reclaim([old, now], "now");
      expect(r.dropped).toEqual([]);
      // Kept recorded: a later run's replacement may commit.
      expect(r.records.get("now")?.[RENAMED_TABLES_KEY]).toEqual([renamed]);
   });

   it("keeps it when no earlier run of this package built it there (guard 3)", async () => {
      // An earlier run names it for another source, or in another place.
      const other = run("other", {
         b2: entry(OLD, "b2"),
         a1: { ...entry(OLD), storageDestinationName: "lake" } as ManifestEntry,
      });
      const now = run("now", { a1: entry(NEW) }, { metadata: withRenamed });
      const r = await reclaim([other, now], "now");
      expect(r.dropped).toEqual([]);
      // That cannot change: forgotten.
      expect(r.records.get("now")?.[RENAMED_TABLES_KEY]).toBeUndefined();
   });

   it("keeps it while a newer run, not this one, is what the package serves (guard 5)", async () => {
      const old = run("old", { a1: entry(OLD) });
      const now = run("now", { a1: entry(NEW) }, { metadata: withRenamed });
      const newer = run("newer", { a1: entry(OLD) });
      const r = await reclaim([old, now, newer], "now");
      expect(r.dropped).toEqual([]);
   });

   it("keeps it while a run this package does not supersede names it (guard 5)", async () => {
      const old = run("old", { a1: entry(OLD) });
      const theirs = run(
         "theirs",
         { a1: entry(OLD) },
         { version: "9.0.0", metadata: { mode: "auto", scope: "version" } },
      );
      const now = run("now", { a1: entry(NEW) }, { metadata: withRenamed });
      const r = await reclaim([old, theirs, now], "now");
      expect(r.dropped).toEqual([]);
      // Kept recorded, for a run after the other one goes.
      expect(r.records.get("now")?.[RENAMED_TABLES_KEY]).toEqual([renamed]);
   });

   it("finishes a table an earlier run recorded and never dropped, and keeps one whose drop failed", async () => {
      const old = run("old", { a1: entry(OLD), b2: entry("legacy_b", "b2") });
      // Committed, then the server stopped before its drops.
      const crashed = run(
         "crashed",
         { a1: entry(NEW), b2: entry("readable_b", "b2") },
         {
            metadata: {
               mode: "auto",
               [RENAMED_TABLES_KEY]: [
                  renamed,
                  {
                     sourceEntityId: "b2",
                     previousName: "legacy_b",
                     physicalTableName: "readable_b",
                     connectionName: "warehouse",
                  },
               ],
            },
         },
      );
      // The next run carried both tables and renamed nothing itself.
      const next = run("next", {
         a1: entry(NEW),
         b2: entry("readable_b", "b2"),
      });
      const r = await reclaim([old, crashed, next], "next", ["legacy_b"]);
      expect(r.dropped.sort()).toEqual([OLD, "legacy_b"].sort());
      expect(r.forgotten).toEqual([OLD]);
      // The failed drop stays recorded, for the next run to try.
      expect(
         (r.records.get("crashed")?.[RENAMED_TABLES_KEY] as RenamedTable[]).map(
            (t) => t.previousName,
         ),
      ).toEqual(["legacy_b"]);
   });

   it("does nothing for a run that did not commit", async () => {
      const old = run("old", { a1: entry(OLD) });
      const now = run(
         "now",
         { a1: entry(NEW) },
         { status: "FAILED", metadata: withRenamed },
      );
      const r = await reclaim([old, now], "now");
      expect(r.dropped).toEqual([]);
      expect(r.records.get("now")?.[RENAMED_TABLES_KEY]).toEqual([renamed]);
   });

   it("rebinds every loaded copy before a drop, and drops nothing while one does not rebind", async () => {
      // A record an earlier run left (its own load failed, so a version it
      // built may still read the old table), retried by a run that carried
      // everything and rebound no other version itself.
      const old = run("old", { a1: entry(OLD) });
      const failedLoad = run(
         "failed-load",
         { a1: entry(NEW) },
         { metadata: withRenamed },
      );
      const next = run("next", { a1: entry(NEW) });
      const kept = await reclaim([old, failedLoad, next], "next", [], {
         bound: false,
      });
      expect(kept.rebinds).toBe(1);
      expect(kept.dropped).toEqual([]);
      // Still recorded, for a run whose rebind succeeds.
      expect(kept.records.get("failed-load")?.[RENAMED_TABLES_KEY]).toEqual([
         renamed,
      ]);
      const done = await reclaim([old, failedLoad, next], "next");
      expect(done.rebinds).toBe(1);
      expect(done.dropped).toEqual([OLD]);
   });

   it("asks for no rebind when nothing passes the guards", async () => {
      const now = run("now", { a1: entry(NEW) }, { metadata: withRenamed });
      const r = await reclaim([now], "now");
      expect(r.rebinds).toBe(0);
   });

   it("reads a table the drop left in place as kept: its boundary stays", async () => {
      const old = run("old", { a1: entry(OLD) });
      const now = run("now", { a1: entry(NEW) }, { metadata: withRenamed });
      const r = await reclaim([old, now], "now", [], { skip: [OLD] });
      expect(r.dropped).toEqual([]);
      expect(r.forgotten).toEqual([]);
      expect(r.records.get("now")?.[RENAMED_TABLES_KEY]).toEqual([renamed]);
   });
});
