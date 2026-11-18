// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Characterization of the facade's unread-table retry
 * (`PackageMaterialization.scheduleUnreadRetry` / `retryUnread`): a bound
 * database table whose columns the bind could not read is read again with
 * backoff, until it reads, a bind replaces it, the package is released, or the
 * attempts run out. Driven through the facade's public surface with fake
 * timers, a fake owner and a fake Postgres connection, so the extraction of an
 * `UnreadRetry` collaborator must leave every case here as it is.
 */

import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
   makeBuildPlan,
   makePlanOutcome,
   okWorkerPlan,
} from "../../test_helpers/materialization_test_fixtures";
import type { FreshnessManifest, ObservedColumns } from "./domain/types";
import { PackageMaterialization } from "./package_materialization";

const ADDRESS = "build-orders";

/** The database entry for the plan's one source (makeBuildPlan's). */
const entries = (): FreshnessManifest =>
   ({
      [ADDRESS]: {
         tableName: "public.orders_t",
         connectionName: "duckdb",
         schema: [{ name: "id", type: "INTEGER" }],
      },
   }) as unknown as FreshnessManifest;

const unread = (): ReadonlyMap<string, ObservedColumns> =>
   new Map([[ADDRESS, { unread: "catalog_query_failed" as const }]]);
const read = (): ReadonlyMap<string, ObservedColumns> =>
   new Map([
      [
         ADDRESS,
         { dialect: "postgres", columns: [{ name: "id", type: "INTEGER" }] },
      ],
   ]);

/**
 * A facade over the one-source plan, whose connection's catalog read fails
 * until `readable` is set, or waits on `hold` when one is given.
 */
async function facade() {
   const state = {
      reads: 0,
      readable: false,
      hold: undefined as Promise<void> | undefined,
   };
   const connection = {
      dialectName: "postgres",
      runSQL: async () => {
         state.reads++;
         if (state.hold) await state.hold;
         if (!state.readable) throw new Error("the catalog read failed");
         return { rows: [{ key: ADDRESS, name: "id", type: "integer" }] };
      },
   };
   const m = new PackageMaterialization({
      packageName: () => "pkg",
      versionId: () => undefined,
      metadata: () => ({}) as never,
      models: () => new Map(),
      modelPolicy: () => ({ setServingResolvers: () => {} }) as never,
      packagePath: () => "/pkg",
      malloyConfig: () => ({}) as never,
      connection: async () => connection as never,
      buildPlanPackage: {} as never,
      publishedSurface: () => undefined,
   });
   await m.applyLoadBuildPlan(
      okWorkerPlan(makePlanOutcome({ plan: makeBuildPlan() })),
   );
   const pending = () =>
      (m.databaseTableUnreadWarnings()[0]?.message ?? "").includes(
         "A retry is pending.",
      );
   return { m, state, pending };
}

/** Let a retry's read, and what it schedules after, run to the end. */
async function settle(): Promise<void> {
   for (let i = 0; i < 100; i++) await Promise.resolve();
}

/** Advance the fake clock by `ms`, then let what fired settle. */
async function advance(ms: number): Promise<void> {
   jest.advanceTimersByTime(ms);
   await settle();
}

beforeEach(() => {
   jest.useFakeTimers();
});
afterEach(() => {
   jest.useRealTimers();
});

describe("the unread-table retry", () => {
   it("reads again with backoff (1s, doubling, capped at 60s), ten times at most", async () => {
      const { m, state, pending } = await facade();
      m.bind({
         database: entries(),
         databaseAsGiven: true,
         observed: unread(),
      });
      expect(m.hasUnreadDatabaseTables()).toBe(true);
      expect(pending()).toBe(true);
      const delays: number[] = [];
      let elapsed = 0;
      for (let step = 0; step < 600 && delays.length < 12; step++) {
         const before = state.reads;
         await advance(1000);
         elapsed += 1000;
         if (state.reads > before) {
            delays.push(elapsed);
            elapsed = 0;
         }
      }
      expect(delays).toEqual([
         1000, 2000, 4000, 8000, 16000, 32000, 60000, 60000, 60000, 60000,
      ]);
      // Out of attempts: still unread, and no retry pending; polls rebind it.
      expect(m.hasUnreadDatabaseTables()).toBe(true);
      expect(pending()).toBe(false);
   });

   it("stops once the table reads", async () => {
      const { m, state, pending } = await facade();
      m.bind({
         database: entries(),
         databaseAsGiven: true,
         observed: unread(),
      });
      await advance(1000);
      expect(state.reads).toBe(1);
      state.readable = true;
      await advance(2000);
      expect(state.reads).toBe(2);
      expect(m.hasUnreadDatabaseTables()).toBe(false);
      expect(pending()).toBe(false);
      await advance(10 * 60_000);
      expect(state.reads).toBe(2);
   });

   it("schedules nothing for a bind that read every table, or that read none (a hand-made bind)", async () => {
      const { m, state } = await facade();
      m.bind({ database: entries(), databaseAsGiven: true, observed: read() });
      m.bind({ database: entries(), databaseAsGiven: true });
      await advance(10 * 60_000);
      expect(state.reads).toBe(0);
   });

   it("cancels the pending retry when a bind replaces the entries", async () => {
      const { m, state } = await facade();
      m.bind({
         database: entries(),
         databaseAsGiven: true,
         observed: unread(),
      });
      m.bind({ database: entries(), databaseAsGiven: true, observed: read() });
      await advance(10 * 60_000);
      expect(state.reads).toBe(0);
      expect(m.hasUnreadDatabaseTables()).toBe(false);
   });

   it("drops a read a bind overtook, and lets the new bind's retry start over", async () => {
      const { m, state } = await facade();
      m.bind({
         database: entries(),
         databaseAsGiven: true,
         observed: unread(),
      });
      let release!: () => void;
      state.hold = new Promise((r) => (release = r));
      await advance(1000);
      expect(state.reads).toBe(1);
      // A new bind while that read waits on the warehouse.
      m.bind({
         database: entries(),
         databaseAsGiven: true,
         observed: unread(),
      });
      state.readable = true;
      state.hold = undefined;
      release();
      await settle();
      // The overtaken read's answer is not applied...
      expect(m.hasUnreadDatabaseTables()).toBe(true);
      // ...and the new bind's own retry runs on its first delay.
      await advance(1000);
      expect(state.reads).toBe(2);
      expect(m.hasUnreadDatabaseTables()).toBe(false);
   });

   it("schedules nothing once the package is released, even with a read in flight", async () => {
      const { m, state, pending } = await facade();
      m.bind({
         database: entries(),
         databaseAsGiven: true,
         observed: unread(),
      });
      m.dispose();
      expect(pending()).toBe(false);
      await advance(10 * 60_000);
      expect(state.reads).toBe(0);

      const second = await facade();
      second.m.bind({
         database: entries(),
         databaseAsGiven: true,
         observed: unread(),
      });
      let release!: () => void;
      second.state.hold = new Promise((r) => (release = r));
      await advance(1000);
      expect(second.state.reads).toBe(1);
      second.m.dispose();
      release();
      await advance(10 * 60_000);
      expect(second.state.reads).toBe(1);
   });

   it("retries only a table a source of this package reads", async () => {
      const { m, state } = await facade();
      const other = {
         "another-definition": { ...entries()[ADDRESS] },
      } as unknown as FreshnessManifest;
      m.bind({
         database: other,
         databaseAsGiven: true,
         observed: new Map([
            ["another-definition", { unread: "timed_out" as const }],
         ]),
      });
      expect(m.hasUnreadDatabaseTables()).toBe(false);
      await advance(10 * 60_000);
      expect(state.reads).toBe(0);
   });
});
