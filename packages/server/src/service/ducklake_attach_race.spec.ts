// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Two first lookups of one DuckLake storage destination used to attach it
// twice. The first locked the instance between the second's ATTACH and its
// storage secret; the locked instance refused that secret, the refusal's
// cleanup detached the catalog, and every later lookup took the locked path,
// which never re-attached, so each stored read failed with `Catalog "…" does
// not exist` until the process restarted. These specs force that interleaving
// on a stubbed session.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { MalloyQueryData } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import sinon from "sinon";
import type { components } from "../api";
import { buildEnvironmentMalloyConfig } from "./connection";
import { DuckLakeCatalogUnavailableError } from "./ducklake_catalog_unavailable";
import { isInstanceLocked } from "./duckdb_lockdown";
import { logger } from "../logger";

type ApiConnection = components["schemas"]["Connection"];

const DESTINATION = {
   name: "credible",
   type: "ducklake",
   ducklakeConnection: {
      catalog: {
         postgresConnection: {
            host: "catalog.invalid",
            port: 5432,
            databaseName: "lake",
            userName: "u",
            password: "p",
         },
         metadataSchema: "lake_meta",
      },
      storage: {
         bucketUrl: "s3://race-bucket/lake/",
         s3Connection: {
            accessKeyId: "AKIARACEFAKE",
            secretAccessKey: "fake",
            region: "us-east-1",
         },
      },
   },
} as ApiConnection;

/** What a stubbed instance holds. */
interface FakeInstance {
   attached: Set<string>;
   attaches: number;
   /** Every alias detached, the preflight's own included. */
   detached: string[];
   locked: boolean;
   externalAccess: boolean;
   /** How many times a lookup asked whether the catalog is attached. */
   catalogChecks: number;
}

/**
 * Stub every statement a DuckLake lookup issues, keeping per-instance state:
 * what is attached, and whether the instance is locked. On a locked instance
 * a `CREATE SECRET` is refused, as the statement gate refuses it.
 * `beforeSecret` runs before each `CREATE SECRET`, numbered from 1, so a
 * spec can hold one until something else has happened.
 */
function stubDuckLake(hooks: {
   beforeSecret?: (n: number) => Promise<void>;
   onAttach?: (n: number) => void;
   onLock?: () => void;
   beforeCatalogCheck?: (n: number) => Promise<void>;
}): { stateOf: (c: DuckDBConnection) => FakeInstance } {
   const states = new WeakMap<object, FakeInstance>();
   // The connection's native instance, opened for real (only its statements
   // are stubbed): the lockdown keys its lock on it, and the last close
   // releases it, so a later connection opens a fresh one.
   const instanceOf = (c: DuckDBConnection): object =>
      (
         DuckDBConnection.activeDBs as Record<
            string,
            { instance: object } | undefined
         >
      )[(c as unknown as { shareKey: string }).shareKey]?.instance ?? c;
   const stateOf = (c: DuckDBConnection): FakeInstance => {
      const key = instanceOf(c);
      let state = states.get(key);
      if (!state) {
         state = {
            attached: new Set(),
            attaches: 0,
            detached: [],
            locked: false,
            externalAccess: true,
            catalogChecks: 0,
         };
         states.set(key, state);
      }
      return state;
   };
   let secrets = 0;
   let attachCount = 0;
   let catalogCheckCount = 0;
   // Typed as the stubbed method returns, from rows the stub makes up.
   const rows = (r: Record<string, unknown>[]) =>
      ({ rows: r, totalRows: r.length }) as unknown as MalloyQueryData;
   sinon.stub(DuckDBConnection.prototype, "runSQL").callsFake(async function (
      this: DuckDBConnection,
      sql: string,
   ) {
      // As a real statement does, wait for the instance to open.
      await (this as unknown as { connecting?: Promise<void> }).connecting;
      const state = stateOf(this);
      const q = sql.trim();
      const attach = /^ATTACH OR REPLACE 'ducklake:[^']*' AS "([^"]+)"/.exec(q);
      if (attach) {
         state.attaches++;
         state.attached.add(attach[1]);
         hooks.onAttach?.(++attachCount);
         return rows([]);
      }
      const detach = /^DETACH "?([^";]+)"?;?$/.exec(q);
      if (detach) {
         state.detached.push(detach[1]);
         state.attached.delete(detach[1]);
         return rows([]);
      }
      if (/^CREATE OR REPLACE SECRET/i.test(q)) {
         await hooks.beforeSecret?.(++secrets);
         if (state.locked) {
            throw new Error(
               "This DuckDB connection is locked down and refuses CREATE SECRET",
            );
         }
         return rows([]);
      }
      if (q === "SHOW DATABASES") {
         return rows([...state.attached].map((name) => ({ name })));
      }
      const alias =
         /FROM duckdb_databases\(\) WHERE lower\(database_name\) = lower\('([^']*)'\)/.exec(
            q,
         );
      if (alias) {
         await hooks.beforeCatalogCheck?.(++catalogCheckCount);
         if ((this as unknown as { closed?: boolean }).closed) {
            throw new Error(`DuckDB connection "${this.name}" is closed`);
         }
         state.catalogChecks++;
         return rows(
            [...state.attached]
               .filter((n) => n.toLowerCase() === alias[1].toLowerCase())
               .map(() => ({ "1": 1 })),
         );
      }
      const setting = /^SELECT current_setting\('([^']+)'\) AS v$/.exec(q);
      if (setting) {
         if (setting[1] === "lock_configuration") {
            return rows([{ v: state.locked }]);
         }
         if (setting[1] === "enable_external_access") {
            return rows([{ v: state.externalAccess }]);
         }
         return rows([{ v: "" }]);
      }
      // A read of a catalog's table binds only when the catalog is attached.
      const read = /FROM "?([A-Za-z_]+)"?\.main\./.exec(q);
      if (read && !state.attached.has(read[1])) {
         throw new Error(`Binder Error: Catalog "${read[1]}" does not exist!`);
      }
      if (q === "SET enable_external_access=false") {
         state.externalAccess = false;
      }
      if (q === "SET lock_configuration=true") {
         state.locked = true;
         hooks.onLock?.();
      }
      return rows([]);
   });
   return { stateOf };
}

/** A promise and its resolver. */
function signal(): { promise: Promise<void>; fire: () => void } {
   let fire!: () => void;
   const promise = new Promise<void>((resolve) => {
      fire = resolve;
   });
   return { promise, fire };
}

const within = (promise: Promise<void>, ms: number) =>
   Promise.race([promise, new Promise<void>((r) => setTimeout(r, ms))]);

describe("concurrent first lookups of a DuckLake destination", () => {
   let root: string;

   beforeEach(() => {
      root = mkdtempSync(join(tmpdir(), "ducklake-attach-race-"));
   });

   afterEach(() => {
      sinon.restore();
      rmSync(root, { recursive: true, force: true });
   });

   it("share one attach, so the lock never lands inside a second one", async () => {
      const secondAttach = signal();
      const locked = signal();
      const { stateOf } = stubDuckLake({
         onAttach: (n) => {
            if (n === 2) secondAttach.fire();
         },
         onLock: () => locked.fire(),
         // The first lookup's secret waits for a second ATTACH (which the
         // shared attach never issues), so a second attach, if any, runs
         // before the lock; that one's secret then waits for the lock.
         beforeSecret: async (n) => {
            if (n === 1) await within(secondAttach.promise, 150);
            else await within(locked.promise, 2000);
         },
      });
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      try {
         const lookups = await Promise.allSettled([
            env.malloyConfig.connections.lookupConnection("credible"),
            env.malloyConfig.connections.lookupConnection("credible"),
         ]);
         expect(lookups.map((l) => l.status)).toEqual([
            "fulfilled",
            "fulfilled",
         ]);
         // A later lookup, as a stored read makes it, finds the catalog.
         const later = (await env.malloyConfig.connections.lookupConnection(
            "credible",
         )) as DuckDBConnection;
         expect(isInstanceLocked(later)).toBe(true);
         const state = stateOf(later);
         expect(state.attached.has("credible")).toBe(true);
         expect(state.attaches).toBe(1);
         expect(state.detached).not.toContain("credible");
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });

   it("reopens a locked instance that lost its catalog, failing that lookup with a typed error", async () => {
      const { stateOf } = stubDuckLake({});
      const reopened: string[] = [];
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
         onDuckLakeReopened: (name) => reopened.push(name),
      });
      try {
         const first = (await env.malloyConfig.connections.lookupConnection(
            "credible",
         )) as DuckDBConnection;
         expect(isInstanceLocked(first)).toBe(true);
         // The catalog goes missing on the locked instance.
         stateOf(first).attached.delete("credible");

         const lost = await env.malloyConfig.connections
            .lookupConnection("credible")
            .then(
               () => undefined,
               (error: unknown) => error,
            );
         expect(lost).toBeInstanceOf(DuckLakeCatalogUnavailableError);
         expect(reopened).toEqual(["credible"]);

         // The next lookup opens a fresh instance and attaches again.
         const healed = (await env.malloyConfig.connections.lookupConnection(
            "credible",
         )) as DuckDBConnection;
         expect(healed).not.toBe(first);
         expect(stateOf(healed).attached.has("credible")).toBe(true);
         expect(isInstanceLocked(healed)).toBe(true);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });
   it("checks a locked instance's catalog once, not on every lookup", async () => {
      const { stateOf } = stubDuckLake({});
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      try {
         let connection: DuckDBConnection | undefined;
         for (let i = 0; i < 5; i++) {
            connection = (await env.malloyConfig.connections.lookupConnection(
               "credible",
            )) as DuckDBConnection;
         }
         expect(stateOf(connection!).catalogChecks).toBe(1);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });

   it("reads a binder error as a lost catalog only when the instance confirms it", async () => {
      const { stateOf } = stubDuckLake({});
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      try {
         const connection =
            (await env.malloyConfig.connections.lookupConnection(
               "credible",
            )) as DuckDBConnection;
         const state = stateOf(connection);
         // A misbinding: another catalog, while this one is attached.
         const misbound = await connection
            .runSQL("SELECT count(*) FROM wrong_catalog.main.t")
            .then(
               () => undefined,
               (error: unknown) => error,
            );
         expect(misbound).not.toBeInstanceOf(DuckLakeCatalogUnavailableError);
         expect(String(misbound)).toContain('Catalog "wrong_catalog"');

         // The catalog itself gone: confirmed, typed, and the next lookup
         // checks again (and reopens).
         state.attached.delete("credible");
         const lost = await connection
            .runSQL("SELECT count(*) FROM credible.main.t")
            .then(
               () => undefined,
               (error: unknown) => error,
            );
         expect(lost).toBeInstanceOf(DuckLakeCatalogUnavailableError);
         const checksBefore = state.catalogChecks;
         await expect(
            env.malloyConfig.connections.lookupConnection("credible"),
         ).rejects.toBeInstanceOf(DuckLakeCatalogUnavailableError);
         expect(state.catalogChecks).toBe(checksBefore + 1);
         const healed = (await env.malloyConfig.connections.lookupConnection(
            "credible",
         )) as DuckDBConnection;
         expect(healed).not.toBe(connection);
         expect(
            await healed.runSQL("SELECT count(*) FROM credible.main.t"),
         ).toBeDefined();
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });

   it("waits for a draining holder of the instance before reopening, reporting the loss once", async () => {
      const { stateOf } = stubDuckLake({});
      const reopened: string[] = [];
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
         onDuckLakeReopened: (name) => reopened.push(name),
      });
      const errorLog = sinon.spy(logger, "error");
      try {
         const first = (await env.malloyConfig.connections.lookupConnection(
            "credible",
         )) as DuckDBConnection;
         stateOf(first).attached.delete("credible");
         // Another generation's connection still holds the instance.
         const shareKey = (first as unknown as { shareKey: string }).shareKey;
         const holders = (
            DuckDBConnection.activeDBs as Record<
               string,
               { connections: unknown[] }
            >
         )[shareKey].connections;
         const draining = {};
         holders.push(draining);
         try {
            for (let i = 0; i < 3; i++) {
               await expect(
                  env.malloyConfig.connections.lookupConnection("credible"),
               ).rejects.toBeInstanceOf(DuckLakeCatalogUnavailableError);
            }
            // Not reopened onto the same instance, and reported once.
            expect(reopened).toEqual([]);
            expect(
               errorLog
                  .getCalls()
                  .filter((c) =>
                     String(c.args[0]).includes("missing from its locked"),
                  ),
            ).toHaveLength(1);
         } finally {
            holders.splice(holders.indexOf(draining), 1);
         }

         // The other holder released: this lookup reopens, once.
         await expect(
            env.malloyConfig.connections.lookupConnection("credible"),
         ).rejects.toBeInstanceOf(DuckLakeCatalogUnavailableError);
         expect(reopened).toEqual(["credible"]);
         const healed = (await env.malloyConfig.connections.lookupConnection(
            "credible",
         )) as DuckDBConnection;
         expect(healed).not.toBe(first);
         expect(stateOf(healed).attached.has("credible")).toBe(true);
         expect(reopened).toEqual(["credible"]);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });

   it("looks up again, rather than return it, when a concurrent lookup reopened the connection", async () => {
      const reopenedSignal = signal();
      // The first locked lookup's check runs at once and finds the catalog
      // gone; the second's waits until that lookup has reopened, so it runs
      // on the closed connection.
      const { stateOf } = stubDuckLake({
         beforeCatalogCheck: async (n) => {
            if (n === 2) await within(reopenedSignal.promise, 2000);
         },
      });
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
         onDuckLakeReopened: () => reopenedSignal.fire(),
      });
      try {
         const first = (await env.malloyConfig.connections.lookupConnection(
            "credible",
         )) as DuckDBConnection;
         stateOf(first).attached.delete("credible");
         const [detecting, concurrent] = await Promise.allSettled([
            env.malloyConfig.connections.lookupConnection("credible"),
            env.malloyConfig.connections.lookupConnection("credible"),
         ]);
         expect(detecting.status).toBe("rejected");
         expect((detecting as PromiseRejectedResult).reason).toBeInstanceOf(
            DuckLakeCatalogUnavailableError,
         );
         // The concurrent lookup found its connection closed and looked up
         // again, onto a fresh instance.
         expect(concurrent.status).toBe("fulfilled");
         const fresh = (concurrent as PromiseFulfilledResult<DuckDBConnection>)
            .value;
         expect(fresh).not.toBe(first);
         expect(stateOf(fresh).attached.has("credible")).toBe(true);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });
});
