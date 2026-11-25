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
import { MalloyConfig, Runtime } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import sinon from "sinon";
import {
   DESTINATION,
   loseCatalog,
   stubDuckLake,
} from "../test_helpers/ducklake_fake";
import { buildEnvironmentMalloyConfig } from "./connection";
import { DuckLakeCatalogUnavailableError } from "./ducklake_catalog_unavailable";
import { isInstanceLocked } from "./duckdb_lockdown";
import { logger } from "../logger";
import { isDestinationCatalogUnavailableError } from "./materialization/serve/leg";

/** The schema reads a DuckLake connection overrides. */
type DuckLakeLike = DuckDBConnection;

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

   it("reopens a locked instance that lost its catalog, the detecting lookup going on to the fresh one", async () => {
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

         // The lookup that finds it gone reopens, and is answered from a
         // fresh instance it attaches.
         const healed = (await env.malloyConfig.connections.lookupConnection(
            "credible",
         )) as DuckDBConnection;
         expect(healed).not.toBe(first);
         expect(stateOf(healed).attached.has("credible")).toBe(true);
         expect(isInstanceLocked(healed)).toBe(true);
         expect(reopened).toEqual(["credible"]);
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
         const healed = (await env.malloyConfig.connections.lookupConnection(
            "credible",
         )) as DuckDBConnection;
         // The cached check was dropped, so this lookup checked again.
         expect(state.catalogChecks).toBe(checksBefore + 1);
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

         // The other holder released: this lookup reopens, once, and is
         // answered from the fresh instance.
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
         const lookups = await Promise.allSettled([
            env.malloyConfig.connections.lookupConnection("credible"),
            env.malloyConfig.connections.lookupConnection("credible"),
         ]);
         // Both looked up again, onto one fresh instance: the detecting one
         // after its own reopen, the concurrent one after finding its
         // connection closed.
         for (const l of lookups) {
            expect(l.status).toBe("fulfilled");
            const fresh = (l as PromiseFulfilledResult<DuckDBConnection>).value;
            expect(fresh).not.toBe(first);
            expect(stateOf(fresh).attached.has("credible")).toBe(true);
         }
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });

   it("confirms a catalog lost after its check was cached on a compile's schema read, and a compile then heals it", async () => {
      const { stateOf } = stubDuckLake({});
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      const runtime = new Runtime({ config: env.malloyConfig });
      const model = `source: s is credible.table('credible.main.t')`;
      try {
         // Attached, locked, and its check cached.
         await env.malloyConfig.connections.lookupConnection("credible");
         const connection =
            (await env.malloyConfig.connections.lookupConnection(
               "credible",
            )) as DuckLakeLike;
         const state = stateOf(connection);
         await runtime.loadModel(model).getModel();

         // A misbinding on the schema read keeps its own error.
         const misbound = await connection
            .fetchTableSchema("k", "wrong_catalog.main.t")
            .then(
               () => undefined,
               (error: unknown) => error,
            );
         expect(misbound).not.toBeInstanceOf(DuckLakeCatalogUnavailableError);

         await loseCatalog(connection, state);
         const lost = await connection
            .fetchTableSchema("k", "credible.main.t")
            .then(
               () => undefined,
               (error: unknown) => error,
            );
         expect(lost).toBeInstanceOf(DuckLakeCatalogUnavailableError);
         await expect(
            connection.fetchSelectSchema({
               connection: "credible",
               selectStr: "SELECT * FROM credible.main.t",
            }),
         ).rejects.toBeInstanceOf(DuckLakeCatalogUnavailableError);

         // A compile now: its lookup checks again (the schema read dropped
         // the cached check), reopens, and compiles on the fresh instance.
         const checksBefore = state.catalogChecks;
         const compiled = await new Runtime({ config: env.malloyConfig })
            .loadModel(model)
            .getModel();
         expect(compiled).toBeDefined();
         expect(state.catalogChecks).toBe(checksBefore + 1);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });

   it("recognises the compile's wrapped schema-read failure as a lost catalog", async () => {
      // Malloy catches the schema read's error and reports it as a problem,
      // with neither its type nor its code: the served copy must still know
      // it, or it would remember the failure past the reopen.
      const { stateOf } = stubDuckLake({});
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      try {
         await env.malloyConfig.connections.lookupConnection("credible");
         const connection =
            (await env.malloyConfig.connections.lookupConnection(
               "credible",
            )) as DuckDBConnection;
         await loseCatalog(connection, stateOf(connection));
         // Malloy reads the schema through the connection it was handed, so
         // the compile sees the loss before any lookup checks again.
         const handedConfig = new MalloyConfig({ connections: {} });
         handedConfig.wrapConnections(() => ({
            lookupConnection: async () => connection,
         }));
         const handed = new Runtime({ config: handedConfig });
         const failure = await handed
            .loadModel(`source: s is credible.table('credible.main.t')`)
            .getModel()
            .then(
               () => undefined,
               (error: unknown) => error,
            );
         expect(failure).toBeDefined();
         expect(failure).not.toBeInstanceOf(DuckLakeCatalogUnavailableError);
         expect(isDestinationCatalogUnavailableError(failure)).toBe(true);

         // A compile failing for any other reason is not one.
         await env.malloyConfig.connections
            .lookupConnection("credible")
            .catch(() => undefined);
         const other = await new Runtime({ config: env.malloyConfig })
            .loadModel(`source: s is credible.table('credible.main.nope')`)
            .getModel()
            .then(
               () => undefined,
               (error: unknown) => error,
            );
         expect(other).toBeDefined();
         expect(isDestinationCatalogUnavailableError(other)).toBe(false);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });
});

/** Drain a stream, keeping the rows it yielded before any error. */
async function drain(
   stream: AsyncIterableIterator<unknown>,
): Promise<{ rows: unknown[]; error?: unknown }> {
   const rows: unknown[] = [];
   try {
      for await (const row of stream) rows.push(row);
      return { rows };
   } catch (error) {
      return { rows, error };
   }
}

/**
 * Fail the native DuckDB stream beneath `connection` with the error `fail`
 * returns for that call (numbered from 1), and otherwise run it: the statement
 * gate, the lockdown and the hard close still run around it, as in production.
 */
function failNativeStreams(
   connection: DuckDBConnection,
   fail: (call: number) => Promise<Error | undefined>,
): sinon.SinonStub {
   const native = (
      connection as unknown as {
         connection: { stream(sql: string): Promise<unknown> };
      }
   ).connection;
   const original = native.stream.bind(native);
   let call = 0;
   return sinon.stub(native, "stream").callsFake(async (sql: string) => {
      const error = await fail(++call);
      if (error) throw error;
      return original(sql);
   });
}

const expiredToken = () =>
   new Error("HTTP Error: ExpiredToken: The provided token has expired.");

/** A connection that can be handed a storage-secret renewer, as a chain secret's is. */
type Renewable = DuckDBConnection & {
   setStorageSecretRenewer(renew: () => Promise<void>): void;
};

// The raw SQL route streams, so a stream on a DuckLake destination needs
// runSQL's recovery: renewing an expired credential, and confirming a lost
// catalog so the next lookup reopens it.
describe("a stream on a DuckLake destination", () => {
   let root: string;

   beforeEach(() => {
      root = mkdtempSync(join(tmpdir(), "ducklake-stream-"));
   });

   afterEach(() => {
      sinon.restore();
      rmSync(root, { recursive: true, force: true });
   });

   async function lookUp(
      env: ReturnType<typeof buildEnvironmentMalloyConfig>,
   ): Promise<Renewable> {
      return (await env.malloyConfig.connections.lookupConnection(
         "credible",
      )) as Renewable;
   }

   /**
    * Replace the driver's stream with one per call from `streams`, each
    * yielding its rows and then throwing its error, if any.
    */
   function stubStreams(
      streams: { rows: unknown[]; error?: Error }[],
   ): sinon.SinonStub {
      let call = 0;
      return sinon
         .stub(DuckDBConnection.prototype, "runSQLStream")
         .callsFake(async function* () {
            const next = streams[call++];
            if (!next) throw new Error("unexpected extra stream");
            for (const row of next.rows) yield row as never;
            if (next.error) throw next.error;
         });
   }

   it("confirms a lost catalog before the first row, and the next lookup heals it", async () => {
      const { stateOf } = stubDuckLake({});
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      try {
         await lookUp(env);
         const connection = await lookUp(env);
         const state = stateOf(connection);

         // A misbinding keeps its own error.
         const misbound = await drain(
            connection.runSQLStream("SELECT * FROM wrong_catalog.main.t"),
         );
         expect(misbound.error).toBeDefined();
         expect(misbound.error).not.toBeInstanceOf(
            DuckLakeCatalogUnavailableError,
         );

         await loseCatalog(connection, state);
         const lost = await drain(
            connection.runSQLStream("SELECT * FROM credible.main.t"),
         );
         expect(lost.rows).toEqual([]);
         expect(lost.error).toBeInstanceOf(DuckLakeCatalogUnavailableError);
         expect(isDestinationCatalogUnavailableError(lost.error)).toBe(true);

         // The cached check was dropped: the next lookup checks again,
         // reopens, and streams from the fresh instance.
         const checksBefore = state.catalogChecks;
         const healed = await lookUp(env);
         expect(state.catalogChecks).toBe(checksBefore + 1);
         expect(healed).not.toBe(connection);
         const read = await drain(
            healed.runSQLStream(
               "SELECT x FROM credible.main.t UNION ALL SELECT 7 AS x",
            ),
         );
         expect(read.error).toBeUndefined();
         expect(read.rows).toEqual([{ x: 7 }]);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });

   it("renews an expired credential refused before the first row, and streams again once", async () => {
      stubDuckLake({});
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      try {
         const connection = await lookUp(env);
         const renew = sinon.fake.resolves(undefined);
         connection.setStorageSecretRenewer(renew);
         const stream = stubStreams([
            {
               rows: [],
               error: new Error("HTTP Error: ExpiredToken: token expired"),
            },
            { rows: [{ x: 1 }, { x: 2 }] },
         ]);

         const read = await drain(
            connection.runSQLStream("SELECT x FROM credible.main.t"),
         );
         expect(read.error).toBeUndefined();
         expect(read.rows).toEqual([{ x: 1 }, { x: 2 }]);
         expect(renew.callCount).toBe(1);
         expect(stream.callCount).toBe(2);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });

   it("renews only once: a second expiry is thrown", async () => {
      stubDuckLake({});
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      try {
         const connection = await lookUp(env);
         const renew = sinon.fake.resolves(undefined);
         connection.setStorageSecretRenewer(renew);
         const second = new Error("HTTP Error: ExpiredToken: still expired");
         stubStreams([
            { rows: [], error: new Error("HTTP Error: ExpiredToken: expired") },
            { rows: [], error: second },
         ]);

         const read = await drain(
            connection.runSQLStream("SELECT x FROM credible.main.t"),
         );
         expect(read.error).toBe(second);
         expect(renew.callCount).toBe(1);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });

   it("shares one renewal between two streams that both meet the expired credential", async () => {
      stubDuckLake({});
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      try {
         const connection = await lookUp(env);
         // The gate hands the connection to the second stream as soon as the
         // first fails, so the second fails while the first's renewal runs.
         const renewing = signal();
         const secondFailed = signal();
         let renewed = false;
         const renew = sinon.fake(async () => {
            renewing.fire();
            await within(secondFailed.promise, 2000);
            renewed = true;
         });
         connection.setStorageSecretRenewer(renew);
         failNativeStreams(connection, async (call) => {
            if (renewed) return undefined;
            if (call === 2) {
               await within(renewing.promise, 2000);
               secondFailed.fire();
            }
            return expiredToken();
         });

         const [a, b] = await Promise.all([
            drain(connection.runSQLStream("SELECT 1 AS x")),
            drain(connection.runSQLStream("SELECT 1 AS x")),
         ]);
         expect(a).toEqual({ rows: [{ x: 1 }] });
         expect(b).toEqual({ rows: [{ x: 1 }] });
         expect(renew.callCount).toBe(1);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });

   it("shares one renewal between two statements that both meet the expired credential", async () => {
      const renewing = signal();
      const secondFailed = signal();
      let renewed = false;
      let reads = 0;
      stubDuckLake({
         beforeRead: async (sql) => {
            if (!sql.includes("expiring") || renewed) return;
            if (++reads === 2) {
               await within(renewing.promise, 2000);
               secondFailed.fire();
            }
            throw expiredToken();
         },
      });
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      try {
         const connection = await lookUp(env);
         const renew = sinon.fake(async () => {
            renewing.fire();
            await within(secondFailed.promise, 2000);
            renewed = true;
         });
         connection.setStorageSecretRenewer(renew);

         const results = await Promise.allSettled([
            connection.runSQL("SELECT 1 AS expiring"),
            connection.runSQL("SELECT 1 AS expiring"),
         ]);
         expect(results.map((r) => r.status)).toEqual([
            "fulfilled",
            "fulfilled",
         ]);
         expect(renew.callCount).toBe(1);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });

   it("throws each statement its own expired credential when the renewal they share fails", async () => {
      const renewing = signal();
      const secondFailed = signal();
      let reads = 0;
      stubDuckLake({
         beforeRead: async (sql) => {
            if (!sql.includes("expiring")) return;
            if (++reads === 2) {
               await within(renewing.promise, 2000);
               secondFailed.fire();
            }
            throw expiredToken();
         },
      });
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      try {
         const connection = await lookUp(env);
         const renew = sinon.fake(async () => {
            renewing.fire();
            await within(secondFailed.promise, 2000);
            await new Promise((r) => setTimeout(r, 20));
            throw new Error("STS: Could not connect to sts.amazonaws.com");
         });
         connection.setStorageSecretRenewer(renew);

         const results = await Promise.allSettled([
            connection.runSQL("SELECT 1 AS expiring"),
            connection.runSQL("SELECT 1 AS expiring"),
         ]);
         for (const result of results) {
            expect(result.status).toBe("rejected");
            const reason = (result as PromiseRejectedResult).reason;
            expect(String(reason)).toContain("ExpiredToken");
            expect(String(reason)).not.toContain("STS");
         }
         expect(renew.callCount).toBe(1);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });

   it("throws a renewal whose own statement meets the expired credential, rather than wait on itself", async () => {
      stubDuckLake({
         beforeRead: async (sql) => {
            if (sql.includes("expiring")) throw expiredToken();
         },
      });
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      try {
         const connection = await lookUp(env);
         const renew = sinon.fake(async () => {
            await connection.runSQL("SELECT 'secret' AS expiring");
         });
         connection.setStorageSecretRenewer(renew);
         const failure = await connection.runSQL("SELECT 1 AS expiring").then(
            () => undefined,
            (error: unknown) => error,
         );
         expect(String(failure)).toContain("ExpiredToken");
         expect(renew.callCount).toBe(1);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });

   it("confirms a catalog lost while the credential renewed, on a statement's run after the renewal", async () => {
      let renewed = false;
      const { stateOf } = stubDuckLake({
         beforeRead: async (sql) => {
            if (sql.includes("credible.main.t") && !renewed) {
               throw expiredToken();
            }
         },
      });
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      try {
         const connection = await lookUp(env);
         const state = stateOf(connection);
         connection.setStorageSecretRenewer(async () => {
            state.attached.delete("credible");
            renewed = true;
         });
         const failure = await connection
            .runSQL("SELECT count(*) FROM credible.main.t")
            .then(
               () => undefined,
               (error: unknown) => error,
            );
         expect(failure).toBeInstanceOf(DuckLakeCatalogUnavailableError);
         // The cached check was dropped: the next lookup checks again.
         const checksBefore = state.catalogChecks;
         expect(await lookUp(env)).not.toBe(connection);
         expect(state.catalogChecks).toBe(checksBefore + 1);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });

   it("confirms a catalog lost while the credential renewed, on a stream's run after the renewal", async () => {
      const { stateOf } = stubDuckLake({});
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      try {
         await lookUp(env);
         const connection = await lookUp(env);
         const state = stateOf(connection);
         let renewed = false;
         connection.setStorageSecretRenewer(async () => {
            await loseCatalog(connection, state);
            renewed = true;
         });
         failNativeStreams(connection, async () =>
            renewed ? undefined : expiredToken(),
         );
         const read = await drain(
            connection.runSQLStream("SELECT * FROM credible.main.t"),
         );
         expect(read.error).toBeInstanceOf(DuckLakeCatalogUnavailableError);
         const checksBefore = state.catalogChecks;
         expect(await lookUp(env)).not.toBe(connection);
         expect(state.catalogChecks).toBe(checksBefore + 1);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });

   it("releases the driver's stream when the consumer stops at the first row", async () => {
      stubDuckLake({});
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      try {
         const connection = await lookUp(env);
         let released = false;
         sinon
            .stub(DuckDBConnection.prototype, "runSQLStream")
            .callsFake(async function* () {
               try {
                  yield { x: 1 } as never;
                  yield { x: 2 } as never;
               } finally {
                  released = true;
               }
            });
         // As the raw SQL route's byte cap does on a first row too wide.
         for await (const row of connection.runSQLStream("SELECT 1")) {
            expect(row).toEqual({ x: 1 });
            break;
         }
         expect(released).toBe(true);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });

   it("throws an error after rows were yielded as it is, without renewing or running again", async () => {
      const { stateOf } = stubDuckLake({});
      const env = buildEnvironmentMalloyConfig([DESTINATION], root, false, {
         origin: "storage-destination",
      });
      try {
         const connection = await lookUp(env);
         const renew = sinon.fake.resolves(undefined);
         connection.setStorageSecretRenewer(renew);
         const expired = new Error("HTTP Error: ExpiredToken: mid-stream");
         const stream = stubStreams([
            { rows: [{ x: 1 }, { x: 2 }], error: expired },
         ]);

         const read = await drain(
            connection.runSQLStream("SELECT x FROM credible.main.t"),
         );
         expect(read.rows).toEqual([{ x: 1 }, { x: 2 }]);
         expect(read.error).toBe(expired);
         expect(renew.callCount).toBe(0);
         expect(stream.callCount).toBe(1);

         // A binder error after rows is not checked against the instance
         // either: the rows already sent are the caller's answer to judge.
         stream.restore();
         stateOf(connection).attached.delete("credible");
         const binder = new Error(
            'Binder Error: Catalog "credible" does not exist!',
         );
         stubStreams([{ rows: [{ x: 1 }], error: binder }]);
         const checksBefore = stateOf(connection).catalogChecks;
         const late = await drain(
            connection.runSQLStream("SELECT x FROM credible.main.t"),
         );
         expect(late.rows).toEqual([{ x: 1 }]);
         expect(late.error).toBe(binder);
         expect(stateOf(connection).catalogChecks).toBe(checksBefore);
      } finally {
         await env.releaseConnections().catch(() => {});
      }
   });
});
