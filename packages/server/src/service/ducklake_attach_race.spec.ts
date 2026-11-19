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
         };
         states.set(key, state);
      }
      return state;
   };
   let secrets = 0;
   let attachCount = 0;
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
});
