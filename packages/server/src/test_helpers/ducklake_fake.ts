// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A DuckLake storage destination with every statement its lookup issues
// stubbed on a real, in-memory DuckDB instance: what is attached, and whether
// the instance is locked, kept per instance. A read of a catalog that is not
// attached fails with DuckDB's binder error, as a lost catalog does.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { MalloyQueryData } from "@malloydata/malloy";
import sinon from "sinon";
import type { components } from "../api";

type ApiConnection = components["schemas"]["Connection"];

export const DESTINATION = {
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
export interface FakeInstance {
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
export function stubDuckLake(hooks: {
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
         // A real stand-in catalog on the instance, so a compile's schema
         // read (`runRawSQL`, not stubbed) finds its table.
         await rawSQL(this, `ATTACH OR REPLACE ':memory:' AS "${attach[1]}"`);
         await rawSQL(
            this,
            `CREATE TABLE IF NOT EXISTS "${attach[1]}".main.t (x INTEGER)`,
         );
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

/** Run `sql` on the connection's real instance, past the stub. */
export function rawSQL(c: DuckDBConnection, sql: string): Promise<unknown> {
   return (
      c as unknown as { runRawSQL(sql: string): Promise<unknown> }
   ).runRawSQL(sql);
}

/** Lose the catalog from the instance, as the race used to. */
export async function loseCatalog(
   c: DuckDBConnection,
   state: FakeInstance,
): Promise<void> {
   state.attached.delete("credible");
   await rawSQL(c, `DETACH "credible"`);
}
