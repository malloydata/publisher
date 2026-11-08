// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import type { Mutex } from "async-mutex";
import { ConnectionSaveError, internalErrorToHttpError } from "../errors";
import { resetInstrumentsForTesting } from "../telemetry";
import {
   type MetricsHarness,
   startMetricsHarness,
} from "../test_helpers/metrics_harness";
import { ConnectionService } from "./connection_service";
import { Environment } from "./environment";
import { EnvironmentStore } from "./environment_store";
import { installSettings } from "../test_helpers/settings_env";
import { parseEnvironmentName } from "./identity";
import { startedStore } from "../test_helpers/started_store";

/**
 * An environment's connection list has replace semantics: a PATCH that leaves
 * a connection out removes it. The database rows restore connections on
 * boot, so the removal must reach them, or the connection comes back on the
 * next restart.
 */

const ENV = "conn_env";

let root: string;
let store: EnvironmentStore | undefined;
let restoreSettings: (() => void) | undefined;

beforeEach(() => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "env-conn-persist-")),
   );
   fs.mkdirSync(path.join(root, "server"));
});

afterEach(async () => {
   await closeStore();
   restoreSettings?.();
   restoreSettings = undefined;
   fs.rmSync(root, { recursive: true, force: true });
});

async function openStore(): Promise<EnvironmentStore> {
   restoreSettings?.();
   restoreSettings = installSettings({
      cwd: path.join(root, "server"),
   }).restore;
   store = startedStore(new EnvironmentStore(path.join(root, "server")));
   await store.finishedInitialization;
   return store;
}

async function closeStore() {
   if (!store) return;
   const current = store;
   store = undefined;
   for (const env of current.getLoadedEnvironments()) {
      await env.closeAllConnections();
   }
   const internals = current as unknown as {
      storageManager: { close(): Promise<void> };
   };
   await internals.storageManager.close();
}

async function restart(): Promise<EnvironmentStore> {
   await closeStore();
   return openStore();
}

const pg = (name: string, port = 5432) =>
   ({
      name,
      type: "postgres",
      postgresConnection: {
         host: "127.0.0.1",
         port,
         userName: "u",
         databaseName: "d",
      },
   }) as never;

async function connectionNames(current: EnvironmentStore) {
   const env = await current.getEnvironment(parseEnvironmentName(ENV));
   return env
      .listApiConnections()
      .map((c) => c.name)
      .sort();
}

describe("environment connection list persistence", () => {
   it("keeps a connection a PATCH removed gone after a restart", async () => {
      const first = await openStore();
      await first.addEnvironment({
         name: ENV,
         connections: [pg("keep_me"), pg("drop_me")],
      } as never);
      expect(await connectionNames(first)).toEqual(["drop_me", "keep_me"]);

      await first.updateEnvironment({
         name: ENV,
         connections: [pg("keep_me")],
      } as never);
      expect(await connectionNames(first)).toEqual(["keep_me"]);

      const second = await restart();
      expect(await connectionNames(second)).toEqual(["keep_me"]);
   });

   it("still persists an added and an updated connection across a restart", async () => {
      const first = await openStore();
      await first.addEnvironment({
         name: ENV,
         connections: [pg("a")],
      } as never);
      await first.updateEnvironment({
         name: ENV,
         connections: [pg("a", 6543), pg("b")],
      } as never);

      const second = await restart();
      expect(await connectionNames(second)).toEqual(["a", "b"]);
      const env = await second.getEnvironment(parseEnvironmentName(ENV));
      expect(env.getApiConnection("a").postgresConnection?.port).toBe(6543);
   });

   it("leaves stored connections alone when a PATCH carries no connection list", async () => {
      const first = await openStore();
      await first.addEnvironment({
         name: ENV,
         connections: [pg("a"), pg("b")],
      } as never);
      await first.updateEnvironment({ name: ENV, readme: "hi" } as never);

      const second = await restart();
      expect(await connectionNames(second)).toEqual(["a", "b"]);
   });

   it("does not prune stored connections against a list marked as a fallback", async () => {
      const first = await openStore();
      await first.addEnvironment({
         name: ENV,
         connections: [pg("a"), pg("b")],
      } as never);
      await first.addEnvironment(
         { name: ENV, connections: [pg("a")] } as never,
         false,
         { connectionsAreFallback: true },
      );

      const second = await restart();
      expect(await connectionNames(second)).toEqual(["a", "b"]);
   });

   it("keeps the row of a connection added while an environment PATCH syncs", async () => {
      const first = await openStore();
      await first.addEnvironment({
         name: ENV,
         connections: [pg("a")],
      } as never);
      const service = new ConnectionService(first);
      const repository = first.storageManager.getRepository();

      // Whether the POST's connection update found the lock taken, i.e. it
      // waits for the PATCH's sync rather than running inside it.
      let postBlocked!: () => void;
      const blocked = new Promise<void>((resolve) => (postBlocked = resolve));
      const originalExclusive =
         Environment.prototype.runConnectionUpdateExclusive;
      Environment.prototype.runConnectionUpdateExclusive = function <T>(
         this: Environment,
         fn: () => Promise<T>,
      ): Promise<T> {
         const mutex = (this as unknown as { connectionMutex: Mutex })
            .connectionMutex;
         if (mutex.isLocked()) postBlocked();
         return originalExclusive.call(this, fn) as Promise<T>;
      };

      // The PATCH's sync reads row "a" after it has snapshotted the in-memory
      // list. Right there, run the POST, and let it go as far as it can: to
      // the end when nothing serializes it, which forces the bad interleaving
      // (its row lands between the snapshot and the pruning listing).
      let post: Promise<void> | undefined;
      let armed = false;
      const originalGet = repository.getConnectionByName;
      repository.getConnectionByName = async (environmentId, name) => {
         if (armed && name === "a") {
            armed = false;
            post = service.addConnection(ENV, "b", pg("b"));
            await Promise.race([post, blocked]);
         }
         return originalGet.call(repository, environmentId, name);
      };

      try {
         armed = true;
         await first.updateEnvironment({
            name: ENV,
            connections: [pg("a")],
         } as never);
         expect(post).toBeDefined();
         await post;
      } finally {
         repository.getConnectionByName = originalGet;
         Environment.prototype.runConnectionUpdateExclusive = originalExclusive;
      }

      expect(await connectionNames(first)).toEqual(["a", "b"]);
      const dbEnvironment = await repository.getEnvironmentByName(ENV);
      const rows = await repository.listConnections(dbEnvironment!.id);
      expect(rows.map((row) => row.name).sort()).toEqual(["a", "b"]);

      const second = await restart();
      expect(await connectionNames(second)).toEqual(["a", "b"]);
   });

   it("fails the update when the connection write fails, instead of answering success", async () => {
      const first = await openStore();
      await first.addEnvironment({
         name: ENV,
         connections: [pg("a")],
      } as never);
      const repository = first.storageManager.getRepository();
      const original = repository.createConnection;
      repository.createConnection = () =>
         Promise.reject(new Error("disk full"));
      try {
         const failure = await first
            .updateEnvironment({
               name: ENV,
               connections: [pg("a"), pg("b")],
            } as never)
            .then(
               () => undefined,
               (error: unknown) => error,
            );
         expect(failure).toBeInstanceOf(ConnectionSaveError);
         expect((failure as Error).message).toContain('"b"');
         // The storage error is logged, not sent.
         expect((failure as Error).message).not.toContain("disk full");
         expect(
            internalErrorToHttpError(failure as Error, { log: false }).status,
         ).toBe(500);
      } finally {
         repository.createConnection = original;
      }
   });
});

/**
 * A connection row that fails to save must not cut the rest of the sync short:
 * memory has already changed, and an environment whose package rows were
 * never written comes back from a restart with no packages.
 */
describe("a connection row that fails to save", () => {
   let harness: MetricsHarness;

   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetInstrumentsForTesting();
   });

   afterEach(async () => {
      await harness.shutdown();
      resetInstrumentsForTesting();
   });

   function packageDir(): string {
      const dir = path.join(root, "sales_source");
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "sales", description: "Sales" }),
      );
      fs.writeFileSync(
         path.join(dir, "sales.malloy"),
         "source: one is duckdb.sql('select 1 as x')\n",
      );
      return dir;
   }

   const environmentPayload = () =>
      ({
         name: ENV,
         connections: [pg("a"), pg("b")],
         storageDestinations: [
            {
               name: "lake",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
                     postgresConnection: {
                        host: "catalog.internal",
                        port: 5432,
                        databaseName: "ducklake",
                        userName: "publisher",
                     },
                  },
                  storage: { bucketUrl: "gs://bucket/lake" },
               },
            },
         ],
         packages: [{ name: "sales", location: packageDir() }],
      }) as never;

   /** Fail the first write of connection "a"'s row; let every other through. */
   function failRowOfAOnce(current: EnvironmentStore) {
      const repository = current.storageManager.getRepository();
      const original = repository.createConnection;
      let failed = false;
      repository.createConnection = (data) => {
         if (!failed && data.name === "a") {
            failed = true;
            return Promise.reject(new Error("disk full"));
         }
         return original.call(repository, data);
      };
      return () => {
         repository.createConnection = original;
      };
   }

   async function storedRows(current: EnvironmentStore) {
      const repository = current.storageManager.getRepository();
      const dbEnvironment = await repository.getEnvironmentByName(ENV);
      expect(dbEnvironment).toBeDefined();
      const id = dbEnvironment!.id;
      return {
         connections: (await repository.listConnections(id))
            .map((row) => row.name)
            .sort(),
         destinations: (await repository.listStorageDestinations(id)).map(
            (row) => row.name,
         ),
         packages: (await repository.listPackages(id)).map((row) => row.name),
      };
   }

   it("on a request, still writes the other rows, then fails naming the connection", async () => {
      const current = await openStore();
      const restore = failRowOfAOnce(current);
      try {
         const failure = await current
            .addEnvironment(environmentPayload())
            .then(
               () => undefined,
               (error: unknown) => error,
            );
         expect(failure).toBeInstanceOf(ConnectionSaveError);
         expect((failure as ConnectionSaveError).connectionNames).toEqual([
            "a",
         ]);
         expect((failure as Error).message).toContain('"a"');
      } finally {
         restore();
      }

      expect(await storedRows(current)).toEqual({
         connections: ["b"],
         destinations: ["lake"],
         packages: ["sales"],
      });
      expect(
         await harness.collectCounter(
            "publisher_connection_save_failures_total",
            { path: "request" },
         ),
      ).toBe(1);
   });

   it("at boot, loads the environment and writes the other rows", async () => {
      const current = await openStore();
      const restore = failRowOfAOnce(current);
      let loaded: Environment;
      try {
         // The boot paths (config load, database restore) pass
         // skipInitialization.
         loaded = await current.addEnvironment(environmentPayload(), true);
      } finally {
         restore();
      }

      expect(loaded.listApiConnections().map((c) => c.name)).toEqual([
         "a",
         "b",
      ]);
      expect(current.getLoadedEnvironments()).toContain(loaded);
      expect(await storedRows(current)).toEqual({
         connections: ["b"],
         destinations: ["lake"],
         packages: ["sales"],
      });
      expect(
         await harness.collectCounter(
            "publisher_connection_save_failures_total",
            { path: "boot" },
         ),
      ).toBe(1);
   });
});
