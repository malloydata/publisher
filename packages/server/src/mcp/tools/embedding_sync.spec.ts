// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The embedding sync as the server runs it: queued when a package loads,
 * one package at a time, saving as it goes, with its progress readable from
 * the package's status. The sync itself (diffing, batching, retrying) is in
 * embedding_index.spec.ts; this file is about when it starts and what it
 * reports.
 */

import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../config";
import type { EnvironmentStore } from "../../service/environment_store";
import type { Package } from "../../service/package";
import {
   EmbeddingProvider,
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createEntityEmbeddingsTable } from "../../storage/duckdb/schema";
import {
   _resetEmbeddingIndexStateForTests,
   _setSyncRetryForTests,
   enqueuePackageSync,
   type EmbeddableEntity,
} from "./embedding_index";
import { SerialQueue, embeddingSyncQueue } from "./embedding_sync_queue";
import {
   getPackageEmbeddingStatus,
   startPackageEmbeddingSync,
} from "./get_context_tool";

let tempDir: string;
let db: DuckDBConnection;

beforeAll(async () => {
   tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "embedding-sync-spec-"));
   db = new DuckDBConnection(path.join(tempDir, "sync.db"));
   await db.initialize();
   await createEntityEmbeddingsTable(db);
});

afterAll(async () => {
   await db.close();
   fs.rmSync(tempDir, { recursive: true, force: true });
});

beforeEach(async () => {
   _resetEmbeddingIndexStateForTests();
   await db.run("DELETE FROM entity_embeddings");
});

afterEach(async () => {
   await embeddingSyncQueue.idle();
   _setEmbeddingProviderForTests(null);
   _clearEmbeddingProviderForTests();
});

// ---------------------------------------------------------------------------
// The queue
// ---------------------------------------------------------------------------

describe("SerialQueue", () => {
   it("runs jobs one after another, in the order queued", async () => {
      const queue = new SerialQueue();
      const events: string[] = [];
      let releaseFirst!: () => void;
      const firstGate = new Promise<void>((resolve) => {
         releaseFirst = resolve;
      });
      const first = queue.enqueue(async () => {
         events.push("first:start");
         await firstGate;
         events.push("first:end");
      });
      const second = queue.enqueue(async () => {
         events.push("second:start");
         events.push("second:end");
      });
      // Let the event loop run: the second job must not start while the first
      // is held.
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(events).toEqual(["first:start"]);
      expect(queue.size).toBe(2);

      releaseFirst();
      await Promise.all([first, second]);
      expect(events).toEqual([
         "first:start",
         "first:end",
         "second:start",
         "second:end",
      ]);
      expect(queue.size).toBe(0);
   });

   it("keeps going after a job throws", async () => {
      const queue = new SerialQueue();
      const events: string[] = [];
      const failed = queue.enqueue(async () => {
         throw new Error("boom");
      });
      const after = queue.enqueue(async () => {
         events.push("after");
      });
      await Promise.all([failed, after]);
      expect(events).toEqual(["after"]);
   });
});

// ---------------------------------------------------------------------------
// Queueing a package's sync
// ---------------------------------------------------------------------------

const ent = (name: string): EmbeddableEntity => ({
   kind: "dimension",
   name,
   source: "src",
   modelPath: "m.malloy",
   embedDoc: "",
});

/** A provider whose every request can be held, and which records them. */
function heldProvider() {
   const requests: string[][] = [];
   const releases: Array<() => void> = [];
   const fetchStub = (async (_url: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      requests.push(body.input);
      await new Promise<void>((resolve) => releases.push(resolve));
      const data = body.input.map((_, index) => ({
         index,
         embedding: [1, 0, 0],
      }));
      return new Response(JSON.stringify({ data }), { status: 200 });
   }) as typeof fetch;
   const provider = new EmbeddingProvider(
      {
         apiKey: "test",
         model: "stub-model",
         baseUrl: "https://stub.example.com/v1",
         minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
      },
      fetchStub,
   );
   /** Wait until `n` requests have arrived, then let the next one through. */
   const arrived = async (n: number) => {
      for (let i = 0; i < 400 && requests.length < n; i++) {
         await new Promise((resolve) => setTimeout(resolve, 5));
      }
      expect(requests.length).toBeGreaterThanOrEqual(n);
   };
   const release = () => releases.shift()!();
   return { provider, requests, arrived, release };
}

describe("enqueuePackageSync", () => {
   it("queues once per Package instance, and again for a reloaded one", async () => {
      const { provider } = heldProvider();
      let prepared = 0;
      const prepare = async () => {
         prepared++;
         return undefined; // nothing to sync: only the call count matters
      };
      const pkg = {} as unknown as Package;
      enqueuePackageSync({
         pkg,
         environmentName: "env",
         packageName: "once",
         prepare,
      });
      enqueuePackageSync({
         pkg,
         environmentName: "env",
         packageName: "once",
         prepare,
      });
      await embeddingSyncQueue.idle();
      expect(prepared).toBe(1);

      enqueuePackageSync({
         pkg: {} as unknown as Package,
         environmentName: "env",
         packageName: "once",
         prepare,
      });
      await embeddingSyncQueue.idle();
      expect(prepared).toBe(2);
      expect(provider).toBeDefined();
   });

   it("syncs two packages one after another, not together", async () => {
      const held = heldProvider();
      const order: string[] = [];
      const queueFor = (packageName: string) =>
         enqueuePackageSync({
            pkg: {} as unknown as Package,
            environmentName: "env",
            packageName,
            prepare: async () => {
               order.push(`${packageName}:prepared`);
               return {
                  db,
                  provider: held.provider,
                  entities: Object.freeze([ent(`${packageName}_field`)]),
               };
            },
         });
      queueFor("first");
      queueFor("second");

      // The first package's request is on the wire and held. The second
      // package has not even been prepared: it waits for the first to finish.
      await held.arrived(1);
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(order).toEqual(["first:prepared"]);
      expect(held.requests.length).toBe(1);

      held.release();
      await held.arrived(2);
      expect(order).toEqual(["first:prepared", "second:prepared"]);
      // The first package's rows were saved before the second began.
      const saved = await db.all<{ package_name: string }>(
         "SELECT DISTINCT package_name FROM entity_embeddings",
      );
      expect(saved.map((r) => r.package_name)).toEqual(["first"]);
      held.release();
      await embeddingSyncQueue.idle();
   });
});

// ---------------------------------------------------------------------------
// Starting on load, and what the status says
// ---------------------------------------------------------------------------

const field = (kind: string, name: string) => ({
   kind,
   name,
   annotations: [],
});

/** A package with one source and two fields: three embedded rows. */
function smallPackage(name: string) {
   const models: Record<string, unknown> = {
      "m.malloy": {
         getSourceInfos: () => [
            {
               name: "src",
               annotations: [],
               schema: {
                  fields: [
                     field("dimension", "alpha"),
                     field("dimension", "beta"),
                  ],
               },
            },
         ],
         getQueries: () => [],
      },
   };
   return {
      getPackageName: () => name,
      listModels: async () => Object.keys(models).map((p) => ({ path: p })),
      getModel: (p: string) => models[p],
   };
}

function storeHolding(pkgs: Record<string, ReturnType<typeof smallPackage>>) {
   return {
      getEnvironment: async () => ({
         getPackage: async (name: string) => pkgs[name],
         getStaleCompileErrors: () => new Map(),
      }),
      storageManager: { getDuckDbConnection: () => db },
   } as unknown as EnvironmentStore;
}

describe("startPackageEmbeddingSync", () => {
   it("builds the index at load, with no question asked, and reports ready", async () => {
      const held = heldProvider();
      _setEmbeddingProviderForTests(held.provider);
      const pkg = smallPackage("loaded");
      const store = storeHolding({ loaded: pkg });

      startPackageEmbeddingSync(store, "env", pkg as unknown as Package);
      await held.arrived(1);
      held.release();
      await embeddingSyncQueue.idle();

      const status = await getPackageEmbeddingStatus(store, "env", "loaded");
      expect(status.status).toBe("ready");
      expect(status.embeddedRows).toBe(3);
      expect(status.totalRows).toBe(3);
      expect(status.startedAt).toBeUndefined();
   });

   it("does nothing for a reloaded package whose content is unchanged", async () => {
      const held = heldProvider();
      _setEmbeddingProviderForTests(held.provider);
      const first = smallPackage("reloaded");
      const store = storeHolding({ reloaded: first });
      startPackageEmbeddingSync(store, "env", first as unknown as Package);
      await held.arrived(1);
      held.release();
      await embeddingSyncQueue.idle();
      expect(held.requests.length).toBe(1);

      // A reload: a new instance with the same content. It is queued, finds
      // the stored vectors current and embeds nothing.
      const second = smallPackage("reloaded");
      const reloadedStore = storeHolding({ reloaded: second });
      startPackageEmbeddingSync(
         reloadedStore,
         "env",
         second as unknown as Package,
      );
      await embeddingSyncQueue.idle();
      expect(held.requests.length).toBe(1);
      expect(
         (await getPackageEmbeddingStatus(reloadedStore, "env", "reloaded"))
            .status,
      ).toBe("ready");
   });

   it("does nothing when no embedding provider is configured", async () => {
      _setEmbeddingProviderForTests(null);
      const pkg = smallPackage("lexical");
      const store = storeHolding({ lexical: pkg });
      startPackageEmbeddingSync(store, "env", pkg as unknown as Package);
      await embeddingSyncQueue.idle();
      const rows = await db.all("SELECT 1 FROM entity_embeddings");
      expect(rows).toEqual([]);
   });

   it("counts embedded rows up batch by batch while indexing", async () => {
      // Two rows per request: three rows take two requests.
      _setSyncRetryForTests({ batchSize: 2 });
      const held = heldProvider();
      _setEmbeddingProviderForTests(held.provider);
      const pkg = smallPackage("progress");
      const store = storeHolding({ progress: pkg });

      startPackageEmbeddingSync(store, "env", pkg as unknown as Package);
      await held.arrived(1);
      let status = await getPackageEmbeddingStatus(store, "env", "progress");
      expect(status.status).toBe("indexing");
      expect(status.embeddedRows).toBe(0);
      expect(status.totalRows).toBe(3);
      expect(status.startedAt).toBeDefined();

      held.release(); // batch 1 returns and is saved
      await held.arrived(2);
      status = await getPackageEmbeddingStatus(store, "env", "progress");
      expect(status.status).toBe("indexing");
      expect(status.embeddedRows).toBe(2);
      expect(status.totalRows).toBe(3);

      held.release();
      await embeddingSyncQueue.idle();
      status = await getPackageEmbeddingStatus(store, "env", "progress");
      expect(status.status).toBe("ready");
      expect(status.embeddedRows).toBe(3);
   });
});

describe("getPackageEmbeddingStatus", () => {
   it("is lexical, by design, when no provider is configured", async () => {
      _setEmbeddingProviderForTests(null);
      const status = await getPackageEmbeddingStatus(
         storeHolding({}),
         "env",
         "any",
      );
      expect(status).toEqual({
         status: "lexical",
         embeddedRows: 0,
         totalRows: 0,
         totalEntities: 0,
         embeddedEntities: 0,
      });
   });

   it("is an error with the reason when the embedding configuration is invalid", async () => {
      const saved = {
         key: process.env.EMBEDDING_API_KEY,
         base: process.env.EMBEDDING_API_BASE,
      };
      process.env.EMBEDDING_API_KEY = "k";
      process.env.EMBEDDING_API_BASE = "not a url";
      _clearEmbeddingProviderForTests();
      try {
         const status = await getPackageEmbeddingStatus(
            storeHolding({}),
            "env",
            "any",
         );
         expect(status.status).toBe("error");
         expect(status.lastError?.message).toContain("EMBEDDING_API_BASE");
      } finally {
         if (saved.key === undefined) delete process.env.EMBEDDING_API_KEY;
         else process.env.EMBEDDING_API_KEY = saved.key;
         if (saved.base === undefined) delete process.env.EMBEDDING_API_BASE;
         else process.env.EMBEDDING_API_BASE = saved.base;
         _clearEmbeddingProviderForTests();
      }
   });

   it("is an error naming the cause, and when the next try is, after the provider fails", async () => {
      const failing = new EmbeddingProvider(
         {
            apiKey: "sk-secret-key-123",
            model: "stub-model",
            baseUrl: "https://stub.example.com/v1",
            minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
         },
         (async () =>
            new Response("no", { status: 401 })) as unknown as typeof fetch,
      );
      _setEmbeddingProviderForTests(failing);
      const pkg = smallPackage("failing");
      const store = storeHolding({ failing: pkg });
      const before = Date.now();
      startPackageEmbeddingSync(store, "env", pkg as unknown as Package);
      await embeddingSyncQueue.idle();

      const status = await getPackageEmbeddingStatus(store, "env", "failing");
      expect(status.status).toBe("error");
      expect(status.reason).toBe("cooldown");
      expect(status.lastError?.message).toContain("authentication failed");
      expect(status.lastError?.message).not.toContain("sk-secret-key-123");
      expect(Date.parse(status.lastError!.retryAt!)).toBeGreaterThan(before);
      expect(status.embeddedRows).toBe(0);
   });
});
