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
   _setTimingForTests,
   deletePackageEmbeddings,
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

   it("writes nothing when the package is deleted while its job is being prepared", async () => {
      const held = heldProvider();
      let startPrepare!: () => void;
      let finishPrepare!: () => void;
      const started = new Promise<void>((r) => (startPrepare = r));
      const gate = new Promise<void>((r) => (finishPrepare = r));
      enqueuePackageSync({
         pkg: {} as unknown as Package,
         environmentName: "env",
         packageName: "deleted_mid_prepare",
         prepare: async () => {
            startPrepare();
            await gate;
            return {
               db,
               provider: held.provider,
               entities: Object.freeze([ent("late_field")]),
            };
         },
      });
      await started;
      // The delete lands while the job is still building its entity list.
      await deletePackageEmbeddings(db, "env", "deleted_mid_prepare");
      finishPrepare();
      await embeddingSyncQueue.idle();

      expect(held.requests.length).toBe(0);
      const rows = await db.all(
         "SELECT 1 FROM entity_embeddings WHERE package_name = 'deleted_mid_prepare'",
      );
      expect(rows.length).toBe(0);
   });

   it("keeps syncing a package whose delete failed, since its rows are still there", async () => {
      const held = heldProvider();
      const brokenDb = {
         run: async () => {
            throw new Error("disk full");
         },
      } as unknown as DuckDBConnection;
      await expect(
         deletePackageEmbeddings(brokenDb, "env", "delete_failed"),
      ).rejects.toThrow("disk full");

      enqueuePackageSync({
         pkg: {} as unknown as Package,
         environmentName: "env",
         packageName: "delete_failed",
         prepare: async () => ({
            db,
            provider: held.provider,
            entities: Object.freeze([ent("kept_field")]),
         }),
      });
      await held.arrived(1);
      held.release();
      await embeddingSyncQueue.idle();
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

/**
 * A store holding `pkgs`. `diskLoads` lists every package the code asked the
 * store to LOAD (getPackage loads from disk on a miss, as the real one does);
 * peekPackage only reports what is held.
 */
function storeHolding(pkgs: Record<string, ReturnType<typeof smallPackage>>) {
   const diskLoads: string[] = [];
   const environment = {
      getPackage: async (name: string) => {
         if (!pkgs[name]) diskLoads.push(name);
         return pkgs[name] ?? smallPackage(name);
      },
      peekPackage: (name: string) => pkgs[name],
      getStaleCompileErrors: () => new Map(),
   };
   return {
      getEnvironment: async () => environment,
      peekEnvironment: () => environment,
      storageManager: { getDuckDbConnection: () => db },
      diskLoads,
   } as unknown as EnvironmentStore & { diskLoads: string[] };
}

describe("startPackageEmbeddingSync", () => {
   it("does not load or embed a package that was unloaded while it waited", async () => {
      const held = heldProvider();
      _setEmbeddingProviderForTests(held.provider);
      const pkg = smallPackage("unloaded");
      // The store no longer holds the package by the time the job runs.
      const store = storeHolding({});

      startPackageEmbeddingSync(store, "env", pkg as unknown as Package);
      await embeddingSyncQueue.idle();

      expect(store.diskLoads).toEqual([]);
      expect(held.requests.length).toBe(0);
   });

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

describe("a provider that is down, and a sync that runs too long", () => {
   const stubProvider = (fetchStub: typeof fetch) =>
      new EmbeddingProvider(
         {
            apiKey: "test",
            model: "stub-model",
            baseUrl: "https://stub.example.com/v1",
            minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
         },
         fetchStub,
      );

   it("sends nothing for the packages queued behind one that found the provider down", async () => {
      let requests = 0;
      const down = stubProvider((async () => {
         requests++;
         return new Response("secret provider detail", { status: 503 });
      }) as unknown as typeof fetch);
      _setEmbeddingProviderForTests(down);
      const first = smallPackage("down_a");
      const second = smallPackage("down_b");
      const store = storeHolding({ down_a: first, down_b: second });

      startPackageEmbeddingSync(store, "env", first as unknown as Package);
      startPackageEmbeddingSync(store, "env", second as unknown as Package);
      await embeddingSyncQueue.idle();

      const afterFirstPackage = requests;
      expect(afterFirstPackage).toBeGreaterThan(0);
      // The second package never asked: its status carries the same cause.
      const a = await getPackageEmbeddingStatus(store, "env", "down_a");
      const b = await getPackageEmbeddingStatus(store, "env", "down_b");
      expect(b.status).toBe("error");
      expect(b.reason).toBe("cooldown");
      expect(b.lastError?.message).toBe(a.lastError?.message);
      expect(requests).toBe(afterFirstPackage);
   });

   it("shows a cause that names the status but not the provider URL", async () => {
      const down = stubProvider(
         (async () =>
            new Response("secret provider detail", {
               status: 503,
            })) as unknown as typeof fetch,
      );
      _setEmbeddingProviderForTests(down);
      const pkg = smallPackage("wording");
      const store = storeHolding({ wording: pkg });
      startPackageEmbeddingSync(store, "env", pkg as unknown as Package);
      await embeddingSyncQueue.idle();

      const message = (await getPackageEmbeddingStatus(store, "env", "wording"))
         .lastError?.message;
      expect(message).toContain("503");
      expect(message).not.toContain("stub.example.com");
   });

   it("stops a sync that outlasts its time limit, keeps what it saved, and frees the queue", async () => {
      _setSyncRetryForTests({ batchSize: 1 });
      _setTimingForTests({ syncDeadlineMs: 30 });
      let requests = 0;
      const slow = stubProvider((async (
         _url: RequestInfo | URL,
         init?: RequestInit,
      ) => {
         requests++;
         const body = JSON.parse(String(init?.body)) as { input: string[] };
         await new Promise((resolve) => setTimeout(resolve, 60));
         return new Response(
            JSON.stringify({
               data: body.input.map((_, index) => ({
                  index,
                  embedding: [1, 0, 0],
               })),
            }),
            { status: 200 },
         );
      }) as unknown as typeof fetch);
      _setEmbeddingProviderForTests(slow);
      const pkg = smallPackage("slow");
      const store = storeHolding({ slow: pkg });
      startPackageEmbeddingSync(store, "env", pkg as unknown as Package);
      await embeddingSyncQueue.idle();

      // One request fits before the limit; the other two never go out.
      expect(requests).toBe(1);
      const status = await getPackageEmbeddingStatus(store, "env", "slow");
      expect(status.status).toBe("error");
      expect(status.reason).toBe("cooldown");
      expect(status.lastError?.message).toContain("time limit");
      expect(status.embeddedRows).toBe(1);
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
      // The status is shown to callers, so it never names the endpoint.
      expect(status.lastError?.message).not.toContain("stub.example.com");
      expect(Date.parse(status.lastError!.retryAt!)).toBeGreaterThan(before);
      expect(status.embeddedRows).toBe(0);
   });
});
