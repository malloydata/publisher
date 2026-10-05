// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A sync that fails with a 4xx the vendor will not change its mind about (a
 * prompt past the context window, a rejected key) is not started again after
 * its cool-down. Every question after the window used to start the same sync,
 * which sent the same request and failed the same way. It is tried again when
 * the package's content or its retrieval settings change, or the server
 * restarts. A failure that can clear (429, 5xx, a timeout) still retries.
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
import {
   _clearChatModelForTests,
   _setChatModelForTests,
} from "../../providers/active";
import { createChatModel } from "../../providers/registry";
import { setRetrievalConfig } from "../../retrieval_config";
import { EmbeddingProvider } from "../../service/embedding_provider";
import type { Package } from "../../service/package";
import type { PackageRetrievalSettings } from "../../service/package_retrieval";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import {
   createEntityEmbeddingsTable,
   createEntityKeyphrasesTable,
   createSourceSummariesTable,
} from "../../storage/duckdb/schema";
import {
   instantRetry,
   jsonResponse,
   stubFetch,
} from "../../test_helpers/fetch_stub";
import {
   _resetEmbeddingIndexStateForTests,
   _setTimingForTests,
   getEmbeddingIndexStatus,
   trySemanticSearch,
   type EmbeddableEntity,
} from "./embedding_index";
import { embeddingSyncQueue } from "./embedding_sync_queue";

let tempDir: string;
let db: DuckDBConnection;

beforeAll(async () => {
   tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "sync-client-error-"));
   db = new DuckDBConnection(path.join(tempDir, "test.db"));
   await db.initialize();
   await createEntityEmbeddingsTable(db);
   await createEntityKeyphrasesTable(db);
   await createSourceSummariesTable(db);
});

afterAll(async () => {
   await db.close();
   fs.rmSync(tempDir, { recursive: true, force: true });
});

beforeEach(async () => {
   _resetEmbeddingIndexStateForTests();
   _setTimingForTests({ cooldownMs: 20 });
   await db.run("DELETE FROM entity_embeddings");
   await db.run("DELETE FROM entity_keyphrases");
   await db.run("DELETE FROM source_summaries");
});

afterEach(async () => {
   await embeddingSyncQueue.idle();
   _clearChatModelForTests();
   setRetrievalConfig(undefined);
});

const entity = (
   kind: string,
   name: string,
   src: string,
   embedDoc = "",
): EmbeddableEntity => ({
   kind,
   name,
   source: src,
   modelPath: "m.malloy",
   embedDoc,
   ...(kind === "source" ? {} : { dataType: "string" }),
});

const entities = (doc = "One row per order.") =>
   Object.freeze([
      entity("source", "orders", "orders", doc),
      entity("dimension", "state", "orders", "State."),
   ]);

const pkg = (retrieval: Partial<PackageRetrievalSettings> = {}): Package =>
   ({
      getRetrievalSettings: () => ({
         representation: "single",
         // Only the source summary step calls the model here.
         keyphrases: "never",
         prompts: {},
         ...retrieval,
      }),
   }) as unknown as Package;

function embedder(options: { status?: number; onRequest?: () => void } = {}) {
   const fetchStub = (async (_url: RequestInfo | URL, init?: RequestInit) => {
      options.onRequest?.();
      if (options.status) {
         return new Response("the request is not acceptable", {
            status: options.status,
         });
      }
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      return jsonResponse({
         data: body.input.map((_t, index) => ({ index, embedding: [1, 0, 1] })),
      });
   }) as typeof fetch;
   return new EmbeddingProvider(
      {
         apiKey: "k",
         model: "m",
         baseUrl: "https://stub.example.com/v1",
         minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
      },
      fetchStub,
   );
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** One search; the first one starts the sync, later ones may restart it. */
const ask = (
   provider: EmbeddingProvider,
   p: Package,
   ents: readonly EmbeddableEntity[],
) =>
   trySemanticSearch({
      db,
      provider,
      pkg: p,
      environmentName: "env",
      packageName: "pkg",
      entities: ents,
      queries: [{ targetIndex: 0, text: "find it", kinds: ["dimension"] }],
      perSourceWindow: 10,
   });

/** Ask until the sync has failed (the answer is no longer `indexing`). */
async function untilFailed(
   provider: EmbeddingProvider,
   p: Package,
   ents: readonly EmbeddableEntity[],
) {
   for (let i = 0; i < 400; i++) {
      const r = await ask(provider, p, ents);
      if (!("unavailable" in r) || r.unavailable !== "indexing") return r;
      await sleep(5);
   }
   throw new Error("the sync never failed");
}

const status = (
   provider: EmbeddingProvider,
   p: Package,
   ents: readonly EmbeddableEntity[],
) => getEmbeddingIndexStatus(db, provider, "env", "pkg", ents, p);

/** Longer than any test runs, so a cool-down never lapses on its own. */
const HELD_COOLDOWN_MS = 60_000;

/**
 * Ask one question with the cool-down already over, let the sync it starts
 * finish, then hold the cool-down again.
 *
 * A retryable failure restarts its sync on the first question after the
 * cool-down. With a short real cool-down, `untilFailed`'s poll can itself land
 * after it lapses and restart the sync, so the request count depends on timer
 * resolution (about 15ms on Windows) rather than on the code under test.
 * Holding the cool-down while polling, and ending it only here, makes "the
 * question after the cool-down" exactly one question. The cool-down stays over
 * until the queue is idle because the queued sync checks the provider breaker
 * (which shares the cool-down) when it runs, not when it is asked for.
 */
async function askAfterCooldown(
   provider: EmbeddingProvider,
   p: Package,
   ents: readonly EmbeddableEntity[],
) {
   _setTimingForTests({ cooldownMs: 0 });
   try {
      const answer = await ask(provider, p, ents);
      await embeddingSyncQueue.idle();
      return answer;
   } finally {
      _setTimingForTests({ cooldownMs: HELD_COOLDOWN_MS });
   }
}

describe("a sync that fails with a client error", () => {
   const llm = (statusCode: number) => {
      const stub = stubFetch([
         () =>
            new Response("maximum context length exceeded", {
               status: statusCode,
            }),
      ]);
      _setChatModelForTests(
         createChatModel(
            {
               provider: "openai-compatible",
               model: "m",
               baseUrl: "https://llm.example.com/v1",
               apiKey: "k",
               timeoutMs: 5_000,
               concurrency: 1,
               maxCallsPerSync: 10,
               maxCallsPerRequest: 5,
            },
            {
               fetchFn: stub.fetchFn,
               retry: { ...instantRetry(), maxAttempts: 1 },
            },
         ),
         { concurrency: 1 },
      );
      return stub;
   };

   it("is not started again by the questions that follow its cool-down (an LLM step)", async () => {
      const stub = llm(400);
      const provider = embedder();
      const p = pkg({ sourceSummary: { enabled: true } });
      const ents = entities();
      const first = await untilFailed(provider, p, ents);
      expect("unavailable" in first && first.unavailable).toBe("cooldown");
      expect(stub.requests).toHaveLength(1);

      // Well past the cool-down, with many questions.
      await sleep(60);
      for (let i = 0; i < 5; i++) {
         await ask(provider, p, ents);
         await sleep(5);
      }
      await embeddingSyncQueue.idle();
      expect(stub.requests).toHaveLength(1);

      const s = await status(provider, p, ents);
      expect(s.status).toBe("error");
      expect(s.stage).toBe("source_summary");
      expect(s.lastError?.retryAt).toBeUndefined();
      expect(s.lastError?.message).toContain("will not be retried");
      expect(s.lastError?.message).toContain("(400)");
   });

   it("is not started again when the embedding call itself is refused with a 4xx", async () => {
      let requests = 0;
      const provider = embedder({ status: 400, onRequest: () => requests++ });
      const p = pkg();
      const ents = entities();
      await untilFailed(provider, p, ents);
      expect(requests).toBe(1);
      await sleep(60);
      for (let i = 0; i < 5; i++) {
         await ask(provider, p, ents);
         await sleep(5);
      }
      await embeddingSyncQueue.idle();
      expect(requests).toBe(1);
   });

   it("is tried again when the package's content changes", async () => {
      const stub = llm(400);
      const provider = embedder();
      const p = pkg({ sourceSummary: { enabled: true } });
      await untilFailed(provider, p, entities());
      expect(stub.requests).toHaveLength(1);
      await sleep(60);
      // An edit to a doc is new content: it gets its own attempt.
      await untilFailed(provider, p, entities("Now it is documented better."));
      expect(stub.requests.length).toBeGreaterThan(1);
   });

   it("still retries after the cool-down when the credential may have been fixed (a 401)", async () => {
      // An auth failure can clear without the package changing: a token is
      // refreshed, a key rotated. It keeps its cool-down and its retry.
      _setTimingForTests({ cooldownMs: HELD_COOLDOWN_MS });
      const stub = llm(401);
      const provider = embedder();
      const p = pkg({ sourceSummary: { enabled: true } });
      const ents = entities();
      await untilFailed(provider, p, ents);
      expect(stub.requests).toHaveLength(1);
      await askAfterCooldown(provider, p, ents);
      await untilFailed(provider, p, ents);
      expect(stub.requests).toHaveLength(2);
      expect(
         (await status(provider, p, ents)).lastError?.retryAt,
      ).toBeDefined();
   });

   it("still retries after the cool-down when the failure can clear (a 503)", async () => {
      _setTimingForTests({ cooldownMs: HELD_COOLDOWN_MS });
      const stub = llm(503);
      const provider = embedder();
      const p = pkg({ sourceSummary: { enabled: true } });
      const ents = entities();
      await untilFailed(provider, p, ents);
      expect(stub.requests).toHaveLength(1);
      await askAfterCooldown(provider, p, ents);
      await untilFailed(provider, p, ents);
      expect(stub.requests).toHaveLength(2);
      const s = await status(provider, p, ents);
      expect(s.lastError?.retryAt).toBeDefined();
   });
});
