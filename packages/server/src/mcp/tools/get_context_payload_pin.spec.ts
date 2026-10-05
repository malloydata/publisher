// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Pins the COMPLETE get_context response for each retrieval path, so a
 * refactor of how a request is retrieved and ranked cannot change a byte of
 * what a caller receives without this file failing.
 *
 * The other get_context specs assert on the field a test is about. This one
 * asserts on all of them at once: the full JSON envelope, key order included,
 * is compared with a committed golden file
 * (testdata/get_context_payloads.golden.json).
 *
 * Regenerate the golden after an INTENDED payload change:
 *
 *    UPDATE_GOLDEN=1 bun test src/mcp/tools/get_context_payload_pin.spec.ts
 *
 * then read the diff of the golden file in review. A missing golden fails the
 * run; it is never created silently.
 *
 * Determinism: lunr and the stub embedding provider are deterministic, entity
 * ids come from names, and nothing time-based reaches the payload, so no field
 * needs normalising. The one thing that is timed is the background vector
 * sync, which these tests wait for by polling (see untilSemantic) rather than
 * by sleeping.
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
import { registerGetContextTool } from "./get_context_tool";
import { _resetEmbeddingIndexStateForTests } from "./embedding_index";
import { embeddingSyncQueue } from "./embedding_sync_queue";
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../config";
import type { EnvironmentStore } from "../../service/environment_store";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createEntityEmbeddingsTable } from "../../storage/duckdb/schema";
import {
   EmbeddingProvider,
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";

const GOLDEN_PATH = path.join(
   __dirname,
   "testdata",
   "get_context_payloads.golden.json",
);
const UPDATE = process.env.UPDATE_GOLDEN === "1";

// ---------------------------------------------------------------------------
// Golden bookkeeping
// ---------------------------------------------------------------------------

const observed: Record<string, unknown> = {};
let golden: Record<string, unknown> | undefined;

beforeAll(() => {
   if (UPDATE) return;
   if (!fs.existsSync(GOLDEN_PATH)) {
      throw new Error(
         `Golden file missing: ${GOLDEN_PATH}. Generate it with ` +
            "UPDATE_GOLDEN=1 and commit it; this spec never creates it on its own.",
      );
   }
   golden = JSON.parse(fs.readFileSync(GOLDEN_PATH, "utf8"));
});

afterAll(() => {
   _clearEmbeddingProviderForTests();
   if (UPDATE) {
      const sorted = Object.fromEntries(
         Object.entries(observed).sort(([a], [b]) => (a < b ? -1 : 1)),
      );
      fs.mkdirSync(path.dirname(GOLDEN_PATH), { recursive: true });
      fs.writeFileSync(GOLDEN_PATH, JSON.stringify(sorted, null, 2) + "\n");
      return;
   }
   // A golden entry nothing exercises any more is a scenario someone deleted.
   const stale = Object.keys(golden ?? {}).filter((k) => !(k in observed));
   if (stale.length > 0 && Object.keys(observed).length > 0) {
      // Only meaningful when the whole file ran, not under `-t`.
      console.warn(`golden entries not exercised: ${stale.join(", ")}`);
   }
});

/** Compare one scenario's payload with its golden entry (or record it). */
function pin(name: string, payload: unknown) {
   observed[name] = payload;
   if (UPDATE) return;
   expect(golden, "golden not loaded").toBeDefined();
   expect(
      name in (golden as object),
      `no golden entry for "${name}"; run with UPDATE_GOLDEN=1`,
   ).toBe(true);
   const expected = (golden as Record<string, unknown>)[name];
   expect(payload).toEqual(expected);
   // toEqual ignores key order; the wire does not.
   expect(JSON.stringify(payload)).toBe(JSON.stringify(expected));
}

// ---------------------------------------------------------------------------
// Handler plumbing (same capture trick as get_context_tool.spec.ts)
// ---------------------------------------------------------------------------

type Content = Array<{ type?: string; resource?: { text: string } }>;
type Handler = (params: Record<string, unknown>) => Promise<{
   isError?: boolean;
   content: Content;
}>;

function captureHandler(store: Partial<EnvironmentStore>): Handler {
   let handler: Handler | undefined;
   const fakeServer = {
      tool: (name: string, _d: string, _s: unknown, h: Handler) => {
         if (name === "get_context") handler = h;
      },
   };
   registerGetContextTool(fakeServer as never, store as EnvironmentStore);
   if (!handler) throw new Error("get_context was not registered");
   return handler;
}

async function call(handler: Handler, params: Record<string, unknown>) {
   const result = await handler(params);
   return JSON.parse(result.content[0].resource!.text);
}

const envWith = (
   pkg: unknown,
   stale: Map<string, { message: string; failedAt: string }> = new Map(),
) =>
   ({
      getPackage: async () => pkg,
      getStaleCompileErrors: () => stale,
   }) as never;

// ---------------------------------------------------------------------------
// Fixture packages
// ---------------------------------------------------------------------------

const field = (kind: string, name: string, doc?: string) => ({
   kind,
   name,
   annotations: doc ? [`#(doc) ${doc}`] : [],
});

const ORDERS_MODEL = {
   getSourceInfos: () => [
      {
         name: "orders",
         annotations: ["#(doc) One row per customer order."],
         schema: {
            fields: [
               field("view", "by_month", "Orders per month."),
               field("dimension", "status", "Order lifecycle status."),
               field("dimension", "state", "State the order ships to."),
               field("dimension", "city"),
               field("measure", "total_revenue", "Sum of order revenue."),
               field("measure", "order_count"),
               field("join", "customer", "The customer who placed it."),
            ],
         },
      },
   ],
   getQueries: () => [
      {
         name: "top_orders",
         sourceName: "orders",
         annotations: ["#(doc) The largest orders."],
      },
   ],
};

const CUSTOMERS_MODEL = {
   getSourceInfos: () => [
      {
         name: "customers",
         annotations: ["#(doc) One row per customer."],
         schema: {
            fields: [
               field("dimension", "state", "State the customer lives in."),
               field("dimension", "region"),
               field("measure", "customer_count", "Distinct customers."),
            ],
         },
      },
   ],
   getQueries: () => [],
};

/** One source with 12 metric_* dimensions: more than the per-source cap. */
const WIDE_MODEL = {
   getSourceInfos: () => [
      {
         name: "telemetry",
         annotations: ["#(doc) Device readings."],
         schema: {
            fields: Array.from({ length: 12 }, (_, i) =>
               field(
                  "dimension",
                  `metric_${String(i).padStart(2, "0")}`,
                  "A metric reading.",
               ),
            ),
         },
      },
   ],
   getQueries: () => [],
};

const packageOf = (models: Record<string, unknown>) => ({
   listModels: async () => Object.keys(models).map((p) => ({ path: p })),
   getModel: (p: string) => models[p],
});

// A fresh object per package, since the entity index is cached per object.
const shop = () =>
   packageOf({
      "orders.malloy": ORDERS_MODEL,
      "customers.malloy": CUSTOMERS_MODEL,
   });
const wide = () => packageOf({ "wide.malloy": WIDE_MODEL });
const empty = () =>
   packageOf({
      "empty.malloy": { getSourceInfos: () => [], getQueries: () => [] },
   });
/** More entities than the semantic index will embed. */
const huge = () =>
   packageOf({
      "huge.malloy": {
         getSourceInfos: () =>
            Array.from({ length: 5_001 }, (_, i) => ({
               name: `s${i}`,
               annotations: [],
               schema: { fields: [] },
            })),
         getQueries: () => [],
      },
   });

/** A stale-compile map for the package of that name. */
const staleFor = (packageName: string) =>
   new Map([
      [
         packageName,
         {
            message: "line 3: missing ')'",
            failedAt: "2026-08-13T00:00:00.000Z",
         },
      ],
   ]);
const STALE = staleFor("pkg");

const scope = (extra: Record<string, unknown> = {}) => ({
   scopes: [{ environment: "pin", package: "pkg", ...extra }],
});
const target = (target_type: string, search_text?: string) => ({
   target_type,
   ...(search_text === undefined ? {} : { search_text }),
});

// ---------------------------------------------------------------------------
// No embedding key configured (the lexical-only server)
// ---------------------------------------------------------------------------

describe("get_context payload pin: no embedding key configured", () => {
   beforeEach(() => {
      _setEmbeddingProviderForTests(null);
      _resetEmbeddingIndexStateForTests();
   });

   const run = (
      name: string,
      params: Record<string, unknown>,
      options: { pkg?: unknown; stale?: typeof STALE } = {},
   ) =>
      it(name, async () => {
         const handler = captureHandler({
            getEnvironment: async () =>
               envWith(options.pkg ?? shop(), options.stale),
         });
         const payload = await call(handler, { ...scope(), ...params });
         // The lexical-only server never advertises a retrieval mode.
         expect("retrieval" in payload).toBe(false);
         expect("retrieval_reason" in payload).toBe(false);
         pin(`unconfigured/${name}`, payload);
      });

   run("listing: every source", { search_targets: [target("source")] });
   run("listing: first page with next_offset", {
      search_targets: [target("source")],
      limit: 1,
   });
   run("listing: second page via offset", {
      search_targets: [target("source")],
      limit: 1,
      offset: 1,
   });
   run("listing: offset beyond the results", {
      search_targets: [target("source")],
      offset: 50,
   });
   run("listing: drill-down into one source", {
      search_targets: ["source", "view", "dimension", "measure", "join"].map(
         (t) => target(t),
      ),
      ...scope({ source: "orders" }),
   });
   run("listing: non-source kinds, capped by limit", {
      search_targets: [target("dimension")],
      limit: 2,
   });
   run("listing: include_code", {
      search_targets: [target("measure")],
      include_code: true,
   });
   run(
      "listing: empty package",
      { search_targets: [target("source")] },
      {
         pkg: empty(),
      },
   );
   run(
      "listing: stale package",
      { search_targets: [target("source")] },
      {
         stale: STALE,
      },
   );
   run("listing: unsupported target type", {
      search_targets: [target("source"), target("dimensional_value", "CA")],
   });
   run("lexical: single target", {
      search_targets: [target("dimension", "state")],
   });
   run("lexical: multiple targets of different kinds", {
      search_targets: [
         target("measure", "revenue"),
         target("dimension", "state"),
         target("source", "customer"),
      ],
   });
   run("lexical: scoped to a source", {
      search_targets: [target("dimension", "state")],
      ...scope({ source: "customers" }),
   });
   run("lexical: scoped to a model path", {
      search_targets: [target("dimension", "state")],
      ...scope({ model_path: "customers.malloy" }),
   });
   run("lexical: scoped to an entity name", {
      search_targets: [target("dimension", "state")],
      ...scope({ entity_name: "state" }),
   });
   run("lexical: no match", {
      search_targets: [target("dimension", "zzzunmatched")],
   });
   run("lexical: source cut warning", {
      search_targets: [target("dimension", "state")],
      limit: 1,
   });
   run(
      "lexical: per-source entity cut warning",
      { search_targets: [target("dimension", "metric")] },
      { pkg: wide() },
   );
   run(
      "lexical: stale package",
      {
         search_targets: [target("dimension", "state")],
      },
      { stale: STALE },
   );
   run("lexical: unsupported target warning", {
      search_targets: [
         target("dimension", "state"),
         target("dimensional_value", "California"),
      ],
   });
   run(
      "lexical: stale, unsupported and source cut together",
      {
         search_targets: [
            target("dimension", "state"),
            target("dimensional_value", "California"),
         ],
         limit: 1,
      },
      { stale: STALE },
   );
});

// ---------------------------------------------------------------------------
// Embedding key configured (semantic, indexing, and every way it errors)
// ---------------------------------------------------------------------------

describe("get_context payload pin: embedding configured", () => {
   let tempDir: string;
   let db: DuckDBConnection;

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "get-context-pin-"));
      db = new DuckDBConnection(path.join(tempDir, "pin.db"));
      await db.initialize();
      await createEntityEmbeddingsTable(db);
   });

   afterAll(async () => {
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   beforeEach(() => {
      _setEmbeddingProviderForTests(null);
      _resetEmbeddingIndexStateForTests();
   });
   afterEach(() => {
      _setEmbeddingProviderForTests(null);
   });

   const BUCKETS = 64;
   /**
    * A deterministic bag-of-words embedding: each word adds weight to one of
    * 64 hash buckets and the vector is normalised. Texts that share words are
    * close, texts that share none are (almost) orthogonal, so scores are
    * stable without a hand-written text -> vector table.
    */
   function embed(text: string): number[] {
      const v = new Array<number>(BUCKETS).fill(0);
      for (const word of text.toLowerCase().match(/[a-z0-9]+/g) ?? []) {
         let h = 2166136261;
         for (const ch of word) h = Math.imul(h ^ ch.charCodeAt(0), 16777619);
         v[(h >>> 0) % BUCKETS] += 1;
      }
      const norm = Math.sqrt(v.reduce((s, x) => s + x * x, 0)) || 1;
      return v.map((x) => x / norm);
   }

   function stubProvider(
      fail = false,
      // Every request waits for this before it answers, so a test can read
      // the response to a question while the index is provably still empty.
      hold?: Promise<void>,
   ): EmbeddingProvider {
      const fetchStub = (async (
         _url: RequestInfo | URL,
         init?: RequestInit,
      ) => {
         if (hold) await hold;
         if (fail) return new Response("down", { status: 500 });
         const body = JSON.parse(String(init?.body)) as { input: string[] };
         const data = body.input.map((text, index) => ({
            index,
            embedding: embed(text),
         }));
         return new Response(JSON.stringify({ data }), { status: 200 });
      }) as typeof fetch;
      return new EmbeddingProvider(
         {
            apiKey: "test",
            model: "stub-model",
            baseUrl: "https://stub.example.com/v1",
            minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
         },
         fetchStub,
      );
   }

   const storeFor = (
      pkg: unknown,
      stale?: typeof STALE,
   ): Partial<EnvironmentStore> => ({
      getEnvironment: async () => envWith(pkg, stale),
      storageManager: { getDuckDbConnection: () => db } as never,
   });

   /** Poll until the background vector sync lands; fail loudly if it never does. */
   async function untilSemantic(
      handler: Handler,
      params: Record<string, unknown>,
   ) {
      for (let i = 0; i < 1_000; i++) {
         const payload = await call(handler, params);
         if (payload.retrieval === "semantic") return payload;
         await new Promise((resolve) => setTimeout(resolve, 5));
      }
      throw new Error("retrieval never became semantic");
   }

   /** Run semantic scenarios against a fresh index per scenario name. */
   const semantic = (
      name: string,
      params: Record<string, unknown>,
      options: { pkg?: () => unknown; stale?: boolean } = {},
   ) =>
      it(`semantic ready: ${name}`, async () => {
         _setEmbeddingProviderForTests(stubProvider());
         const handler = captureHandler(
            storeFor(
               (options.pkg ?? shop)(),
               options.stale ? staleFor(`sem-${name}`) : undefined,
            ),
         );
         const full = {
            ...params,
            scopes: [
               {
                  environment: "pin",
                  package: `sem-${name}`,
                  ...(
                     params.scopes as Record<string, unknown>[] | undefined
                  )?.[0],
               },
            ],
         };
         const payload = await untilSemantic(handler, full);
         pin(`semantic/${name}`, payload);
      });

   // Semantic queries here name the doc text, not just "state". Two sources
   // declare a `state` dimension whose names embed identically, and the scan
   // orders an exact score tie in a way DuckDB does not promise, which made
   // the golden flaky. Each query below has one unique best match.
   semantic("single target", {
      search_targets: [target("dimension", "state the customer lives in")],
   });
   semantic("multiple targets of different kinds", {
      search_targets: [
         target("measure", "sum of order revenue"),
         target("dimension", "state the order ships to"),
         target("source", "one row per customer"),
      ],
   });
   semantic("scoped to a source", {
      search_targets: [target("dimension", "state the order ships to")],
      scopes: [{ source: "customers" }],
   });
   semantic("scoped to a model path", {
      search_targets: [target("dimension", "state the order ships to")],
      scopes: [{ model_path: "orders.malloy" }],
   });
   semantic("scoped to an entity name", {
      search_targets: [target("dimension", "state the order ships to")],
      scopes: [{ entity_name: "state" }],
   });
   semantic("query with little overlap", {
      search_targets: [target("dimension", "zzqx wwvk")],
   });
   semantic("source cut warning", {
      search_targets: [target("dimension", "state the order ships to")],
      limit: 1,
   });
   semantic(
      "per-source entity cut warning",
      { search_targets: [target("dimension", "a metric reading")] },
      { pkg: wide },
   );
   semantic(
      "stale package",
      { search_targets: [target("dimension", "state the order ships to")] },
      { stale: true },
   );
   semantic("unsupported target warning", {
      search_targets: [
         target("dimension", "state the order ships to"),
         target("dimensional_value", "California"),
      ],
   });
   semantic(
      "empty package",
      { search_targets: [target("dimension", "state the order ships to")] },
      { pkg: empty },
   );

   // -- Cases a configured server cannot rank, one reason at a time ------

   /**
    * Ask while the embedding provider is held, so the answer is read with the
    * index provably empty, then let the sync finish so it cannot hold the
    * process-wide sync queue against the scenarios after this one.
    */
   async function askWhileIndexing(
      handler: Handler,
      params: Record<string, unknown>,
   ) {
      let release!: () => void;
      const hold = new Promise<void>((resolve) => {
         release = resolve;
      });
      _setEmbeddingProviderForTests(stubProvider(false, hold));
      try {
         return await call(handler, params);
      } finally {
         release();
         await embeddingSyncQueue.idle();
      }
   }

   it("fallback: indexing (first call, sync not done)", async () => {
      const handler = captureHandler(storeFor(shop()));
      const payload = await askWhileIndexing(handler, {
         search_targets: [target("dimension", "state the order ships to")],
         scopes: [{ environment: "pin", package: "fb-indexing" }],
      });
      expect(payload.retrieval).toBe("indexing");
      expect(payload.retrieval_progress.embedded).toBe(0);
      pin("fallback/indexing", payload);
   });

   it("fallback: indexing with a stale note", async () => {
      const handler = captureHandler(storeFor(shop(), staleFor("fb-stale")));
      const payload = await askWhileIndexing(handler, {
         search_targets: [target("dimension", "state the order ships to")],
         scopes: [{ environment: "pin", package: "fb-stale" }],
      });
      expect(payload.retrieval).toBe("indexing");
      pin("fallback/indexing with stale note", payload);
   });

   it("fallback: provider-error, then cooldown", async () => {
      const handler = captureHandler(storeFor(shop()));
      const params = {
         search_targets: [target("dimension", "state the order ships to")],
         scopes: [{ environment: "pin", package: "fb-provider" }],
      };
      _setEmbeddingProviderForTests(stubProvider());
      await untilSemantic(handler, params);

      _setEmbeddingProviderForTests(stubProvider(true));
      // Both errors name the time the next try is allowed, which moves with
      // the clock. Check it is a time still ahead, then pin the rest.
      const withoutRetryTime = (payload: unknown) => {
         const text = JSON.stringify(payload);
         const retryAt = text.match(/\d{4}-\d{2}-\d{2}T[\d:.]+Z/)?.[0];
         expect(retryAt).toBeDefined();
         expect(Date.parse(retryAt as string)).toBeGreaterThan(Date.now());
         return JSON.parse(text.replace(retryAt as string, "<retry-at>"));
      };

      const failed = await call(handler, params);
      expect(failed.retrieval_reason).toBe("provider-error");
      pin("fallback/provider-error", withoutRetryTime(failed));

      const cooled = await call(handler, params);
      expect(cooled.retrieval_reason).toBe("cooldown");
      pin("fallback/cooldown", withoutRetryTime(cooled));
   });

   it("fallback: too-many-entities", async () => {
      _setEmbeddingProviderForTests(stubProvider());
      const handler = captureHandler(storeFor(huge()));
      const payload = await call(handler, {
         search_targets: [target("source", "s42")],
         limit: 3,
         scopes: [{ environment: "pin", package: "fb-huge" }],
      });
      expect(payload.retrieval_reason).toBe("too-many-entities");
      pin("fallback/too-many-entities", payload);
   });

   it("fallback: unavailable (malformed embedding configuration)", async () => {
      const saved = {
         key: process.env.EMBEDDING_API_KEY,
         base: process.env.EMBEDDING_API_BASE,
      };
      process.env.EMBEDDING_API_KEY = "k";
      process.env.EMBEDDING_API_BASE = "not a url";
      _clearEmbeddingProviderForTests();
      try {
         const handler = captureHandler(storeFor(shop()));
         const payload = await call(handler, {
            search_targets: [target("dimension", "state the order ships to")],
            scopes: [{ environment: "pin", package: "fb-badcfg" }],
         });
         expect(payload.retrieval_reason).toBe("unavailable");
         pin("fallback/unavailable malformed config", payload);
      } finally {
         if (saved.key === undefined) delete process.env.EMBEDDING_API_KEY;
         else process.env.EMBEDDING_API_KEY = saved.key;
         if (saved.base === undefined) delete process.env.EMBEDDING_API_BASE;
         else process.env.EMBEDDING_API_BASE = saved.base;
         _clearEmbeddingProviderForTests();
         _setEmbeddingProviderForTests(null);
      }
   });

   it("fallback: unavailable (no storage handle)", async () => {
      _setEmbeddingProviderForTests(stubProvider());
      const store = storeFor(shop());
      delete (store as { storageManager?: unknown }).storageManager;
      const payload = await call(captureHandler(store), {
         search_targets: [target("dimension", "state the order ships to")],
         scopes: [{ environment: "pin", package: "fb-nostorage" }],
      });
      expect(payload.retrieval_reason).toBe("unavailable");
      pin("fallback/unavailable no storage", payload);
   });

   it("configured listing carries no retrieval marker", async () => {
      // Listing never ranks, so it never reports a retrieval mode.
      _setEmbeddingProviderForTests(stubProvider());
      const payload = await call(captureHandler(storeFor(shop())), {
         search_targets: [target("source")],
         scopes: [{ environment: "pin", package: "configured-listing" }],
      });
      expect("retrieval" in payload).toBe(false);
      pin("configured/listing", payload);
   });
});
