// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Pins the COMPLETE get_context response for a listing and for a ranked
 * answer, so a refactor of how a request is answered cannot change a byte of
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
 * Determinism: the stub embedding provider is deterministic, entity ids come
 * from names, and nothing time-based reaches the payload, so no field needs
 * normalising. The one thing that is timed is the background index build,
 * which these tests wait for by polling (see untilSemantic) rather than by
 * sleeping. No chat model is configured, so a ranked answer is the engine's
 * own, unjudged.
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
import * as path from "path";
import { registerGetContextTool } from "./get_context_tool";
import { setRetrievalConfig } from "../../retrieval_config";
import {
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";
import type { EnvironmentStore } from "../../service/environment_store";
import { _setFcjValueFetcherForTests } from "../context/engine_state";
import { compileFcjFixture } from "../../test_helpers/fcj_fixture";
import { fcjValueFetcher } from "../../test_helpers/fcj_index_fixture";
import {
   callPayload,
   semanticHarness,
   untilSemantic,
   type SemanticHarness,
} from "../../test_helpers/get_context_llm_harness";

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

const packageOf = (models: Record<string, unknown>) => ({
   listSkills: () => [],
   listModels: async () => Object.keys(models).map((p) => ({ path: p })),
   getModel: (p: string) => models[p],
});

// A fresh object per package, since the entity index is cached per object.
const shop = () =>
   packageOf({
      "orders.malloy": ORDERS_MODEL,
      "customers.malloy": CUSTOMERS_MODEL,
   });
const empty = () =>
   packageOf({
      "empty.malloy": { getSourceInfos: () => [], getQueries: () => [] },
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

describe("get_context payload pin: listings", () => {
   // Value search off: these pins are what a server with it turned off answers
   // to a dimensional_value target. The on-mode pins are further down.
   beforeEach(() => {
      _setEmbeddingProviderForTests(null);
      setRetrievalConfig({ dimensionValues: { enabled: false } });
   });
   afterEach(() => setRetrievalConfig(undefined));

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
         // A listing never ranks, so it never reports a retrieval mode.
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
});

// ---------------------------------------------------------------------------
// A configured server: a listing still carries no retrieval marker
// ---------------------------------------------------------------------------

describe("get_context payload pin: a listing on a server with embeddings", () => {
   beforeEach(() => {
      setRetrievalConfig({ dimensionValues: { enabled: false } });
   });
   afterEach(() => {
      _setEmbeddingProviderForTests(null);
      setRetrievalConfig(undefined);
   });

   it("configured listing carries no retrieval marker", async () => {
      const harness = await semanticHarness();
      try {
         harness.reset();
         const { payload } = await callPayload(harness.handlerFor(shop()), {
            search_targets: [target("source")],
            scopes: [{ environment: "pin", package: "configured-listing" }],
         });
         expect("retrieval" in payload).toBe(false);
         pin("configured/listing", payload);
      } finally {
         await harness.close();
      }
   });
});

// ---------------------------------------------------------------------------
// A ranked answer from the engine
// ---------------------------------------------------------------------------

describe("get_context payload pin: ranked answers", () => {
   let harness: SemanticHarness;
   beforeAll(async () => {
      harness = await semanticHarness();
      _setFcjValueFetcherForTests(fcjValueFetcher);
   });
   afterAll(async () => {
      _setFcjValueFetcherForTests(undefined);
      await harness.close();
   });
   beforeEach(() => harness.reset());
   afterEach(() => _setEmbeddingProviderForTests(null));

   const scopes = [{ environment: "pin", package: "fixture" }];
   const params = {
      search_targets: [
         target("measure", "number of flights"),
         target("dimension", "start date"),
      ],
      scopes,
   };

   it("is indexing on the first call, with no results and a way forward", async () => {
      const { pkg } = await compileFcjFixture();
      const { payload } = await callPayload(harness.handlerFor(pkg), params);
      expect(payload.retrieval).toBe("indexing");
      pin("ranked/indexing", payload);
   });

   it("is the engine's own answer once the index is built", async () => {
      const { pkg } = await compileFcjFixture();
      const { payload } = await untilSemantic(harness.handlerFor(pkg), params);
      expect(payload.retrieval).toBe("semantic");
      expect(payload.engine).toBe("fcj");
      pin("ranked/answer", payload);
   });

   it("scoped to a source, keeps to that source", async () => {
      const { pkg } = await compileFcjFixture();
      const { payload } = await untilSemantic(harness.handlerFor(pkg), {
         search_targets: [target("dimension", "start date")],
         scopes: [{ ...scopes[0], source: "flights" }],
      });
      pin("ranked/scoped to a source", payload);
   });

   it("says an error when the request is refused", async () => {
      const { pkg } = await compileFcjFixture();
      const { payload, isError } = await callPayload(harness.handlerFor(pkg), {
         search_targets: [target("dimension")],
         offset: 3,
         scopes: [{ ...scopes[0], source: "flights" }],
      });
      expect(isError).toBe(true);
      pin(
         "ranked/offset refused on a listing that is not a source browse",
         payload,
      );
   });
});
