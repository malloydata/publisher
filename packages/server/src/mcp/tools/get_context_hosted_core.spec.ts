// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * get_context with an embedding provider, end to end, over a package the real
 * compiler produced (see get_context_join_fixture): the semantic index holds
 * direct fields only, and the joined copies appear at assembly with damped
 * scores. The provider is a deterministic stand-in; the DuckDB scan, the
 * topology and the assembly are the real ones.
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
   EmbeddingProvider,
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createEntityEmbeddingsTable } from "../../storage/duckdb/schema";
import {
   compileJoinFixture,
   storeServing,
} from "../../test_helpers/get_context_join_fixture";
import { _resetEmbeddingIndexStateForTests } from "./embedding_index";
import { embeddingSyncQueue } from "./embedding_sync_queue";
import { getPackageIndex, registerGetContextTool } from "./get_context_tool";

type Handler = (params: Record<string, unknown>) => Promise<{
   content: Array<{ resource?: { text: string } }>;
}>;

interface WireEntity {
   name: string;
   entity_type: string;
   relevance?: number;
   join_path?: string;
   relationship?: string;
   matched_targets?: Array<{ search_text: string; relevance: number }>;
}
interface WireCard {
   source_info: { resource_id: { source: string } };
   relevance?: number;
   entities?: WireEntity[];
}

let tempDir: string;
let db: DuckDBConnection;

beforeAll(async () => {
   tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "get-context-hosted-"));
   db = new DuckDBConnection(path.join(tempDir, "hosted.db"));
   await db.initialize();
   await createEntityEmbeddingsTable(db);
});

afterAll(async () => {
   _clearEmbeddingProviderForTests();
   await db.close();
   fs.rmSync(tempDir, { recursive: true, force: true });
});

beforeEach(() => {
   _setEmbeddingProviderForTests(stubProvider());
   _resetEmbeddingIndexStateForTests();
});
afterEach(() => {
   _setEmbeddingProviderForTests(null);
});

const BUCKETS = 64;
/** Bag-of-words hash embedding: texts that share words are close. */
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

function stubProvider(): EmbeddingProvider {
   const fetchStub = (async (_url: RequestInfo | URL, init?: RequestInit) => {
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

/** A handler over a freshly compiled fixture package, and that package. */
async function freshHandler(packageName: string) {
   const { pkg } = await compileJoinFixture();
   const store = storeServing(pkg, {
      storageManager: { getDuckDbConnection: () => db },
   });
   let handler: Handler | undefined;
   registerGetContextTool(
      {
         tool: (name: string, _d: string, _s: unknown, h: Handler) => {
            if (name === "get_context") handler = h;
         },
      } as never,
      store,
   );
   if (!handler) throw new Error("get_context was not registered");
   const call = async (
      params: Record<string, unknown>,
      scope: Record<string, unknown> = {},
   ) => {
      const result = await (handler as Handler)({
         ...params,
         scopes: [{ environment: "env", package: packageName, ...scope }],
      });
      return JSON.parse(result.content[0].resource?.text as string);
   };
   const untilSemantic = async (
      params: Record<string, unknown>,
      scope: Record<string, unknown> = {},
   ) => {
      for (let i = 0; i < 1_000; i++) {
         const payload = await call(params, scope);
         if (payload.retrieval === "semantic") return payload;
         await new Promise((resolve) => setTimeout(resolve, 5));
      }
      throw new Error("retrieval never became semantic");
   };
   return { pkg, store, call, untilSemantic };
}

const cardOf = (payload: { sources: WireCard[] }, source: string) =>
   payload.sources.find((c) => c.source_info.resource_id.source === source);

const entityOf = (card: WireCard | undefined, name: string) =>
   card?.entities?.find((e) => e.name === name);

describe("get_context with joins made at assembly", () => {
   it("embeds the direct fields only, fewer rows than the joined copies would add", async () => {
      const { pkg, untilSemantic } = await freshHandler("count");
      await untilSemantic({
         search_targets: [{ target_type: "dimension", search_text: "name" }],
      });
      const index = await getPackageIndex(storeServing(pkg), "env", "count");
      const embeddedKeys = (
         await db.all<{ entity_name: string }>(
            `SELECT DISTINCT entity_kind, entity_source, entity_name
               FROM entity_embeddings
              WHERE environment_name = 'env' AND package_name = 'count'`,
         )
      ).length;
      const direct = index.directEntities.length;
      const withCopies = index.retrievalEntities.length;
      expect(embeddedKeys).toBe(direct);
      expect(embeddedKeys).toBeLessThan(withCopies);
      // None of the embedded rows is a joined copy.
      const dotted = await db.all(
         `SELECT 1 FROM entity_embeddings
           WHERE package_name = 'count' AND entity_name LIKE '%.%'`,
      );
      expect(dotted).toEqual([]);
      // The numbers this fixture embeds, before and after.
      expect({ before: withCopies, after: embeddedKeys }).toEqual({
         before: 73,
         after: 29,
      });
   });

   it("returns a card for every root that reaches the field, damped per join", async () => {
      const { untilSemantic } = await freshHandler("roots");
      const payload = await untilSemantic({
         search_targets: [
            {
               target_type: "dimension",
               search_text: "name of the customer",
            },
         ],
      });
      const direct = entityOf(cardOf(payload, "cust"), "name");
      expect(direct?.relevance).toBeGreaterThan(0.5);
      const near = (actual: number | undefined, expected: number) =>
         expect(Math.abs((actual as number) - expected)).toBeLessThan(2e-4);

      // One join: 0.81 of the direct score. Both roots that join cust once.
      for (const [root, name] of [
         ["inv", "customer.name"],
         ["reg", "c.name"],
         ["ord", "buyer.name"],
         ["ord", "seller.name"],
      ] as const) {
         const copy = entityOf(cardOf(payload, root), name);
         expect(copy?.join_path).toBe(name.replace(/\.name$/, ""));
         near(copy?.relevance, (direct?.relevance as number) * 0.81);
      }
      // Two joins: 0.729.
      const two = entityOf(cardOf(payload, "ord"), "region_r.c.name");
      near(two?.relevance, (direct?.relevance as number) * 0.729);
      // A source ranks by its best field, direct or joined.
      expect(cardOf(payload, "ord")?.relevance).toBe(
         entityOf(cardOf(payload, "ord"), "buyer.name")?.relevance,
      );
   });

   it("reaches three joins deep, which the index never copied", async () => {
      const { untilSemantic } = await freshHandler("deep");
      const payload = await untilSemantic({
         search_targets: [
            {
               target_type: "dimension",
               search_text: "iso code of the country",
            },
         ],
      });
      const direct = entityOf(cardOf(payload, "country"), "country_code");
      const three = entityOf(
         cardOf(payload, "ord"),
         "region_r.c.origin.country_code",
      );
      expect(three?.join_path).toBe("region_r.c.origin");
      expect(
         Math.abs(
            (three?.relevance as number) -
               (direct?.relevance as number) * 0.6561,
         ),
      ).toBeLessThan(2e-4);
   });

   it("reports damped per-target scores that agree with the entity's relevance", async () => {
      const { untilSemantic } = await freshHandler("targets");
      const payload = await untilSemantic({
         search_targets: [
            {
               target_type: "dimension",
               search_text: "name of the customer",
            },
         ],
      });
      const copy = entityOf(cardOf(payload, "inv"), "customer.name");
      expect(copy?.matched_targets).toEqual([
         {
            search_text: "name of the customer",
            relevance: copy?.relevance as number,
         },
      ]);
   });

   it("answers a source drill-down from the fields that source reaches", async () => {
      const { untilSemantic } = await freshHandler("scoped");
      const payload = await untilSemantic(
         {
            search_targets: [
               {
                  target_type: "dimension",
                  search_text: "name of the customer",
               },
            ],
         },
         { source: "inv" },
      );
      // cust is out of scope, but inv reaches its `name`: the card is inv's,
      // and cust has none.
      expect(
         payload.sources.map((c: WireCard) => c.source_info.resource_id.source),
      ).toEqual(["inv"]);
      expect(entityOf(cardOf(payload, "inv"), "customer.name")).toBeDefined();
   });

   it("applies a model path to the root the copy lands in", async () => {
      const { untilSemantic } = await freshHandler("pathed");
      const payload = await untilSemantic(
         {
            search_targets: [
               {
                  target_type: "dimension",
                  search_text: "name of the customer",
               },
            ],
         },
         { model_path: "nowhere.malloy" },
      );
      expect(payload.sources).toEqual([]);
   });
});

afterAll(async () => {
   // The sync queue is process-wide; let it drain so a later spec starts clean.
   await embeddingSyncQueue.idle();
});
