// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Dotted rows (joined fields the index keeps because assembly cannot rebuild
 * them) get their own per-source window, so they cannot take the slots of the
 * source's own fields. On a package that joins inline tables one source can
 * have hundreds of them scoring above its own fields (on one benchmark
 * package, 625 extra scored rows per target pushed labelled fields from rank 4
 * to 10 down to rank 13 to 33).
 */

import {
   afterAll,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createEntityEmbeddingsTable } from "../../storage/duckdb/schema";
import { EmbeddingProvider } from "../../service/embedding_provider";
import type { Package } from "../../service/package";
import {
   DEFAULT_EMBEDDING_MIN_SIMILARITY,
   _resetEmbeddingIndexStateForTests,
   trySemanticSearch,
   type EmbeddableEntity,
   type SemanticSearchResult,
} from "./embedding_index";
import { assembleCards } from "./get_context_assembly";
import type { PipelineContext, PipelineSettings } from "./get_context_pipeline";
import {
   MAX_JOINED_ROWS_PER_SOURCE_TARGET,
   type ResultEntity,
} from "./get_context_tool";

// One source with four direct dimensions and four dotted ones. The dotted ones
// score HIGHER than every direct field, the way a date dimension's `year`
// outranks the source's own `order_year_month`.
const QUERY = "find it";
const VECTORS: Record<string, number[]> = {
   [QUERY]: [1, 0, 0],
   "d one": [0.9, 0.436, 0],
   "d two": [0.8, 0.6, 0],
   "d three": [0.7, 0.714, 0],
   "d four": [0.6, 0.8, 0],
   "j a": [1, 0, 0],
   "j b": [0.95, 0.312, 0],
   "j c": [0.92, 0.392, 0],
   "j d": [0.91, 0.415, 0],
};
const ENTITIES: EmbeddableEntity[] = [
   "d_one",
   "d_two",
   "d_three",
   "d_four",
   "j.a",
   "j.b",
   "j.c",
   "j.d",
].map((name) => ({
   kind: "dimension",
   name,
   source: "src",
   modelPath: "m.malloy",
   embedDoc: "",
}));

let tempDir: string;
let db: DuckDBConnection;

beforeAll(async () => {
   tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "joined-window-spec-"));
   db = new DuckDBConnection(path.join(tempDir, "test.db"));
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

const FACETS = {
   representation: "facets",
   keyphrases: "never",
   prompts: {},
} as const;

function provider(): EmbeddingProvider {
   const fetchStub = (async (_url: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      const data = body.input.map((text, index) => {
         const embedding = VECTORS[text];
         if (!embedding) throw new Error(`no stub vector for "${text}"`);
         return { index, embedding };
      });
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

async function scan(
   window: number,
   joinedWindow?: number,
): Promise<SemanticSearchResult> {
   const args = {
      db,
      provider: provider(),
      pkg: { getRetrievalSettings: () => FACETS } as unknown as Package,
      environmentName: "env",
      packageName: "pkg",
      entities: ENTITIES,
      queries: [{ targetIndex: 0, text: QUERY, kinds: ["dimension"] }],
      perSourceWindow: window,
      ...(joinedWindow === undefined
         ? {}
         : { perSourceJoinedWindow: joinedWindow }),
   };
   for (let i = 0; i < 200; i++) {
      const result = await trySemanticSearch(args);
      if (!("unavailable" in result) || result.unavailable !== "indexing") {
         return result;
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
   }
   throw new Error("sync never completed");
}

function names(result: SemanticSearchResult): string[] {
   if (!("hits" in result)) throw new Error("expected hits");
   return result.hits.map((h) => h.name);
}

describe("the scan's own window for dotted rows", () => {
   it("without one, the dotted rows share the window and take every slot", async () => {
      expect(names(await scan(3))).toEqual(["j.a", "j.b", "j.c"]);
   });

   it("with one, the source's own fields keep their full window", async () => {
      const kept = names(await scan(3, 2));
      expect(kept.sort()).toEqual(
         ["d_one", "d_three", "d_two", "j.a", "j.b"].sort(),
      );
   });

   it("a window of 0 keeps no dotted row and every own field in the window", async () => {
      expect(names(await scan(3, 0)).sort()).toEqual(
         ["d_one", "d_three", "d_two"].sort(),
      );
   });

   it("refuses a window that is not a whole number instead of using it", async () => {
      // The search wraps a failure into an unavailable result (and logs the
      // message above): no hit is served from a bad window.
      for (const bad of [2.5, -1]) {
         const result = await scan(3, bad);
         expect("hits" in result).toBe(false);
      }
   });
});

describe("assembly's card cap leaves room for the dotted rows", () => {
   const rows = (n: number): ResultEntity[] =>
      Array.from({ length: n }, (_, i) => ({
         kind: "dimension",
         name: `f${i}`,
         source: "src",
         environmentName: "env",
         packageName: "pkg",
         modelPath: "m.malloy",
         doc: "",
         score: 0.9 - i * 0.01,
         targetScores: new Map([[0, 0.9 - i * 0.01]]),
         bestTarget: 0,
      }));

   const cardRows = (settings: PipelineSettings): number => {
      const ctx = {
         request: { searches: [] },
         pkgIndex: {},
         settings,
      } as unknown as PipelineContext;
      const state = assembleCards(
         { rows: rows(20), retrieval: "semantic", belowCutoffCount: 0 },
         ctx,
      );
      return state.cards[0].rows.length;
   };

   const base = {
      joins: "index",
      entityWindow: { perSourcePerTarget: 10 },
      joinMaxDepth: 10,
      joinDamping: 0.9,
      scoring: "cosine",
      maxChars: null,
      reserveChars: 1_000,
   } as PipelineSettings;

   it("holds 10 rows per target without a joined window and 13 with one of 3", () => {
      expect(cardRows(base)).toBe(10);
      expect(
         cardRows({
            ...base,
            entityWindow: {
               perSourcePerTarget: 10,
               joinedPerSourcePerTarget: 3,
            },
         }),
      ).toBe(13);
   });

   it("the server's setting is 3", () => {
      expect(MAX_JOINED_ROWS_PER_SOURCE_TARGET).toBe(3);
   });
});
