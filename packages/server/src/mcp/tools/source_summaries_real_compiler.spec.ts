// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Summaries are per (model file, source), and the sync must see the same
 * entity list a request does. This runs the real sync over the entities the real
 * compiler produces for two files that each define `cust`, where the second
 * file's `orders` joins its own `cust`. The sync used to be handed a list
 * deduplicated by (kind, source, name), so the second file's `cust` was never
 * summarized, its `orders` was summarized from a prompt where the join did not
 * resolve, and a request then rejected the stored row because its hash was
 * computed over the full list.
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
import { setRetrievalConfig } from "../../retrieval_config";
import { EmbeddingProvider } from "../../service/embedding_provider";
import type { Package } from "../../service/package";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import {
   createEntityEmbeddingsTable,
   createEntityKeyphrasesTable,
   createSourceSummariesTable,
} from "../../storage/duckdb/schema";
import {
   compileModelFiles,
   storeServing,
} from "../../test_helpers/get_context_join_fixture";
import {
   scriptedChat,
   summaryReply,
} from "../../test_helpers/get_context_llm_harness";
import {
   KEY_SEPARATOR,
   _resetEmbeddingIndexStateForTests,
   getEmbeddingIndexStatus,
   trySemanticSearch,
} from "./embedding_index";
import { embeddingSyncQueue } from "./embedding_sync_queue";
import { getPackageIndex } from "./get_context_tool";
import { indexSettingsOf } from "./index_settings";
import {
   currentSummaryHashes,
   loadSourceSummaries,
   summaryKey,
} from "./source_summaries";

const FILES = {
   "a.malloy": `
source: cust is duckdb.sql("select 1 as id, 'x' as region") extend {
  #(doc) Customers as the first file sees them.
  dimension: a_region is region
}
`,
   "b.malloy": `
source: cust is duckdb.sql("select 1 as id, 'gold' as tier") extend {
  #(doc) Customers as the second file sees them.
  dimension: b_tier is tier
}

source: orders is duckdb.sql("select 1 as id, 1 as cust_id") extend {
  #(doc) One row per order.
  dimension: order_id is id
  join_one: cust on cust.id = cust_id
}
`,
};

let tempDir: string;
let db: DuckDBConnection;

beforeAll(async () => {
   tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "summary-real-spec-"));
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
   await db.run("DELETE FROM entity_embeddings");
   await db.run("DELETE FROM entity_keyphrases");
   await db.run("DELETE FROM source_summaries");
});

afterEach(async () => {
   await embeddingSyncQueue.idle();
   _clearChatModelForTests();
   setRetrievalConfig(undefined);
});

const embedder = () =>
   new EmbeddingProvider(
      {
         apiKey: "k",
         model: "m",
         baseUrl: "https://stub.example.com/v1",
         minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
      },
      (async (_url: RequestInfo | URL, init?: RequestInit) => {
         const body = JSON.parse(String(init?.body)) as { input: string[] };
         return new Response(
            JSON.stringify({
               data: body.input.map((_, index) => ({
                  index,
                  embedding: [1, 0, 1],
               })),
            }),
            { status: 200 },
         );
      }) as typeof fetch,
   );

/** A package for the settings (keyphrases off, so only summaries call the model). */
const settingsPkg = (): Package =>
   ({
      getRetrievalSettings: () => ({
         representation: "single",
         keyphrases: "never",
         prompts: {},
      }),
   }) as unknown as Package;

describe("source summaries through the sync, over the real compiler's entities", () => {
   it("summarizes every source in every file, and the request accepts what the sync stored", async () => {
      const { pkg: compiled } = await compileModelFiles(FILES);
      const index = await getPackageIndex(
         storeServing(compiled),
         "env",
         "sync",
      );
      const chat = scriptedChat(summaryReply);
      _setChatModelForTests(chat.model, { concurrency: 1 });
      const provider = embedder();
      const pkg = settingsPkg();

      // Run the sync the way a search starts it, with the package's own list.
      for (let i = 0; i < 400; i++) {
         const result = await trySemanticSearch({
            db,
            provider,
            pkg,
            environmentName: "env",
            packageName: "sync",
            entities: index.retrievalEntities as never,
            queries: [
               { targetIndex: 0, text: "find it", kinds: ["dimension"] },
            ],
            perSourceWindow: 10,
         });
         if (!("unavailable" in result) || result.unavailable !== "indexing") {
            break;
         }
         await new Promise((resolve) => setTimeout(resolve, 5));
      }

      const asked = chat.prompts.map(
         (p) => /^Source name: (.*)$/m.exec(p)?.[1],
      );
      expect(asked.sort()).toEqual(["cust", "cust", "orders"]);

      // Each file's rows are there under its own key.
      const settings = indexSettingsOf(pkg).sourceSummary!;
      const current = currentSummaryHashes(index.directEntities, settings);
      expect([...current.keys()].sort()).toEqual(
         [
            summaryKey("a.malloy", "cust"),
            summaryKey("b.malloy", "cust"),
            summaryKey("b.malloy", "orders"),
         ].sort(),
      );
      // The request side reads them against the hashes of ITS list. Every row
      // the sync stored must pass that check, or it is dropped on every request.
      const served = await loadSourceSummaries(db, "env", "sync", current);
      expect([...served.keys()].sort()).toEqual([...current.keys()].sort());

      // The second file's `orders` was written from a prompt in which its own
      // `cust` resolved, not from one where the join was left unexpanded.
      const ordersPrompt = chat.prompts.find((p) =>
         p.includes("Source name: orders"),
      );
      expect(ordersPrompt).toContain("b_tier");
      expect(ordersPrompt).not.toContain("a_region");

      // And the status agrees: ready, with every source counted and done.
      const status = await getEmbeddingIndexStatus(
         db,
         provider,
         "env",
         "sync",
         index.retrievalEntities as never,
         pkg,
      );
      expect(status.status).toBe("ready");
      expect(status.sourceSummaryProgress).toEqual({
         done: 3,
         total: 3,
         capped: false,
      });
      expect(KEY_SEPARATOR).toBeDefined();
   });
});
