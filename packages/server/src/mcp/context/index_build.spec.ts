// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { DEFAULT_FCJ_SETTINGS } from "../../service/package_retrieval";
import { createCatalogTables } from "../../storage/duckdb/schema";
import type { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import {
   compileFcjFixture,
   openTempDb,
   type FcjFixture,
} from "../../test_helpers/fcj_fixture";
import { fcjValueFetcher } from "../../test_helpers/fcj_index_fixture";
import {
   numberedLines,
   scriptedChat,
   stubEmbeddingProvider,
} from "../../test_helpers/get_context_llm_harness";
import { listItems, resolveDocParts, splitDocByRules } from "./doc_parts";
import { buildFcjIndex } from "./index_build";
import { KeywordIndex, stem, terms } from "./keyword";
import { VectorSyncError } from "./vectors";
import { SyncRequestBudget } from "../tools/get_context_llm";

describe("doc parts by rules", () => {
   it("keeps a one-sentence doc whole", () => {
      expect(splitDocByRules("City name.")).toEqual({
         what: "City name.",
         values: [],
         rules: [],
         examples: [],
      });
   });

   it("takes a rule out of what the field is", () => {
      expect(
         splitDocByRules(
            "Order amount in dollars. Exclude test orders when summing.",
         ),
      ).toEqual({
         what: "Order amount in dollars.",
         values: [],
         rules: ["Exclude test orders when summing."],
         examples: [],
      });
   });

   it("reads a value list and an example query", () => {
      expect(
         splitDocByRules(
            "Sales channel of the order. Values: DTC, B2B or Amazon. Example: run: orders -> { group_by: channel }",
         ),
      ).toEqual({
         what: "Sales channel of the order.",
         values: ["DTC", "B2B", "Amazon"],
         rules: [],
         examples: ["Example: run: orders -> { group_by: channel }"],
      });
   });

   it("keeps the first sentence when every sentence is a rule", () => {
      expect(
         splitDocByRules("Never sum this. Always filter by region first."),
      ).toEqual({
         what: "Never sum this.",
         values: [],
         rules: ["Never sum this.", "Always filter by region first."],
         examples: [],
      });
   });

   it("does not read a prose colon as a list", () => {
      expect(
         listItems("Note: the amount is gross of tax and of shipping fees"),
      ).toEqual([]);
      expect(listItems("One of Gold, Silver, Bronze.")).toEqual([
         "Gold",
         "Silver",
         "Bronze",
      ]);
   });
});

describe("doc parts by the model", () => {
   let db: DuckDBConnection;
   let close: () => Promise<void>;
   beforeEach(async () => {
      ({ db, close } = await openTempDb(createCatalogTables));
   });
   afterEach(async () => close());

   const docs = [
      "Order amount in dollars. Exclude test orders when summing.",
      "City name.",
   ];

   it("sends only multi-sentence docs, keeps the reply, and reuses it on the next build", async () => {
      const chat = scriptedChat((prompt) => {
         expect(numberedLines(prompt)).toEqual([
            [1, "Order amount in dollars. Exclude test orders when summing."],
         ]);
         return JSON.stringify({
            results: [
               {
                  index: 1,
                  what: "Order amount in dollars.",
                  values: [],
                  rules: ["Exclude test orders when summing."],
                  examples: [],
               },
            ],
         });
      });
      const llm = {
         db,
         environmentName: "env",
         packageName: "p",
         chat: chat.model,
         instructions: "split",
         concurrency: 2,
         budget: new SyncRequestBudget(10),
      };
      const first = await resolveDocParts({ docs, llm });
      expect(first.longDocs).toBe(1);
      expect(first.byModel).toBe(1);
      expect(first.byRules).toBe(0);
      expect(first.run).toMatchObject({ reused: 0, generated: 1, missing: 0 });
      const second = await resolveDocParts({ docs, llm });
      expect(second.run).toMatchObject({ reused: 1, generated: 0, missing: 0 });
      expect(chat.prompts.length).toBe(1);
   });

   it("falls back to the rules when the model fails, and says why", async () => {
      const chat = scriptedChat(() => {
         throw new Error("vendor down");
      });
      const out = await resolveDocParts({
         docs,
         llm: {
            db,
            environmentName: "env",
            packageName: "p",
            chat: chat.model,
            instructions: "split",
            concurrency: 1,
            budget: new SyncRequestBudget(10),
         },
      });
      expect(out.byModel).toBe(0);
      expect(out.byRules).toBe(1);
      expect(out.run?.failure).toBeDefined();
      expect(out.run?.missing).toBe(1);
   });
});

describe("keyword index", () => {
   it("splits names the way a person reads them and stems lightly", () => {
      expect(terms("custId total_sales orders shipped running cities")).toEqual(
         ["cust", "id", "sale", "order", "ship", "run", "city"],
      );
      expect(stem("fulfilled")).toBe("fulfill");
   });

   it("ranks by BM25 within what `allow` admits", () => {
      const index = new KeywordIndex([
         { key: "a", text: "flight count: number of flights" },
         { key: "b", text: "city name of the city" },
         { key: "c", text: "arrives at: airport the flight lands at" },
      ]);
      const hits = index.search("flights to Denver", () => true);
      expect(hits.map((h) => h.key)).toEqual(["a", "c"]);
      expect(
         index.search("flights", (k) => k !== "a").map((h) => h.key),
      ).toEqual(["c"]);
   });
});

describe("index build", () => {
   let fixture: FcjFixture;
   let db: DuckDBConnection;
   let close: () => Promise<void>;
   beforeEach(async () => {
      fixture = await compileFcjFixture();
      ({ db, close } = await openTempDb(createCatalogTables));
   });
   afterEach(async () => close());

   const build = (llm?: Parameters<typeof buildFcjIndex>[0]["llm"]) =>
      buildFcjIndex({
         pkgIndex: fixture.pkgIndex,
         db,
         provider: stubEmbeddingProvider(),
         environmentName: "env",
         packageName: "fixture",
         settings: DEFAULT_FCJ_SETTINGS,
         ...(llm ? { llm } : {}),
      });

   it("embeds one derived row per node, and reuses every row on a rebuild", async () => {
      const first = await build();
      expect(first.report.vectors).toEqual({
         rows: 51,
         embedded: 51,
         reused: 0,
         deleted: 0,
         charsEmbedded: first.report.vectors.charsEmbedded,
         dims: 64,
      });
      expect(first.report.questions.skipped).toBe("no LLM is configured");
      expect(first.report.docParts).toEqual({
         longDocs: 1,
         byModel: 0,
         byRules: 1,
         skipped: "no LLM is configured: split by rules",
      });
      // The rule in sales' doc is not in its search text.
      expect(first.derived.get("measure:orders@shop.malloy:sales")).toBe(
         "sales: Order amount in dollars. Computed from amount. In orders: One row per order placed in the web shop.",
      );
      expect(
         first.parts.get("measure:orders@shop.malloy:sales")?.rules,
      ).toEqual(["Exclude test orders when summing."]);
      const second = await build();
      expect(second.report.vectors).toMatchObject({
         rows: 51,
         embedded: 0,
         reused: 51,
         deleted: 0,
      });
   });

   it("names the vectors step, its progress and the deadline when embedding fails", async () => {
      const stub = stubEmbeddingProvider();
      const failing = Object.create(stub) as typeof stub;
      failing.embedBatch = async () => {
         throw new Error("Embedding request failed: timed out after 30000ms");
      };
      const err = await buildFcjIndex({
         pkgIndex: fixture.pkgIndex,
         db,
         provider: failing,
         environmentName: "env",
         packageName: "fixture",
         settings: DEFAULT_FCJ_SETTINGS,
      }).then(
         () => undefined,
         (e: Error) => e,
      );
      expect(err).toBeInstanceOf(VectorSyncError);
      expect(err!.message).toBe(
         "The vectors step failed after embedding 0 of 51 new rows (0 reused): " +
            "Embedding request failed: timed out after 30000ms. Each embedding request of up to " +
            "256 rows has a 30000 ms deadline (EMBEDDING_BATCH_TIMEOUT_MS), and a timeout, a 429 " +
            "or a 5xx is retried up to 5 attempts. The rows already embedded are stored, so the " +
            "next build embeds only the rest.",
      );
   });

   describe("value vectors", () => {
      const withValues = (
         settings = DEFAULT_FCJ_SETTINGS,
         provider = stubEmbeddingProvider(),
      ) =>
         buildFcjIndex({
            pkgIndex: fixture.pkgIndex,
            db,
            provider,
            environmentName: "env",
            packageName: "fixture",
            settings,
            valueRead: {
               environmentStore: {} as never,
               fetcher: fcjValueFetcher,
            },
         });
      const VALUES = [
         "Denver",
         "Boulder",
         "Denver International (DIA)",
         "Chicago O'Hare (ORD)",
      ];

      it("embeds each distinct stored value once, and reuses them on a rebuild", async () => {
         const first = await withValues();
         expect(first.values.distinctValues()).toEqual(VALUES);
         expect(first.values.embedded).toBe(4);
         expect(first.report.valueVectors).toEqual({
            values: 4,
            embedded: 4,
            reused: 0,
            deleted: 0,
            charsEmbedded: VALUES.join("").length,
            ms: first.report.valueVectors.ms,
         });
         const rows = await db.all<{ node_key: string; facet: string }>(
            "SELECT node_key, facet FROM value_embeddings ORDER BY node_key",
         );
         expect(rows.map((r) => [r.node_key, r.facet])).toEqual(
            [...VALUES].sort().map((v) => [v, "value"]),
         );
         // The field rows are untouched by the value rows.
         expect(first.report.vectors.rows).toBe(51);
         const second = await withValues();
         expect(second.report.valueVectors).toMatchObject({
            embedded: 0,
            reused: 4,
         });
         expect(second.values.embedded).toBe(4);
      });

      it("embeds nothing with valueEmbeddings off", async () => {
         const off = await withValues({
            ...DEFAULT_FCJ_SETTINGS,
            valueEmbeddings: false,
         });
         expect(off.report.valueVectors).toEqual({
            values: 4,
            embedded: 0,
            reused: 0,
            deleted: 0,
            charsEmbedded: 0,
            ms: 0,
            skipped: "retrieval.fcj.valueEmbeddings is false",
         });
         expect(off.values.embedded).toBe(0);
         const rows = await db.all("SELECT * FROM value_embeddings");
         expect(rows).toEqual([]);
      });

      it("keeps the build when the values cannot be embedded, and says so", async () => {
         const stub = stubEmbeddingProvider();
         const failing = Object.create(stub) as typeof stub;
         failing.embedBatch = async (texts, timeoutMs, retry) => {
            if (texts.includes("Boulder")) throw new Error("rate limited");
            return stub.embedBatch(texts, timeoutMs, retry);
         };
         const built = await withValues(DEFAULT_FCJ_SETTINGS, failing);
         expect(built.report.vectors.embedded).toBe(51);
         expect(built.values.embedded).toBe(0);
         expect(built.report.valueVectors.failure).toStartWith(
            "The value vectors step failed after embedding 0 of 4 new rows (0 reused): rate limited.",
         );
      });
   });

   it("adds three question rows per field and view when the model writes them", async () => {
      const chat = scriptedChat((prompt) => {
         if (prompt.includes("<docs>")) {
            return JSON.stringify({
               results: numberedLines(prompt).map(([i, line]) => ({
                  index: i,
                  what: line.split(". ")[0],
                  values: [],
                  rules: [],
                  examples: [],
               })),
            });
         }
         return JSON.stringify({
            results: numberedLines(prompt).map(([i]) => ({
               index: i,
               questions: [`how many ${i}?`, `by ${i}?`, `which ${i}?`],
            })),
         });
      });
      const index = await build({
         chat: chat.model,
         concurrency: 2,
         maxCallsPerSync: 100,
      });
      // 30 dimensions + 7 measures + 2 views get questions; sources and joins do not.
      expect(index.report.questions).toEqual({
         nodes: 39,
         written: 39,
         reused: 0,
         missing: 0,
      });
      expect(index.report.vectors.rows).toBe(51 + 39 * 3);
      expect(
         index.vectors
            .get("measure:flights@travel.malloy:flight_count")
            ?.map((f) => f.facet),
      ).toEqual(["derived", "q1", "q2", "q3"]);
   });

   it("writes questionsPerNode rows with the chosen wording, and shows values only to the values wording", async () => {
      const seen: string[] = [];
      const chat = scriptedChat((prompt) => {
         if (prompt.includes("<docs>")) {
            return JSON.stringify({
               results: numberedLines(prompt).map(([i, line]) => ({
                  index: i,
                  what: line.split(". ")[0],
                  values: [],
                  rules: [],
                  examples: [],
               })),
            });
         }
         seen.push(prompt);
         return JSON.stringify({
            results: numberedLines(prompt).map(([i]) => ({
               index: i,
               questions: [1, 2, 3, 4, 5, 6].map((k) => `phrase ${k} of ${i}`),
            })),
         });
      });
      const run = (settings: typeof DEFAULT_FCJ_SETTINGS) =>
         buildFcjIndex({
            pkgIndex: fixture.pkgIndex,
            db,
            provider: stubEmbeddingProvider(),
            environmentName: "env",
            packageName: "fixture",
            settings,
            llm: { chat: chat.model, concurrency: 1, maxCallsPerSync: 100 },
            // The two #(index) dimensions read these values, most frequent first.
            valueRead: {
               environmentStore: {} as never,
               fetcher: async () => ({
                  values: [
                     { value: "Boston", count: 9 },
                     { value: "Denver", count: 5 },
                  ],
                  truncated: false,
               }),
            },
         });
      const five = await run({
         ...DEFAULT_FCJ_SETTINGS,
         questionsPrompt: "phrases",
         questionsPerNode: 5,
      });
      // Six came back; five are kept, so 39 nodes x 5 rows.
      expect(five.report.vectors.rows).toBe(51 + 39 * 5);
      expect(
         five.vectors
            .get("measure:flights@travel.malloy:flight_count")
            ?.map((f) => f.facet),
      ).toEqual(["derived", "q1", "q2", "q3", "q4", "q5"]);
      expect(seen.every((p) => !p.includes(" Values: "))).toBe(true);
      expect(seen[0]).toContain(
         '"questions": ["...", "...", "...", "...", "..."]',
      );
      seen.length = 0;
      // The values wording is a new prompt, so every row is written again,
      // and an #(index) dimension shows its stored values on its line.
      const withValues = await run({
         ...DEFAULT_FCJ_SETTINGS,
         questionsPrompt: "values",
      });
      expect(withValues.report.questions).toMatchObject({
         written: 39,
         reused: 0,
      });
      const lines = seen.flatMap((p) => numberedLines(p).map(([, l]) => l));
      const shown = lines.filter((l) => l.includes(" Values: "));
      expect(shown.length).toBe(2);
      expect(shown.every((l) => l.endsWith(" Values: Boston, Denver."))).toBe(
         true,
      );
   });
});
