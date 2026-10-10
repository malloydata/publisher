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
import {
   numberedLines,
   scriptedChat,
   stubEmbeddingProvider,
} from "../../test_helpers/get_context_llm_harness";
import { listItems, resolveDocParts, splitDocByRules } from "./doc_parts";
import { buildFcjIndex } from "./index_build";
import { KeywordIndex, stem, terms } from "./keyword";
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
      expect(index.hasTerm("denver")).toBe(false);
      expect(index.hasTerm("airport")).toBe(true);
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
         rows: 50,
         embedded: 50,
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
         rows: 50,
         embedded: 0,
         reused: 50,
         deleted: 0,
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
      // 29 dimensions + 7 measures + 2 views get questions; sources and joins do not.
      expect(index.report.questions).toEqual({
         nodes: 38,
         written: 38,
         reused: 0,
         missing: 0,
      });
      expect(index.report.vectors.rows).toBe(50 + 38 * 3);
      expect(
         index.vectors
            .get("measure:flights@travel.malloy:flight_count")
            ?.map((f) => f.facet),
      ).toEqual(["derived", "q1", "q2", "q3"]);
   });
});
