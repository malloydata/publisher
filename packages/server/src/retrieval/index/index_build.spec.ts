// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DuckDBConnection as MalloyDuckDB } from "@malloydata/db-duckdb";
import type { Connection } from "@malloydata/malloy";
import { Model } from "../../service/model";
import { DEFAULT_PACKAGE_RETRIEVAL } from "../../service/package_retrieval_settings";
import { getPackageIndex } from "../model/package_entities";
import { storeServing } from "../../test_helpers/get_context_join_fixture";
import { setRetrievalConfig } from "../config";
import { DEFAULT_FCJ_SETTINGS } from "../../service/package_retrieval_settings";
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
import { deleteEngineRows, ENGINE_TABLES } from "./catalog_store";
import { listItems, resolveDocParts, splitDocByRules } from "./doc_parts";
import { buildFcjIndex } from "./index_build";
import { KeywordIndex, stem, terms } from "./keyword";
import { VectorSyncError, type VectorProgress } from "./vectors";
import { SyncRequestBudget } from "../llm/llm_calls";

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

   it("keeps a link, a quoted value and Malloy code inside their sentence", () => {
      const example = 'Example: run: orders -> { where: country = "U.S." }';
      expect(splitDocByRules(`Amount. ${example}`)).toEqual({
         what: "Amount.",
         values: [],
         rules: [],
         examples: [example],
      });
      expect(splitDocByRules('Channel. Values: "Really? yes", "No".')).toEqual({
         what: "Channel.",
         values: ["Really? yes", "No"],
         rules: [],
         examples: [],
      });
      expect(splitDocByRules("Link to https://example.com/?q=x.").what).toBe(
         "Link to https://example.com/?q=x.",
      );
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

   it("preserves ASCII admission and uses Unicode for non-ASCII docs", async () => {
      const sent = [
         "Bestellbetrag in Euro. Über alle Währungen nie summieren.",
         "Importe del pedido. ¿Incluye impuestos?",
         "Сумма заказа в рублях. Исключайте тестовые заказы.",
         "Ποσό παραγγελίας. Εξαιρέστε τις δοκιμαστικές παραγγελίες.",
         "注文金額です。テスト注文は除外してください。",
         "مبلغ الطلب. استبعد طلبات الاختبار.",
         '"注文金額です。" "テスト注文は除外してください。"',
         "Сумма заказа. **Исключайте тестовые заказы.**",
         // A control: `sentencesOf` already finds two sentences here.
         "Amount. (exclude test orders.)",
         // Known limitation of the per-doc non-ASCII guard.
         "Größe laut https://example.com/?q=x.",
      ];
      const notSent = [
         "Сумма заказа в рублях.",
         "注文金額です。",
         // One sentence under the fixed "en" locale: Greek's ends one at ";".
         "Ποσό παραγγελίας; Εξαιρέστε τις δοκιμαστικές.",
         // Unicode ends a sentence inside each of these ASCII docs.
         "Link to https://example.com/?q=x.",
         'Example: run: orders -> { where: country = "U.S." }',
         "Uses a ?? b as default",
         "Run: orders -> { group_by: a; aggregate: b }",
      ];
      const chat = scriptedChat((prompt) =>
         JSON.stringify({
            results: numberedLines(prompt).map(([index, what]) => ({
               index,
               what,
            })),
         }),
      );
      const out = await resolveDocParts({
         docs: [...sent, ...notSent],
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
      expect(
         chat.prompts.flatMap((p) => numberedLines(p).map(([, line]) => line)),
      ).toEqual(sent);
      expect(out.longDocs).toBe(sent.length);
   });

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

   it("embeds every row again when the requested vector length changes", async () => {
      const first = await build();
      expect(first.report.vectors.reused).toBe(0);
      // The same model, asked for another length (EMBEDDING_DIMENSIONS).
      const resized = Object.defineProperty(
         Object.create(stubEmbeddingProvider()),
         "dimensions",
         { value: 64 },
      );
      const again = await buildFcjIndex({
         pkgIndex: fixture.pkgIndex,
         db,
         provider: resized,
         environmentName: "env",
         packageName: "fixture",
         settings: DEFAULT_FCJ_SETTINGS,
      });
      expect(again.report.vectors.reused).toBe(0);
      expect(again.report.vectors.embedded).toBe(first.report.vectors.rows);
   });

   it("sends no LLM request once a budget shared with an earlier step is spent", async () => {
      const chat = scriptedChat(() => "{}");
      // Source summaries spent the build's whole maxCallsPerSync.
      const budget = new SyncRequestBudget(0);
      const built = await buildFcjIndex({
         pkgIndex: fixture.pkgIndex,
         db,
         provider: stubEmbeddingProvider(),
         environmentName: "env",
         packageName: "fixture",
         settings: DEFAULT_FCJ_SETTINGS,
         llm: { chat: chat.model, concurrency: 1, maxCallsPerSync: 100 },
         budget,
      });
      expect(chat.prompts).toHaveLength(0);
      expect(built.report.questions.written).toBe(0);
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

   it("reports the vectors step's progress after each stored batch, and reused rows at once", async () => {
      const batched = Object.defineProperty(
         Object.create(stubEmbeddingProvider()),
         "maxBatch",
         { value: 20 },
      );
      const seen: VectorProgress[] = [];
      const run = () =>
         buildFcjIndex({
            pkgIndex: fixture.pkgIndex,
            db,
            provider: batched,
            environmentName: "env",
            packageName: "fixture",
            settings: DEFAULT_FCJ_SETTINGS,
            onVectorProgress: (p) => seen.push(p),
         });
      await run();
      // One derived row per node, so rows and entities move together here.
      expect(seen.map((p) => [p.embeddedRows, p.embeddedEntities])).toEqual([
         [0, 0],
         [20, 20],
         [40, 40],
         [51, 51],
      ]);
      expect(
         new Set(seen.map((p) => `${p.totalRows}/${p.totalEntities}`)),
      ).toEqual(new Set(["51/51"]));
      seen.length = 0;
      await run();
      expect(seen).toEqual([
         {
            totalRows: 51,
            embeddedRows: 51,
            totalEntities: 51,
            embeddedEntities: 51,
         },
      ]);
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
      beforeEach(() => setRetrievalConfig({ egress: { preset: "full" } }));
      afterEach(() => setRetrievalConfig(undefined));
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

      it("embeds no stored value under the default egress preset, and says why", async () => {
         setRetrievalConfig(undefined);
         const built = await withValues();
         expect(built.values.embedded).toBe(0);
         expect(built.report.valueVectors).toMatchObject({
            values: 4,
            embedded: 0,
            skipped:
               'egress.preset is "default": stored values are sent to the embedding provider only under "full"',
         });
         const rows = await db.all("SELECT * FROM value_embeddings");
         expect(rows).toEqual([]);
      });

      it("reads no stored value when the operator turned value search off", async () => {
         setRetrievalConfig({
            egress: { preset: "full" },
            dimensionValues: { enabled: false },
         });
         const built = await withValues();
         expect(built.values.size).toBe(0);
         expect(built.report.values).toEqual({
            skipped:
               "retrieval.dimensionValues.enabled is false in publisher.config.json",
         });
      });

      it("leaves no row in any engine table once the package is deleted", async () => {
         await withValues();
         const count = async (table: string) =>
            Number(
               (
                  await db.all<{ n: number }>(
                     `SELECT count(*) AS n FROM ${table} WHERE environment_name = 'env' AND package_name = 'fixture'`,
                  )
               )[0].n,
            );
         expect(await count("dimension_values")).toBeGreaterThan(0);
         expect(await count("value_embeddings")).toBeGreaterThan(0);
         await deleteEngineRows(db, "env", "fixture");
         for (const table of ENGINE_TABLES) {
            expect([table, await count(table)]).toEqual([table, 0]);
         }
      });

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
      // Under the default egress preset the values wording shows no stored
      // value: they are data, and leave the server only under `full`.
      await run({ ...DEFAULT_FCJ_SETTINGS, questionsPrompt: "values" });
      expect(seen.every((p) => !p.includes(" Values: "))).toBe(true);
      seen.length = 0;
      // Under `full`, an #(index) dimension shows its stored values on its
      // line, so only those two lines are written again.
      setRetrievalConfig({ egress: { preset: "full" } });
      const withValues = await run({
         ...DEFAULT_FCJ_SETTINGS,
         questionsPrompt: "values",
      }).finally(() => setRetrievalConfig(undefined));
      expect(withValues.report.questions).toMatchObject({
         written: 2,
         reused: 37,
      });
      const lines = seen.flatMap((p) => numberedLines(p).map(([, l]) => l));
      const shown = lines.filter((l) => l.includes(" Values: "));
      expect(shown.length).toBe(2);
      expect(shown.every((l) => l.endsWith(" Values: Boston, Denver."))).toBe(
         true,
      );
   });
});

describe("the value index on a model with access gates", () => {
   // Every gated source here carries an #(index) dimension, so only the
   // derivation of `gated` (catalogInputOf) keeps its values out.
   const GATED_FILES: Record<string, string> = {
      "gates.malloy": `##! experimental.givens
given:
  ROLE :: string
  TENANTS :: string[]

#(authorize) 'analyst' = $ROLE
source: authorized is duckdb.sql("select 'a' as label") extend {
  #(index)
  dimension: tag is label
}

#(access_filter) label in $TENANTS
source: filtered is duckdb.sql("select 'f' as label") extend {
  #(index)
  dimension: tag is label
}

source: authorized_ext is authorized extend {
  #(index)
  dimension: tag2 is label
}

source: open is duckdb.sql("select 'o' as label") extend {
  #(index)
  dimension: tag is label
}
`,
      // A #(secure) given is in scope for every source of its file, so each
      // source it defines counts as gated: the safe side. It also imports
      // gates.malloy, so \`open\` is served here too, beside a secure given it
      // cannot read: \`open\` must stay ungated, decided by the file that
      // defines it.
      "secure.malloy": `##! experimental.givens
import "gates.malloy"

#(secure)
given: TENANT :: string is 'w'

source: secure_reader is duckdb.sql("select 's' as label") extend {
  where: label = $TENANT
  #(index)
  dimension: tag is label
}
`,
      "reports.malloy": `import "gates.malloy"

source: imported_ext is authorized extend {
  #(index)
  dimension: tag3 is label
}
`,
   };

   it("reads and embeds only the ungated source's values, whatever the gate and wherever the source is served", async () => {
      // Real Models: the compiled sources carry the gates (getSources), which
      // the compiler-only fixture does not.
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fcj-gated-"));
      const duckdb = new MalloyDuckDB("duckdb", ":memory:", dir);
      const models = new Map<string, Model>();
      for (const [file, text] of Object.entries(GATED_FILES)) {
         fs.writeFileSync(path.join(dir, file), text);
      }
      for (const file of Object.keys(GATED_FILES)) {
         const model = await Model.create(
            "fixture",
            dir,
            file,
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         expect(
            (model as unknown as { compilationError?: Error }).compilationError,
         ).toBeUndefined();
         models.set(file, model);
      }
      const pkg = {
         getPackageName: () => "fixture",
         getRetrievalSettings: () => DEFAULT_PACKAGE_RETRIEVAL,
         listSkills: () => [],
         listModels: async () => [...models.keys()].map((p) => ({ path: p })),
         getModel: (p: string) => models.get(p),
      };
      const pkgIndex = await getPackageIndex(
         new WeakMap(),
         storeServing(pkg),
         "env",
         "fixture",
      );
      const { db, close } = await openTempDb(createCatalogTables);
      const asked: string[] = [];
      setRetrievalConfig({ egress: { preset: "full" } });
      try {
         const built = await buildFcjIndex({
            pkgIndex,
            db,
            provider: stubEmbeddingProvider(),
            environmentName: "env",
            packageName: "fixture",
            settings: DEFAULT_FCJ_SETTINGS,
            valueRead: {
               environmentStore: {} as never,
               fetcher: async (req) => {
                  asked.push(`${req.modelPath}:${req.source}.${req.dimension}`);
                  return {
                     values: [{ value: `${req.source}-value`, count: 1 }],
                     truncated: false,
                  };
               },
            },
         });
         expect(asked).toEqual(["gates.malloy:open.tag"]);
         const stored = await db.all<{ value: string }>(
            "SELECT DISTINCT value FROM dimension_values",
         );
         expect(stored.map((r) => r.value)).toEqual(["open-value"]);
         const embedded = await db.all<{ node_key: string }>(
            "SELECT node_key FROM value_embeddings",
         );
         expect(embedded.map((r) => r.node_key)).toEqual(["open-value"]);
         expect(built.values.distinctValues()).toEqual(["open-value"]);
      } finally {
         setRetrievalConfig(undefined);
         await close();
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});
