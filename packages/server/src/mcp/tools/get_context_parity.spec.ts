// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The settings that make Publisher's retrieval comparable with Credible's
// hosted retrieval, seen through the handler: value refine, the per-source
// candidate window, and one vector per entity.

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
   _resetValueIndexStateForTests,
   _settleValueIndexForTests,
} from "../../retrieval/dim_values";
import {
   _clearRetrievalConfigForTests,
   _setRetrievalConfigForTests,
} from "../../retrieval/retrieval_config";
import {
   EmbeddingProvider,
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";
import {
   _setLlmProviderForTests,
   type LlmRequest,
} from "../../service/llm_provider";
import { _resetLlmRunnerForTests } from "../../service/llm_runner";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import {
   createDimensionValueTables,
   createEntityEmbeddingsTable,
   createEntityEnrichmentTable,
} from "../../storage/duckdb/schema";
import { _resetEmbeddingIndexStateForTests } from "./embedding_index";
import { captureHandler, fakeLlm, parse, storeFor } from "./retrieval_test_kit";

const field = (
   kind: string,
   name: string,
   doc?: string,
   annotations: string[] = [],
) => ({
   kind,
   name,
   type: { kind: "string_type" },
   annotations: [...(doc ? [`#(doc) ${doc}`] : []), ...annotations],
});

/** A package whose sources each hold the given measures. */
function packageOf(
   sources: Record<string, Array<{ name: string; doc?: string }>>,
   dimensions: Record<
      string,
      Array<{ name: string; values: Array<[string, number]> }>
   > = {},
) {
   const values = new Map<string, Array<[string, number]>>();
   const infos = Object.entries(sources).map(([name, ms]) => ({
      name,
      annotations: [`#(doc) Rows of ${name}.`],
      schema: {
         fields: [
            ...ms.map((m) => field("measure", m.name, m.doc)),
            ...(dimensions[name] ?? []).map((d) => {
               values.set(d.name, d.values);
               return field("dimension", d.name, `The ${d.name}.`, [
                  "#(index)",
               ]);
            }),
         ],
      },
   }));
   const model = {
      getSourceInfos: () => infos,
      getQueries: () => [],
      getSources: () => infos.map((i) => ({ name: i.name })),
      getQueryResults: async (_s: unknown, _q: unknown, query: string) => {
         const dim = [...values.keys()].find((d) => query.includes(`\`${d}\``));
         if (!dim) throw new Error(`unexpected query: ${query}`);
         return {
            compactResult: values
               .get(dim)!
               .map(([v, w]) => ({ [dim]: v, value_weight__: w })),
         };
      },
   };
   return {
      listModels: async () => [{ path: "m.malloy" }],
      getModel: () => model,
   };
}

/** cosine to the query "sales" ([1, 0]): "strong" is 0.9, "weak" 0.3, anything else 0. */
const vectorFor = (text: string): number[] => {
   if (text === "sales") return [1, 0];
   if (/strong/.test(text)) return [0.9, Math.sqrt(1 - 0.81)];
   if (/weak/.test(text)) return [0.3, Math.sqrt(1 - 0.09)];
   return [0, 1];
};

function embedder(asked: string[] = []): EmbeddingProvider {
   const fetchStub = (async (_u: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      asked.push(...body.input);
      return new Response(
         JSON.stringify({
            data: body.input.map((t, index) => ({
               index,
               embedding: vectorFor(t),
            })),
         }),
         { status: 200 },
      );
   }) as typeof fetch;
   return new EmbeddingProvider(
      {
         apiKey: "t",
         model: "stub",
         baseUrl: "https://stub.example.com/v1",
         minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
      },
      fetchStub,
   );
}

describe("Credible-parity settings", () => {
   let tempDir: string;
   let db: DuckDBConnection;
   const ENV = "parity";

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "get-context-parity-"));
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await createEntityEmbeddingsTable(db);
      await createEntityEnrichmentTable(db);
      await createDimensionValueTables(db);
   });
   afterAll(async () => {
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
      _clearEmbeddingProviderForTests();
   });
   beforeEach(async () => {
      for (const t of [
         "entity_embeddings",
         "dimension_values",
         "dimension_value_state",
      ]) {
         await db.run(`DELETE FROM ${t}`);
      }
      _setEmbeddingProviderForTests(null);
      _setLlmProviderForTests(null);
      _resetEmbeddingIndexStateForTests();
      _resetValueIndexStateForTests();
      _resetLlmRunnerForTests();
   });
   afterEach(() => _clearRetrievalConfigForTests());

   const ask = (
      pkg: unknown,
      searches: unknown[],
      extra: Record<string, unknown> = {},
   ) =>
      captureHandler(storeFor(pkg, db))({
         search_targets: searches,
         scopes: [{ environment: ENV, package: "p", ...extra }],
      } as never);

   /** Ask until the semantic index is warm, then return that answer. */
   async function semantic(pkg: unknown, searches: unknown[]) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let last: any;
      for (let i = 0; i < 400; i++) {
         last = parse(await ask(pkg, searches));
         if (last.retrieval === "semantic") return last;
         await new Promise((r) => setTimeout(r, 5));
      }
      throw new Error(
         `never became semantic: retrieval=${last?.retrieval} reason=${last?.retrieval_reason} warnings=${JSON.stringify(last?.warnings)}`,
      );
   }

   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const entityNamesBySource = (payload: any): Record<string, string[]> => {
      const out: Record<string, string[]> = {};
      for (const c of payload.sources) {
         const s = c.source_info.resource_id.source;
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         out[s] = [...(out[s] ?? []), ...c.entities.map((e: any) => e.name)];
      }
      return out;
   };

   describe("value refine", () => {
      const pkg = packageOf(
         {
            customers: [
               { name: "customer_count", doc: "Number of customers." },
            ],
         },
         {
            customers: [
               {
                  name: "tier",
                  values: [
                     ["Premium", 50],
                     ["Premium Plus", 20],
                     ["Basic", 30],
                  ],
               },
            ],
         },
      );
      const VALUE = [
         { target_type: "dimensional_value", search_text: "premium" },
      ];
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const valuesOf = (payload: any) =>
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         payload.sources.flatMap((c: any) =>
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            c.entities.flatMap((e: any) =>
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               (e.values ?? []).map((v: any) => v.value),
            ),
         );

      async function withRefine(
         refine: Record<string, unknown> | null,
         reply?: (r: LlmRequest) => string,
      ) {
         _setRetrievalConfigForTests({
            dimensionalValues: {
               mode: "annotated",
               ...(refine ? { refine } : {}),
            },
            egress: { dimensionalValues: true },
            llm: { model: "m", cache: { enabled: false }, backoffMs: 0 },
         });
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(
            fakeLlm(
               reply ??
                  ((req) => {
                     const line = (v: string) =>
                        Number(
                           req.user.match(
                              new RegExp(`- \\[(\\d+)\\] ${v}:`),
                           )![1],
                        );
                     return JSON.stringify([
                        { index: line("Premium"), score: "HIGH" },
                     ]);
                  }),
               seen,
            ),
         );
         await ask(pkg, [{ target_type: "source" }]);
         await _settleValueIndexForTests(ENV, "p");
         return { seen, payload: parse(await ask(pkg, VALUE)) };
      }

      it("without it, every match the lexical arm finds is returned", async () => {
         const { seen, payload } = await withRefine(null);
         expect(valuesOf(payload).sort()).toEqual(["Premium", "Premium Plus"]);
         expect(seen).toHaveLength(0);
      });

      it("with it, the model's omissions are dropped and the stage is reported", async () => {
         const { seen, payload } = await withRefine({ enabled: true });
         expect(valuesOf(payload)).toEqual(["Premium"]);
         expect(seen.length).toBeGreaterThan(0);
         expect(seen.every((r) => r.stage === "valueRefine")).toBe(true);
         expect(payload.retrieval_stages).toEqual({ valueRefine: "ok" });
      });

      it("a failing model leaves the matches as they were, with a warning", async () => {
         const { payload } = await withRefine(
            { enabled: true },
            () => "not json at all",
         );
         expect(valuesOf(payload).sort()).toEqual(["Premium", "Premium Plus"]);
         expect(payload.retrieval_stages.valueRefine).toBe("failed:malformed");
         expect(payload.warnings.join(" ")).toContain(
            "value refine unavailable",
         );
      });
   });

   describe("candidate window", () => {
      const pkg = packageOf({
         big: ["strong_1", "strong_2", "strong_3", "strong_4", "strong_5"].map(
            (n) => ({ name: n, doc: `${n} sales` }),
         ),
         small: ["weak_1", "weak_2"].map((n) => ({
            name: n,
            doc: `${n} sales`,
         })),
      });
      const SALES = [{ target_type: "measure", search_text: "sales" }];

      async function windowed(candidates: Record<string, unknown>) {
         _setRetrievalConfigForTests({ candidates });
         _setEmbeddingProviderForTests(embedder());
         return entityNamesBySource(await semantic(pkg, SALES));
      }

      it("global (the default) crowds a weak source out when the window is tight", async () => {
         const got = await windowed({ window: "global", perTargetLimit: 3 });
         expect(Object.keys(got)).toEqual(["big"]);
         expect(got.big).toHaveLength(3);
      });

      it("per-source keeps each source's best rows, so the weak source still contributes", async () => {
         const got = await windowed({
            window: "per-source",
            perSourceLimit: 2,
         });
         expect(got.big).toEqual(["strong_1", "strong_2"]);
         expect(got.small).toEqual(["weak_1", "weak_2"]);
      });

      it("per-source with a limit of one takes the single best of each", async () => {
         const got = await windowed({
            window: "per-source",
            perSourceLimit: 1,
         });
         expect(got.big).toHaveLength(1);
         expect(got.small).toHaveLength(1);
      });
   });

   describe("one vector per entity", () => {
      // A fresh package object each time: the index caches its content
      // fingerprint per instance, and the representation is part of it. A server
      // fixes the representation at start-up, so only a test changes it.
      const make = () =>
         packageOf({
            shop: [
               { name: "alpha_score", doc: "General alpha metric." },
               { name: "gamma_total" },
            ],
         });
      const rows = async () =>
         Number(
            (
               await db.all<{ n: number }>(
                  "SELECT count(*) AS n FROM entity_embeddings",
               )
            )[0].n,
         );

      async function indexed(representation: "facets" | "single") {
         _setRetrievalConfigForTests({ embedding: { representation } });
         const asked: string[] = [];
         _setEmbeddingProviderForTests(embedder(asked));
         await semantic(make(), [
            { target_type: "measure", search_text: "sales" },
         ]);
         return asked;
      }

      it("facets (the default) embeds a name and a doc row for a documented field", async () => {
         const asked = await indexed("facets");
         expect(asked).toContain("alpha score");
         expect(asked).toContain("alpha score: General alpha metric.");
         expect(await rows()).toBeGreaterThan(3);
      });

      it("single embeds one row per entity: the doc as written, else the name", async () => {
         const asked = await indexed("single");
         // The source and its two fields: three entities, three rows.
         expect(await rows()).toBe(3);
         // The doc alone, not "name: doc", and no separate name row.
         expect(asked).toContain("General alpha metric.");
         expect(asked).not.toContain("alpha score");
         expect(asked).not.toContain("alpha score: General alpha metric.");
         // A field with no doc falls back to its name.
         expect(asked).toContain("gamma total");
      });

      it("changing the representation re-embeds the package, and drops the rows it no longer uses", async () => {
         await indexed("facets");
         const before = await rows();
         _resetEmbeddingIndexStateForTests();
         await indexed("single");
         expect(await rows()).toBe(3);
         expect(before).toBeGreaterThan(3);
      });
   });
});
