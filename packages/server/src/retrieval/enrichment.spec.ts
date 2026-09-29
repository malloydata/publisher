// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../config";
import {
   _resetEmbeddingIndexStateForTests,
   deletePackageEmbeddings,
   enrichmentOverlayVersion,
} from "../mcp/tools/embedding_index";
import { EmbeddingProvider } from "../service/embedding_provider";
import {
   LlmError,
   type LlmProvider,
   type LlmRequest,
} from "../service/llm_provider";
import { LlmRunner, runnerSettingsFrom } from "../service/llm_runner";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import {
   createEntityEmbeddingsTable,
   createEntityEnrichmentTable,
} from "../storage/duckdb/schema";
import type { EnrichableEntity } from "./egress";
import {
   _resetEnrichmentStateForTests,
   getEnrichmentStatus,
   hydrateEnrichment,
   kickEnrichment,
   planEnrichment,
   renderKeyphrase,
   runEnrichment,
   _settleEnrichmentForTests,
} from "./enrichment";
import { resolveRetrievalConfig, type RetrievalConfig } from "./retrieval_config";

const ENV = "env";
const PKG = "pkg";

const entity = (
   kind: string,
   name: string,
   source: string | undefined,
   embedDoc = "",
   extra: Partial<EnrichableEntity> = {},
): EnrichableEntity => ({
   kind,
   name,
   source,
   modelPath: "m.malloy",
   embedDoc,
   ...extra,
});

const ENTITIES: EnrichableEntity[] = [
   entity("source", "orders", "orders", "Every order placed."),
   entity("dimension", "cust_ltv", "orders", "", { dataType: "number", code: "sum(amount)" }),
   entity("measure", "total_revenue", "orders", "Revenue."),
   entity(
      "measure",
      "net_revenue",
      "orders",
      "Revenue after refunds, discounts, taxes, shipping, and every other adjustment we make.",
   ),
   entity("view", "by_month", "orders", ""),
];

function config(over: Record<string, unknown> = {}): RetrievalConfig {
   return resolveRetrievalConfig({
      enrichment: {
         enabled: true,
         sourceSummary: { enabled: true },
         keyphrase: { batchSize: 1 },
      },
      llm: { model: "m", backoffMs: 0, cache: { enabled: false } },
      ...over,
   });
}

/** A scripted LLM that answers single-field and summary prompts. */
function llm(
   seen: LlmRequest[] = [],
   reply?: (req: LlmRequest) => string | LlmError,
): LlmProvider {
   return {
      id: "fake",
      async complete(req) {
         seen.push(req);
         const custom = reply?.(req);
         if (custom instanceof LlmError) throw custom;
         if (custom !== undefined) return { text: custom, model: req.model, latencyMs: 1 };
         if (req.stage === "summary") {
            return {
               text: JSON.stringify({
                  summary: "A summary of the source.",
                  one_line_summary: "Orders.",
               }),
               model: req.model,
               latencyMs: 1,
            };
         }
         const name = req.user.match(/Field name: (\S+)/)?.[1] ?? "x";
         return {
            text: JSON.stringify({ keyphrase: `Keyphrase for ${name}.` }),
            model: req.model,
            latencyMs: 1,
         };
      },
   };
}

const runnerFor = (provider: LlmProvider, cfg: RetrievalConfig) =>
   new LlmRunner(provider, runnerSettingsFrom(cfg.llm));

/** Embeds anything, and remembers what it was asked. */
function embedder(asked: string[] = []): EmbeddingProvider {
   const fetchStub = (async (_u: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      asked.push(...body.input);
      return new Response(
         JSON.stringify({ data: body.input.map((_, index) => ({ index, embedding: [1, 0] })) }),
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

describe("enrichment", () => {
   let tempDir: string;
   let db: DuckDBConnection;

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "enrichment-"));
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await createEntityEmbeddingsTable(db);
      await createEntityEnrichmentTable(db);
   });
   afterAll(async () => {
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });
   beforeEach(async () => {
      _resetEmbeddingIndexStateForTests();
      _resetEnrichmentStateForTests();
      await db.run("DELETE FROM entity_embeddings");
      await db.run("DELETE FROM entity_enrichment");
   });

   const args = (cfg: RetrievalConfig, provider: LlmProvider, extra: Partial<Parameters<typeof runEnrichment>[0]> = {}) => ({
      db,
      provider: embedder(),
      environmentName: ENV,
      packageName: PKG,
      entities: ENTITIES,
      config: cfg,
      runner: runnerFor(provider, cfg),
      ...extra,
   });

   describe("planning", () => {
      const plan = (cfg: RetrievalConfig) =>
         planEnrichment({
            db,
            environmentName: ENV,
            packageName: PKG,
            entities: ENTITIES,
            config: cfg,
            models: { keyphrase: "m", summary: "m" },
         });
      const names = async (cfg: RetrievalConfig) =>
         (await plan(cfg)).pending.map((i) => `${i.what}:${i.entity.name}`).sort();

      it("asks for a keyphrase where the doc is missing or long, and a summary per source", async () => {
         expect(await names(config())).toEqual([
            "keyphrase:by_month",
            "keyphrase:cust_ltv",
            "keyphrase:net_revenue",
            "source_summary:orders",
         ]);
      });

      it("leaves a short doc alone, since it is already its own best keyphrase", async () => {
         expect(await names(config())).not.toContain("keyphrase:total_revenue");
      });

      it("covers every field in always mode, and none in never mode", async () => {
         const always = await names(
            config({ enrichment: { enabled: true, keyphrase: { mode: "always" } } }),
         );
         expect(always).toContain("keyphrase:total_revenue");
         const never = await names(
            config({ enrichment: { enabled: true, keyphrase: { mode: "never" }, sourceSummary: { enabled: true } } }),
         );
         expect(never).toEqual(["source_summary:orders"]);
      });

      it("uses a stricter threshold for views than for fields", async () => {
         const c = config({
            enrichment: { enabled: true, keyphrase: { wordThreshold: 1, viewWordThreshold: 50 } },
         });
         const entities = [
            entity("measure", "m1", "s", "two words"),
            entity("view", "v1", "s", "a view doc with quite a lot of words in it, really"),
         ];
         const p = await planEnrichment({
            db,
            environmentName: ENV,
            packageName: PKG,
            entities,
            config: c,
            models: { keyphrase: "m", summary: "m" },
         });
         expect(p.pending.map((i) => i.entity.name)).toEqual(["m1"]);
      });

      it("asks for summaries first, then the emptiest docs", async () => {
         const p = await plan(config());
         expect(p.pending[0].what).toBe("source_summary");
         const kp = p.pending.filter((i) => i.what === "keyphrase").map((i) => i.entity.name);
         // the two with no doc come before the one with a long doc
         expect(kp.indexOf("net_revenue")).toBe(2);
      });
   });

   describe("running", () => {
      it("writes keyphrases and summaries, stores them, and installs the overlay", async () => {
         const cfg = config();
         const asked: string[] = [];
         const status = await runEnrichment(
            args(cfg, llm(), { provider: embedder(asked) }),
         );
         expect(status).toMatchObject({ status: "ready", eligible: 4, enriched: 4, failed: 0 });
         const rows = await db.all<any>(
            "SELECT entity_name, enrichment, status, text, text2 FROM entity_enrichment ORDER BY entity_name, enrichment",
         );
         expect(rows.map((r: any) => `${r.enrichment}:${r.entity_name}:${r.status}`)).toEqual([
            "keyphrase:by_month:ok",
            "keyphrase:cust_ltv:ok",
            "keyphrase:net_revenue:ok",
            "source_summary:orders:ok",
         ]);
         expect(rows.find((r: any) => r.enrichment === "keyphrase" && r.entity_name === "cust_ltv").text).toBe(
            "Keyphrase for cust_ltv.",
         );
         const summary = rows.find((r: any) => r.enrichment === "source_summary");
         expect(summary.text).toBe("A summary of the source.");
         expect(summary.text2).toBe("Orders.");
         // The generated facets are embedded rows, rendered through the template.
         const facets = await db.all<any>(
            "SELECT entity_name, facet FROM entity_embeddings WHERE facet = 'kw' OR facet LIKE 'sum:%' ORDER BY entity_name, facet",
         );
         expect(facets.map((f: any) => `${f.entity_name}:${f.facet}`)).toEqual([
            "by_month:kw",
            "cust_ltv:kw",
            "net_revenue:kw",
            "orders:sum:0",
         ]);
         expect(asked).toContain("cust ltv: Keyphrase for cust_ltv.");
         expect(enrichmentOverlayVersion(ENV, PKG)).toBe(1);
      });

      it("asks the LLM nothing the second time, and embeds nothing new", async () => {
         const cfg = config();
         const seen: LlmRequest[] = [];
         await runEnrichment(args(cfg, llm(seen)));
         const first = seen.length;
         expect(first).toBe(4);
         const asked: string[] = [];
         const again = await runEnrichment(args(cfg, llm(seen), { provider: embedder(asked) }));
         expect(seen.length).toBe(first);
         expect(again.status).toBe("ready");
         expect(asked).toEqual([]);
      });

      it.each([
         // The field's own keyphrase, and the source summary that lists its doc.
         ["a changed doc", (e: EnrichableEntity[]) => e.map((x) => (x.name === "net_revenue" ? { ...x, embedDoc: `${x.embedDoc} And more.` } : x)), undefined, 2],
         ["a changed model", undefined, { llm: { model: "other", backoffMs: 0, cache: { enabled: false } } }, 4],
         ["a switched egress class", undefined, { egress: { code: true } }, 4],
      ] as const)("asks again when it saw something different: %s", async (_label, mutate, over, expected) => {
         const seen: LlmRequest[] = [];
         await runEnrichment(args(config(), llm(seen)));
         seen.length = 0;
         await runEnrichment(
            args(config(over as never), llm(seen), {
               entities: mutate ? mutate(ENTITIES) : ENTITIES,
            }),
         );
         expect(seen.length).toBe(expected);
      });

      it("does not ask again for an edit to a field it does not enrich", async () => {
         const seen: LlmRequest[] = [];
         await runEnrichment(args(config(), llm(seen)));
         seen.length = 0;
         // total_revenue has a short doc, so no keyphrase; but the source summary
         // lists it, so only that one call is made.
         await runEnrichment(
            args(config(), llm(seen), {
               entities: ENTITIES.map((x) =>
                  x.name === "total_revenue" ? { ...x, embedDoc: "Revenue, edited." } : x,
               ),
            }),
         );
         expect(seen.map((r) => r.stage)).toEqual(["summary"]);
      });

      it("stores a failure, and does not retry it until the delay has passed", async () => {
         const cfg = config({ enrichment: { enabled: true, keyphrase: { batchSize: 1 }, retryAfterMs: 60_000 } });
         const bad = llm([], (req) =>
            req.user.includes("Field name: cust_ltv") ? "no json here at all" : undefined as never,
         );
         const first = await runEnrichment(args(cfg, bad));
         expect(first.status).toBe("partial");
         expect(first.failed).toBe(1);
         const row = await db.all<any>(
            "SELECT status, attempts, last_error FROM entity_enrichment WHERE entity_name = 'cust_ltv'",
         );
         expect(row[0].status).toBe("failed");
         expect(row[0].attempts).toBe(1);
         expect(row[0].last_error).toContain("could not be read");
         const seen: LlmRequest[] = [];
         await runEnrichment(args(cfg, llm(seen)));
         expect(seen).toHaveLength(0); // still waiting out retryAfterMs
         _resetEnrichmentStateForTests();
         const retry = config({ enrichment: { enabled: true, keyphrase: { batchSize: 1 }, retryAfterMs: 0 } });
         await runEnrichment(args(retry, llm(seen)));
         expect(seen).toHaveLength(1);
         const healed = await db.all<any>(
            "SELECT status, attempts FROM entity_enrichment WHERE entity_name = 'cust_ltv'",
         );
         expect(healed[0]).toMatchObject({ status: "ok", attempts: 2 });
      });

      it("defers what the call budget cannot cover, then finishes on the next run", async () => {
         const cfg = config({
            indexing: { maxLlmCallsPerSync: 2 },
         });
         const seen: LlmRequest[] = [];
         const first = await runEnrichment(args(cfg, llm(seen)));
         expect(seen).toHaveLength(2);
         expect(first).toMatchObject({ status: "partial", enriched: 2, deferredByBudget: 2 });
         // The summary went first, then the emptiest doc.
         expect(seen[0].stage).toBe("summary");
         seen.length = 0;
         const second = await runEnrichment(args(cfg, llm(seen)));
         expect(seen).toHaveLength(2);
         expect(second).toMatchObject({ status: "ready", enriched: 4, deferredByBudget: 0 });
      });

      it("adds no more rows than the package's item budget leaves, summaries first", async () => {
         const cfg = config();
         const seen: LlmRequest[] = [];
         const status = await runEnrichment(args(cfg, llm(seen), { itemBudget: 2 }));
         // Nothing past the budget is asked, so no LLM call is spent on it.
         expect(seen).toHaveLength(2);
         expect(seen[0].stage).toBe("summary");
         expect(status).toMatchObject({ status: "partial", enriched: 2, deferredByBudget: 2 });
      });

      it("adds nothing when the entities' own rows already fill the budget", async () => {
         const seen: LlmRequest[] = [];
         const status = await runEnrichment(args(config(), llm(seen), { itemBudget: 0 }));
         expect(seen).toHaveLength(0);
         expect(status).toMatchObject({ status: "partial", enriched: 0, deferredByBudget: 4 });
      });

      it("leaves cached text over a lowered budget out of the index, without clearing it", async () => {
         const cfg = config();
         await runEnrichment(args(cfg, llm([])));
         const seen: LlmRequest[] = [];
         const lowered = await runEnrichment(args(cfg, llm(seen), { itemBudget: 1 }));
         // The cache still answers, so nothing is asked again ...
         expect(seen).toHaveLength(0);
         // ... but only one row (the summary) is installed, and the rest wait.
         expect(lowered).toMatchObject({ status: "partial", enriched: 1 });
         const rows = await db.all<{ n: number }>(
            "SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_enrichment WHERE status = 'ok'",
         );
         expect(rows[0].n).toBe(4);
      });

      it("batches keyphrases by source and reads a batch reply", async () => {
         const cfg = config({
            enrichment: { enabled: true, keyphrase: { batchSize: 10 } },
         });
         const seen: LlmRequest[] = [];
         const p = llm(seen, (req) =>
            req.stage === "keyphrase"
               ? JSON.stringify([0, 1, 2].map((index) => ({ index, keyphrase: `Batch phrase ${index}.` })))
               : undefined as never,
         );
         const status = await runEnrichment(args(cfg, p));
         expect(seen.filter((r) => r.stage === "keyphrase")).toHaveLength(1);
         expect(status.enriched).toBe(3);
         const kp = await db.all<any>(
            "SELECT text FROM entity_enrichment WHERE enrichment = 'keyphrase' ORDER BY text",
         );
         expect(kp.map((r: any) => r.text)).toEqual(["Batch phrase 0.", "Batch phrase 1.", "Batch phrase 2."]);
      });

      it("marks a field the batch reply skipped as failed, not enriched", async () => {
         const cfg = config({ enrichment: { enabled: true, keyphrase: { batchSize: 10 } } });
         const p = llm([], (req) =>
            req.stage === "keyphrase"
               ? JSON.stringify([{ index: 0, keyphrase: "Only one." }])
               : undefined as never,
         );
         const status = await runEnrichment(args(cfg, p));
         expect(status.failed).toBe(2);
         expect(status.enriched).toBe(1);
      });

      it("serves the base index and reports failed when embeddings are down", async () => {
         const down = new EmbeddingProvider(
            {
               apiKey: "t",
               model: "stub",
               baseUrl: "https://stub.example.com/v1",
               minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
            },
            (async () => new Response("down", { status: 500 })) as unknown as typeof fetch,
         );
         const cfg = config();
         const status = await runEnrichment(args(cfg, llm(), { provider: down }));
         expect(status.status).toBe("failed");
         expect(status.error).toContain("500");
         // The generated text is still stored, so a retry costs no LLM calls.
         const rows = await db.all<any>("SELECT count(*) AS n FROM entity_enrichment");
         expect(Number(rows[0].n)).toBe(4);
         expect(enrichmentOverlayVersion(ENV, PKG)).toBe(0);
      });

      it("fails clearly, without a call, when no model is set", async () => {
         const cfg = resolveRetrievalConfig({
            enrichment: { enabled: true },
         });
         const seen: LlmRequest[] = [];
         const status = await runEnrichment(args(cfg, llm(seen)));
         expect(status.status).toBe("failed");
         expect(status.error).toContain("no model for keyphrase generation");
         expect(seen).toHaveLength(0);
      });

      it("refuses to run, without a call, when the names class is off", async () => {
         const seen: LlmRequest[] = [];
         const status = await runEnrichment(
            args(config({ egress: { names: false } }), llm(seen)),
         );
         expect(status.status).toBe("failed");
         expect(status.error).toContain("retrieval.egress.names is off");
         expect(seen).toHaveLength(0);
      });
   });

   describe("what leaves the machine", () => {
      const tenant = "#(access_filter) \"$TENANT = 'acme'\"";
      const sensitive: EnrichableEntity[] = [
         entity("source", "orders", "orders", "Every order."),
         entity("measure", "secret_total", "orders", "", {
            dataType: "number",
            code: `${tenant}\n#(authorize) "$ROLE = 'admin'"\nsum(amount) { where: region = 'west' }`,
         }),
         entity("view", "leaky_view", "orders", "", {
            code: `${tenant}\nrun: orders -> { aggregate: n is count() }`,
         }),
      ];

      it.each([
         ["the default classes", {}],
         ["every class on", { egress: { preset: "full" } }],
         ["code alone", { egress: { code: true, schemaContext: true } }],
      ])("never sends a predicate annotation to the provider: %s", async (_label, over) => {
         const seen: LlmRequest[] = [];
         const cfg = config(over);
         await runEnrichment(args(cfg, llm(seen), { entities: sensitive }));
         expect(seen.length).toBeGreaterThan(0);
         const everything = seen.map((r) => `${r.system}\n${r.user}`).join("\n");
         expect(everything).not.toContain("acme");
         expect(everything).not.toContain("TENANT");
         expect(everything).not.toContain("access_filter");
         expect(everything).not.toContain("#(authorize)");
         expect(everything).not.toContain("admin");
      });

      it("sends the expression itself only when the code class is on", async () => {
         const off: LlmRequest[] = [];
         await runEnrichment(args(config(), llm(off), { entities: sensitive }));
         expect(off.map((r) => r.user).join("\n")).not.toContain("region = 'west'");
         const on: LlmRequest[] = [];
         _resetEnrichmentStateForTests();
         await db.run("DELETE FROM entity_enrichment");
         await runEnrichment(
            args(config({ egress: { code: true } }), llm(on), { entities: sensitive }),
         );
         expect(on.map((r) => r.user).join("\n")).toContain("sum(amount) { where: region = 'west' }");
      });

      it("says a withheld input was withheld, so the model does not invent it", async () => {
         const seen: LlmRequest[] = [];
         await runEnrichment(args(config(), llm(seen), { entities: sensitive }));
         const kp = seen.find((r) => r.stage === "keyphrase")!;
         expect(kp.user).toContain("Sibling fields (schema context):\n(not provided)");
         expect(kp.user).toContain("Field code:\n(not provided)");
      });

      it("sends no descriptions when the docs class is off", async () => {
         const seen: LlmRequest[] = [];
         const withDoc = [
            entity("measure", "net_revenue", "orders", "Revenue after refunds, discounts, taxes, shipping, and every other adjustment we make."),
         ];
         await runEnrichment(
            args(config({ egress: { docs: false } }), llm(seen), { entities: withDoc }),
         );
         expect(seen.map((r) => r.user).join("\n")).not.toContain("Revenue after refunds");
      });
   });

   describe("through a restart", () => {
      it("restores generated facets from the cache without asking the LLM or re-embedding", async () => {
         const cfg = config();
         await runEnrichment(args(cfg, llm()));
         // Simulate a restart: memory is gone, the database is not.
         _resetEmbeddingIndexStateForTests();
         _resetEnrichmentStateForTests();
         expect(enrichmentOverlayVersion(ENV, PKG)).toBe(0);
         const instance = {};
         await hydrateEnrichment(instance, {
            db,
            environmentName: ENV,
            packageName: PKG,
            entities: ENTITIES,
            config: cfg,
            envModel: undefined,
         });
         expect(enrichmentOverlayVersion(ENV, PKG)).toBe(1);
         expect(getEnrichmentStatus(ENV, PKG)).toMatchObject({ status: "ready", enriched: 4 });
         // A later run over the same cache changes nothing and asks for nothing.
         const seen: LlmRequest[] = [];
         const asked: string[] = [];
         await runEnrichment(args(cfg, llm(seen), { provider: embedder(asked) }));
         expect(seen).toHaveLength(0);
         expect(asked).toEqual([]);
      });

      it("hydrates once per package instance", async () => {
         const cfg = config();
         await runEnrichment(args(cfg, llm()));
         _resetEmbeddingIndexStateForTests();
         const instance = {};
         const hydrateArgs = {
            db,
            environmentName: ENV,
            packageName: PKG,
            entities: ENTITIES,
            config: cfg,
         };
         await hydrateEnrichment(instance, hydrateArgs);
         const v = enrichmentOverlayVersion(ENV, PKG);
         await hydrateEnrichment(instance, hydrateArgs);
         expect(enrichmentOverlayVersion(ENV, PKG)).toBe(v);
      });

      it("removes a deleted package's generated text with its embeddings", async () => {
         await runEnrichment(args(config(), llm()));
         await deletePackageEmbeddings(db, ENV, PKG);
         const n = await db.all<any>("SELECT count(*) AS n FROM entity_enrichment");
         expect(Number(n[0].n)).toBe(0);
      });
   });

   describe("kicking", () => {
      it("runs once per package instance and is safe to call on every request", async () => {
         const cfg = config();
         const seen: LlmRequest[] = [];
         const instance = {};
         const a = args(cfg, llm(seen));
         kickEnrichment(instance, a);
         kickEnrichment(instance, a);
         kickEnrichment(instance, a);
         await _settleEnrichmentForTests(ENV, PKG);
         expect(seen).toHaveLength(4);
         kickEnrichment(instance, a);
         await _settleEnrichmentForTests(ENV, PKG);
         expect(seen).toHaveLength(4);
         expect(getEnrichmentStatus(ENV, PKG)?.status).toBe("ready");
      });

      it("goes again for unfinished work once retryAfterMs has passed", async () => {
         const cfg = config({ indexing: { maxLlmCallsPerSync: 2 }, enrichment: { enabled: true, sourceSummary: { enabled: true }, keyphrase: { batchSize: 1 }, retryAfterMs: 1000 } });
         const seen: LlmRequest[] = [];
         const instance = {};
         const a = args(cfg, llm(seen));
         const t0 = Date.now();
         kickEnrichment(instance, a, t0);
         await _settleEnrichmentForTests(ENV, PKG);
         expect(seen).toHaveLength(2);
         kickEnrichment(instance, a, t0 + 10); // too soon
         await _settleEnrichmentForTests(ENV, PKG);
         expect(seen).toHaveLength(2);
         kickEnrichment(instance, a, t0 + 5_000);
         await _settleEnrichmentForTests(ENV, PKG);
         expect(seen).toHaveLength(4);
      });
   });

   describe("renderKeyphrase", () => {
      it("fills the template with the humanized name and the phrase", () => {
         expect(renderKeyphrase("{name}: {keyphrase}", "cust_ltv", "Lifetime spend.")).toBe(
            "cust ltv: Lifetime spend.",
         );
         expect(renderKeyphrase("{keyphrase}", "x", "Only this.")).toBe("Only this.");
      });
   });
});
