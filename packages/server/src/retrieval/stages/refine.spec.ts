// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The refine stage on hand-built rows: the parts a handler-level test cannot
// reach cheaply (two targets, one entity at several model paths, join damping).

import { describe, expect, it } from "bun:test";
import { LlmBudget, LlmRunner, runnerSettingsFrom } from "../../service/llm_runner";
import type { LlmProvider, LlmRequest } from "../../service/llm_provider";
import { resolveEgress, resolveRetrievalConfig } from "../retrieval_config";
import type { RunLlm } from "../run";
import type { StageRow } from "../stage_types";
import { runRefine } from "./refine";

const row = (
   name: string,
   scores: Record<number, number>,
   extra: Partial<StageRow> = {},
): StageRow => ({
   kind: "measure",
   name,
   source: "orders",
   modelPath: "m.malloy",
   embedDoc: `${name} doc`,
   candidateScores: new Map(Object.entries(scores).map(([t, s]) => [Number(t), s])),
   rankScore: Math.max(...Object.values(scores)),
   ...extra,
});

function llmFor(
   reply: (req: LlmRequest) => string,
   seen: LlmRequest[] = [],
   config = resolveRetrievalConfig({ llm: { model: "m", cache: { enabled: false } } }),
): RunLlm {
   const provider: LlmProvider = {
      id: "fake",
      async complete(req) {
         seen.push(req);
         return { text: reply(req), model: req.model, latencyMs: 1 };
      },
   };
   return {
      runner: new LlmRunner(provider, runnerSettingsFrom(config.llm)),
      budget: new LlmBudget(50, 60_000),
      model: undefined,
   };
}

const idxOf = (req: LlmRequest, name: string) =>
   Number(req.user.match(new RegExp(`- \\[(\\d+)\\] ${name} \\(`))![1]);

const run = (
   rows: StageRow[],
   searches: Array<{ targetIndex: number; text: string }>,
   llm: RunLlm,
   config = resolveRetrievalConfig({ refine: { enabled: true }, llm: { model: "m" } }),
) =>
   runRefine({
      rows,
      searches,
      config,
      llm,
      lexical: false,
      egress: resolveEgress(config),
   });

describe("refine.concurrency", () => {
   /** The most batches ever in flight at once, for six one-row batches. */
   async function peak(refineConcurrency: number | null): Promise<number> {
      const config = resolveRetrievalConfig({
         refine: { enabled: true, batchSize: 1, concurrency: refineConcurrency },
         llm: { model: "m", concurrency: 6, cache: { enabled: false } },
      });
      let inFlight = 0;
      let most = 0;
      const provider: LlmProvider = {
         id: "slow",
         async complete(req) {
            inFlight++;
            most = Math.max(most, inFlight);
            await new Promise((r) => setTimeout(r, 15));
            inFlight--;
            return { text: "[]", model: req.model, latencyMs: 15 };
         },
      };
      const llm: RunLlm = {
         runner: new LlmRunner(provider, runnerSettingsFrom(config.llm)),
         budget: new LlmBudget(50, 60_000),
         model: undefined,
      };
      const rows = ["a", "b", "c", "d", "e", "f"].map((n) => row(`m_${n}`, { 0: 0.5 }));
      await runRefine({
         rows,
         searches: [{ targetIndex: 0, text: "x" }],
         config,
         llm,
         lexical: false,
         egress: resolveEgress(config),
      });
      return most;
   }

   it("caps the batches in flight below the process-wide llm.concurrency", async () => {
      expect(await peak(2)).toBe(2);
      expect(await peak(1)).toBe(1);
   });

   it("leaves the batches to llm.concurrency when it is null", async () => {
      expect(await peak(null)).toBe(6);
   });
});

describe("runRefine on hand-built rows", () => {
   it("rates each target's candidates separately and keeps a row for the targets that kept it", async () => {
      const seen: LlmRequest[] = [];
      const rows = [
         row("revenue_total", { 0: 0.9, 1: 0.5 }),
         row("customer_count", { 1: 0.8 }),
      ];
      const llm = llmFor((req) => {
         const phrase = req.user.match(/PHRASE:\nText: "([^"]+)"/)![1];
         return phrase === "revenue"
            ? JSON.stringify([{ index: idxOf(req, "revenue_total"), score: "HIGH", reason: "a" }])
            : JSON.stringify([
                 { index: idxOf(req, "customer_count"), score: "HIGH", reason: "b" },
                 { index: idxOf(req, "revenue_total"), score: "LOW", reason: "c" },
              ]);
      }, seen);
      const out = await run(
         rows,
         [
            { targetIndex: 0, text: "revenue" },
            { targetIndex: 1, text: "customers" },
         ],
         llm,
      );
      expect(out.status).toBe("ok");
      expect(seen).toHaveLength(2); // one call per target
      const byName = new Map(out.rows.map((r) => [r.name, r]));
      // revenue_total: HIGH for target 0, LOW (pruned) for target 1.
      expect([...byName.get("revenue_total")!.targetScores!.keys()]).toEqual([0]);
      expect(byName.get("revenue_total")!.matchReasons!.get(0)).toBe("a");
      expect(byName.get("revenue_total")!.bestTarget).toBe(0);
      expect([...byName.get("customer_count")!.targetScores!.keys()]).toEqual([1]);
      expect(out.dropped).toEqual({});
   });

   it("gives every model path of one entity a single verdict from a single line", async () => {
      const seen: LlmRequest[] = [];
      const rows = [
         row("revenue_total", { 0: 0.9 }, { modelPath: "a.malloy" }),
         row("revenue_total", { 0: 0.9 }, { modelPath: "b.malloy" }),
      ];
      const llm = llmFor(
         (req) =>
            JSON.stringify([{ index: idxOf(req, "revenue_total"), score: "HIGH", reason: "x" }]),
         seen,
      );
      const out = await run(rows, [{ targetIndex: 0, text: "revenue" }], llm);
      expect(seen[0].user.match(/- \[\d+\]/g)).toHaveLength(1);
      expect(out.rows.map((r) => r.modelPath)).toEqual(["a.malloy", "b.malloy"]);
      expect(out.rows[0].score).toBe(out.rows[1].score);
   });

   it("damps only the similarity of a joined field, never its level", async () => {
      const config = resolveRetrievalConfig({
         refine: { enabled: true, minLevel: "LOW" },
         llm: { model: "m" },
         scoring: { joinDepthDamping: 0.5 },
      });
      const rows = [
         row("own", { 0: 0.8 }),
         row("customer.joined", { 0: 0.8 }, { joinPath: "customer" }),
         row("a.b.deep", { 0: 0.8 }, { joinPath: "a.b" }),
      ];
      const llm = llmFor(
         (req) =>
            JSON.stringify(
               ["own", "customer.joined", "a.b.deep"].map((n) => ({
                  index: idxOf(req, n.replace(/\./g, "\\.")),
                  score: "HIGH",
                  reason: "r",
               })),
            ),
         [],
         config,
      );
      const out = await run(rows, [{ targetIndex: 0, text: "x" }], llm, config);
      const raw = Object.fromEntries(out.rows.map((r) => [r.name, r.rankScore]));
      // level HIGH = 3, plus similarity 0.8 discounted 0.5 per hop.
      expect(raw["own"]).toBeCloseTo(3.8, 6);
      expect(raw["customer.joined"]).toBeCloseTo(3 + 0.8 * 0.5, 6);
      expect(raw["a.b.deep"]).toBeCloseTo(3 + 0.8 * 0.25, 6);
      // Still HIGH: a joined field never falls a whole level.
      expect(Math.min(...Object.values(raw).map((v) => v as number))).toBeGreaterThan(3);
      expect(out.rows.map((r) => r.name)).toEqual(["own", "customer.joined", "a.b.deep"]);
   });

   it("does not damp at all by default", async () => {
      const rows = [row("customer.joined", { 0: 0.8 }, { joinPath: "customer" })];
      const llm = llmFor((req) =>
         JSON.stringify([{ index: idxOf(req, "customer\\.joined"), score: "HIGH", reason: "r" }]),
      );
      const out = await run(rows, [{ targetIndex: 0, text: "x" }], llm);
      expect(out.rows[0].rankScore).toBeCloseTo(3.8, 6);
   });

   it("skips when the egress names class is off", async () => {
      const config = resolveRetrievalConfig({
         refine: { enabled: true },
         llm: { model: "m" },
         egress: { names: false },
      });
      const out = await run(
         [row("a", { 0: 0.5 })],
         [{ targetIndex: 0, text: "x" }],
         llmFor(() => "[]"),
         config,
      );
      expect(out.status).toBe("skipped:egress");
   });

   it("truncates a long description to descChars", async () => {
      const seen: LlmRequest[] = [];
      const config = resolveRetrievalConfig({
         refine: { enabled: true, descChars: 10 },
         llm: { model: "m" },
      });
      await run(
         [row("a", { 0: 0.5 }, { embedDoc: "0123456789ABCDEF" })],
         [{ targetIndex: 0, text: "x" }],
         llmFor(() => "[]", seen),
         config,
      );
      expect(seen[0].user).toContain("(measure, source: orders): 0123456789…");
      expect(seen[0].user).not.toContain("ABCDEF");
   });

   it("uses a keyphrase when there is no doc", async () => {
      const seen: LlmRequest[] = [];
      await run(
         [row("a", { 0: 0.5 }, { embedDoc: "", keyphrase: "total order revenue" })],
         [{ targetIndex: 0, text: "x" }],
         llmFor(() => "[]", seen),
      );
      expect(seen[0].user).toContain("): total order revenue");
   });
});
