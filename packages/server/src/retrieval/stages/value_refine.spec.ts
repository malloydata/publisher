// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The value refine stage on hand-built hits: the caps, the batches, what the
// model is shown, what a level does to a score, and how it fails.

import { describe, expect, it } from "bun:test";
import { LlmError, type LlmProvider, type LlmRequest } from "../../service/llm_provider";
import { LlmBudget, LlmRunner, runnerSettingsFrom } from "../../service/llm_runner";
import type { ValueHit } from "../dim_values";
import { resolveEgress, resolveRetrievalConfig } from "../retrieval_config";
import type { RunLlm } from "../run";
import { runValueRefine } from "./value_refine";

const hit = (
   value: string,
   score: number,
   source = "customers",
   dimension = "tier",
   targetIndex = 0,
): ValueHit => ({ targetIndex, source, dimension, value, score, weight: 1 });

function setup(
   reply: (req: LlmRequest) => string | LlmError,
   over: Record<string, unknown> = {},
   seen: LlmRequest[] = [],
) {
   const config = resolveRetrievalConfig({
      dimensionalValues: { mode: "annotated", refine: { enabled: true, ...((over.refine as object) ?? {}) } },
      egress: { dimensionalValues: true, ...((over.egress as object) ?? {}) },
      llm: { model: "m", cache: { enabled: false }, backoffMs: 0 },
   });
   const provider: LlmProvider = {
      id: "fake",
      async complete(req) {
         seen.push(req);
         const out = reply(req);
         if (out instanceof LlmError) throw out;
         return { text: out, model: req.model, latencyMs: 1 };
      },
   };
   const llm: RunLlm = {
      runner: new LlmRunner(provider, runnerSettingsFrom(config.llm)),
      budget: new LlmBudget(100, 60_000),
      model: undefined,
   };
   return { config, llm, egress: resolveEgress(config) };
}

const idx = (req: LlmRequest, value: string) =>
   Number(req.user.match(new RegExp(`- \\[(\\d+)\\] ${value}:`))![1]);
const rate = (items: Array<[number, string]>) =>
   JSON.stringify(items.map(([index, score]) => ({ index, score })));
const run = (hits: ValueHit[], s: ReturnType<typeof setup>, text = "premium") =>
   runValueRefine({ hits, searches: [{ targetIndex: 0, text }], config: s.config, llm: s.llm, egress: s.egress });

describe("runValueRefine", () => {
   it("keeps what the model rates at or above minLevel, and drops the rest", async () => {
      const s = setup((req) =>
         rate([
            [idx(req, "Premium"), "HIGH"],
            [idx(req, "Prem"), "MEDIUM"],
            [idx(req, "Primer"), "LOW"],
         ]),
      );
      const out = await run([hit("Premium", 1), hit("Prem", 0.95), hit("Primer", 0.5), hit("Basic", 0.4)], s);
      expect(out.status).toBe("ok");
      expect(out.hits.map((h) => h.value)).toEqual(["Premium", "Prem"]);
      // Primer was rated LOW (under MEDIUM); Basic was left out.
      expect(out.dropped).toEqual({ below_min_level: 1, llm_omitted: 1 });
   });

   it("scores a kept value as its level plus its match score, through the knots", async () => {
      const s = setup((req) => rate([[idx(req, "Premium"), "HIGH"], [idx(req, "Prem"), "MEDIUM"]]));
      const out = await run([hit("Premium", 1), hit("Prem", 0.5)], s);
      // HIGH is 3 + 1.0 = 4.0 -> 1.0; MEDIUM is 2 + 0.5 = 2.5 -> 0.8.
      expect(out.hits.map((h) => h.score)).toEqual([1, 0.8]);
   });

   it("keeps a LOW value when minLevel is LOW", async () => {
      const s = setup((req) => rate([[idx(req, "Primer"), "LOW"]]), { refine: { minLevel: "LOW" } });
      const out = await run([hit("Primer", 0.5), hit("Basic", 0.4)], s);
      expect(out.hits.map((h) => h.value)).toEqual(["Primer"]);
   });

   it("shows the model the phrase and one line per value, and nothing else about the query", async () => {
      const seen: LlmRequest[] = [];
      const s = setup(() => "[]", {}, seen);
      await run([hit("Premium", 1, "customers", "tier"), hit("Basic", 0.4, "customers", "tier")], s, "premium");
      expect(seen).toHaveLength(1);
      expect(seen[0].user).toContain('PHRASE:\nText: "premium"');
      expect(seen[0].user).toContain("- [0] Premium: customers.tier");
      expect(seen[0].user).toContain("- [1] Basic: customers.tier");
      // The entity prompt carries a QUERY block; this one deliberately does not.
      expect(seen[0].user).not.toContain("QUERY:");
      expect(seen[0].stage).toBe("valueRefine");
   });

   it("sends at most maxPerSource values of a source, best first", async () => {
      const seen: LlmRequest[] = [];
      const s = setup(() => "[]", { refine: { maxPerSource: 2 } }, seen);
      const out = await run([hit("A", 0.9), hit("B", 0.8), hit("C", 0.7), hit("D", 0.6, "orders", "status")], s);
      const sent = seen.map((r) => r.user).join("\n");
      expect(sent).toContain("A: customers.tier");
      expect(sent).toContain("B: customers.tier");
      expect(sent).not.toContain("C: customers.tier");
      expect(sent).toContain("D: orders.status");
      expect(out.dropped.refine_cap).toBe(1);
   });

   it("sends at most maxCandidates values in all", async () => {
      const seen: LlmRequest[] = [];
      const s = setup(() => "[]", { refine: { maxCandidates: 3, maxPerSource: 10 } }, seen);
      const hits = ["A", "B", "C", "D", "E"].map((v, i) => hit(v, 0.9 - i * 0.1));
      await run(hits, s);
      const lines = seen.flatMap((r) => r.user.split("\n").filter((l) => /^- \[\d+\]/.test(l)));
      expect(lines).toHaveLength(3);
   });

   it("splits into batches of batchSize", async () => {
      const seen: LlmRequest[] = [];
      const s = setup(() => "[]", { refine: { batchSize: 2 } }, seen);
      await run(["A", "B", "C", "D", "E"].map((v, i) => hit(v, 0.9 - i * 0.1)), s);
      expect(seen).toHaveLength(3);
   });

   it("rates each target's values against its own phrase", async () => {
      const seen: LlmRequest[] = [];
      const s = setup(() => "[]", {}, seen);
      await runValueRefine({
         hits: [hit("Premium", 1, "customers", "tier", 0), hit("Paris", 1, "customers", "city", 1)],
         searches: [
            { targetIndex: 0, text: "premium" },
            { targetIndex: 1, text: "paris" },
         ],
         config: s.config,
         llm: s.llm,
         egress: s.egress,
      });
      const phrases = seen.map((r) => r.user.match(/Text: "([^"]+)"/)![1]).sort();
      expect(phrases).toEqual(["paris", "premium"]);
   });

   it("does not run without the dimensionalValues egress class: the values are customer data", async () => {
      const seen: LlmRequest[] = [];
      const s = setup(() => "[]", { egress: { dimensionalValues: false } }, seen);
      const out = await run([hit("Premium", 1)], s);
      expect(out.status).toBe("skipped:egress");
      expect(seen).toHaveLength(0);
      expect(out.hits).toHaveLength(1);
   });

   it("leaves the values as they were, and says so, when the model fails", async () => {
      const s = setup(() => new LlmError("boom", "http", false));
      const hits = [hit("Premium", 1), hit("Basic", 0.4)];
      const out = await run(hits, s);
      expect(out.status).toBe("failed:http");
      expect(out.hits).toEqual(hits);
      expect(out.warnings[0]).toContain("value refine unavailable");
   });

   it("keeps the values of a batch that failed, and rates the others", async () => {
      let n = 0;
      const s = setup(
         (req) => (n++ === 0 ? new LlmError("boom", "http", false) : rate([[idx(req, "C"), "HIGH"]])),
         { refine: { batchSize: 2 } },
      );
      const out = await run([hit("A", 0.9), hit("B", 0.8), hit("C", 0.7)], s);
      expect(out.status).toBe("partial:1/2");
      expect(out.hits.map((h) => h.value).sort()).toEqual(["A", "B", "C"]);
   });

   it("skips with nothing to rate", async () => {
      const s = setup(() => "[]");
      expect((await run([], s)).status).toBe("skipped:no_candidates");
   });
});
