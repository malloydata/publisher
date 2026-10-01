// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// What the rerank prompt carries: source docs, the generated source summary,
// and the values of a value-indexed dimension, each behind its own egress class.

import { describe, expect, it } from "bun:test";
import {
   LlmBudget,
   LlmRunner,
   runnerSettingsFrom,
} from "../../service/llm_runner";
import type { LlmProvider, LlmRequest } from "../../service/llm_provider";
import { resolveEgress, resolveRetrievalConfig } from "../retrieval_config";
import type { RunLlm } from "../run";
import type { StageRow } from "../stage_types";
import { runRerank } from "./rerank";

const row = (
   source: string,
   name: string,
   score: number,
   extra: Partial<StageRow> = {},
): StageRow => ({
   kind: "dimension",
   name,
   source,
   modelPath: "m.malloy",
   embedDoc: `${name} doc`,
   candidateScores: new Map([[0, score]]),
   rankScore: score,
   ...extra,
});

const ROWS = [
   row("customers", "tier", 0.9, {
      values: [
         { value: "Premium" },
         { value: "Basic" },
         { value: "Enterprise" },
      ],
   }),
   row("orders", "status", 0.5),
];

async function promptFor(over: Record<string, unknown>): Promise<string> {
   const config = resolveRetrievalConfig({
      rerank: {
         enabled: true,
         skipIfAtMost: 0,
         ...((over.rerank as object) ?? {}),
      },
      egress: over.egress ?? {},
      llm: { model: "m", cache: { enabled: false } },
   });
   const seen: LlmRequest[] = [];
   const provider: LlmProvider = {
      id: "fake",
      async complete(req) {
         seen.push(req);
         return {
            text: JSON.stringify([
               { source: "customers", index: 0, score: 3 },
               { source: "orders", index: 1, score: 1 },
            ]),
            model: req.model,
            latencyMs: 1,
         };
      },
   };
   const llm: RunLlm = {
      runner: new LlmRunner(provider, runnerSettingsFrom(config.llm)),
      budget: new LlmBudget(10, 60_000),
      model: undefined,
   };
   await runRerank({
      rows: ROWS,
      searches: [{ targetIndex: 0, text: "premium customers" }],
      config,
      llm,
      lexical: false,
      egress: resolveEgress(config),
      packageName: "p",
      docsFor: (key) =>
         key.includes("customers") ? "Customer accounts." : undefined,
      summaryFor: (source) =>
         source === "customers" ? "Generated summary of customers." : undefined,
      scoredByLlm: false,
   });
   return seen[0].user;
}

describe("the rerank prompt", () => {
   it("carries the source doc and the generated summary", async () => {
      const prompt = await promptFor({});
      expect(prompt).toContain("Description: Customer accounts.");
      expect(prompt).toContain("Summary: Generated summary of customers.");
   });

   it("leaves the summary out with the docs class off, since it is written from the docs", async () => {
      const prompt = await promptFor({ egress: { docs: false } });
      expect(prompt).not.toContain("Generated summary of customers.");
      expect(prompt).not.toContain("Customer accounts.");
   });

   it("keeps dimension values out by default: they are customer data", async () => {
      const prompt = await promptFor({});
      expect(prompt).not.toContain("Premium");
   });

   it("shows the values with the dimensionalValues class on, best first, capped", async () => {
      const prompt = await promptFor({ egress: { dimensionalValues: true } });
      expect(prompt).toContain("Values: Premium, Basic, Enterprise");
      const capped = await promptFor({
         egress: { dimensionalValues: true },
         rerank: { valuesPerEntity: 2 },
      });
      expect(capped).toContain("Values: Premium, Basic");
      expect(capped).not.toContain("Enterprise");
   });

   it("shows no values when valuesPerEntity is 0, even with the class on", async () => {
      const prompt = await promptFor({
         egress: { dimensionalValues: true },
         rerank: { valuesPerEntity: 0 },
      });
      expect(prompt).not.toContain("Premium");
   });
});
