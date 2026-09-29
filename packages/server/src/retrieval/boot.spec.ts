// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { LlmConfig } from "../config";
import {
   checkRetrievalAgainstEnvironment,
   enabledStages,
   llmAvailable,
   resolveStageModel,
} from "./boot";
import { resolveRetrievalConfig } from "./retrieval_config";

const ollama: LlmConfig = { apiKey: "", baseUrl: "http://localhost:11434/v1" };
const openai: LlmConfig = {
   apiKey: "k",
   baseUrl: "https://api.openai.com/v1",
   model: "gpt-4o-mini",
};

describe("retrieval boot checks", () => {
   it("is quiet for the defaults", () => {
      const c = resolveRetrievalConfig(undefined);
      expect(checkRetrievalAgainstEnvironment(c, null, false)).toEqual({
         errors: [],
         warnings: [],
      });
      expect(enabledStages(c)).toEqual([]);
   });

   it("warns, not fails, when a stage is enabled with no LLM", () => {
      const c = resolveRetrievalConfig({ refine: { enabled: true } });
      const r = checkRetrievalAgainstEnvironment(c, null, true);
      expect(r.errors).toEqual([]);
      expect(r.warnings[0]).toContain("refine");
      expect(r.warnings[0]).toContain("no LLM is available");
   });

   it("fails when a stage will run with no model to call", () => {
      const c = resolveRetrievalConfig({ refine: { enabled: true } });
      const r = checkRetrievalAgainstEnvironment(c, ollama, true);
      expect(r.errors).toHaveLength(1);
      expect(r.errors[0]).toContain("retrieval.llm.models.refine");
      expect(r.errors[0]).toContain("LLM_MODEL=");
   });

   it("passes when the model comes from any of the three places", () => {
      const stage = { refine: { enabled: true } };
      expect(
         checkRetrievalAgainstEnvironment(
            resolveRetrievalConfig({ ...stage, llm: { model: "m" } }),
            ollama,
            true,
         ).errors,
      ).toEqual([]);
      expect(
         checkRetrievalAgainstEnvironment(
            resolveRetrievalConfig({
               ...stage,
               llm: { models: { refine: "m" } },
            }),
            ollama,
            true,
         ).errors,
      ).toEqual([]);
      expect(
         checkRetrievalAgainstEnvironment(
            resolveRetrievalConfig(stage),
            { ...ollama, model: "m" },
            true,
         ).errors,
      ).toEqual([]);
   });

   it("prefers the stage model over the shared one over the env's", () => {
      const c = resolveRetrievalConfig({
         llm: { model: "shared", models: { rerank: "big" } },
      });
      expect(resolveStageModel(c, openai, "rerank")).toBe("big");
      expect(resolveStageModel(c, openai, "refine")).toBe("shared");
      expect(
         resolveStageModel(resolveRetrievalConfig(undefined), openai, "refine"),
      ).toBe("gpt-4o-mini");
   });

   it("treats llm.enabled=false as no LLM even when configured", () => {
      const c = resolveRetrievalConfig({
         llm: { enabled: false },
         refine: { enabled: true },
      });
      expect(llmAvailable(c, openai)).toBe(false);
      const r = checkRetrievalAgainstEnvironment(c, openai, true);
      expect(r.errors).toEqual([]);
      expect(r.warnings).toHaveLength(1);
   });

   it("warns that enrichment needs embeddings", () => {
      const c = resolveRetrievalConfig({ enrichment: { enabled: true } });
      const r = checkRetrievalAgainstEnvironment(c, openai, false);
      expect(r.warnings.join(" ")).toContain("needs embeddings");
   });

   it("lists exactly the stages that are switched on", () => {
      const c = resolveRetrievalConfig({
         refine: { enabled: true },
         enrichment: { enabled: true, sourceSummary: { enabled: true } },
      });
      expect([...enabledStages(c)].sort() as string[]).toEqual([
         "keyphrase",
         "refine",
         "summary",
      ]);
   });
});
