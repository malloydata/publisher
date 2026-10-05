// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { embeddingStartupNotices } from "./config";
import type { RetrievalEmbeddingConfig } from "./retrieval_config";

/** The block as the parser returns it: the prefixes are always present. */
const run = (file?: Partial<RetrievalEmbeddingConfig>) =>
   embeddingStartupNotices(
      file && ({ queryPrefix: "", documentPrefix: "", ...file } as never),
   );

const VARS = [
   "EMBEDDING_API_KEY",
   "EMBEDDING_API_BASE",
   "EMBEDDING_MODEL",
   "EMBEDDING_DIMENSIONS",
] as const;

describe("embeddingStartupNotices", () => {
   const saved: Record<string, string | undefined> = {};
   beforeEach(() => {
      for (const v of VARS) {
         saved[v] = process.env[v];
         delete process.env[v];
      }
   });
   afterEach(() => {
      for (const v of VARS) {
         if (saved[v] === undefined) delete process.env[v];
         else process.env[v] = saved[v];
      }
   });

   for (const provider of ["openai", "openai-compatible", "google"] as const) {
      it(`warns when provider ${provider} is set and EMBEDDING_API_KEY is not`, () => {
         const notices = run({ provider, model: "m" });
         expect(notices).toHaveLength(1);
         expect(notices[0].level).toBe("warn");
         expect(notices[0].message).toContain(
            `retrieval.embedding names provider "${provider}" but EMBEDDING_API_KEY is not set`,
         );
         expect(notices[0].message).toContain("lexical");
         expect(notices[0].message).toContain("Fix: set EMBEDDING_API_KEY");
      });
   }

   it("does not warn when the key is set", () => {
      process.env.EMBEDDING_API_KEY = "k";
      expect(run({ provider: "openai", model: "m" })).toEqual([]);
   });

   it("does not warn for providers that need no key", () => {
      expect(run({ provider: "ollama", model: "m" })).toEqual([]);
      expect(run({ provider: "vertex", model: "m" })).toEqual([]);
   });

   it("says nothing with no embedding block", () => {
      expect(run(undefined)).toEqual([]);
   });

   it("says when the file overrides a different EMBEDDING_MODEL and that a model change re-embeds", () => {
      process.env.EMBEDDING_API_KEY = "k";
      process.env.EMBEDDING_MODEL = "env-model";
      const notices = run({
         provider: "openai",
         model: "file-model",
      });
      expect(notices).toHaveLength(1);
      expect(notices[0].level).toBe("info");
      expect(notices[0].message).toContain(
         'retrieval.embedding.model "file-model" in publisher.config.json overrides EMBEDDING_MODEL "env-model"',
      );
      expect(notices[0].message).toContain("re-embed");
   });

   it("says the same for dimensions, and the host only for a base URL", () => {
      process.env.EMBEDDING_API_KEY = "k";
      process.env.EMBEDDING_DIMENSIONS = "512";
      process.env.EMBEDDING_API_BASE = "https://user:pw@env.example.com/v1/x";
      const notices = run({
         provider: "openai-compatible",
         model: "m",
         dimensions: 1024,
         baseUrl: "https://file.example.com/v1",
      });
      const text = notices.map((n) => n.message).join("\n");
      expect(text).toContain(
         "retrieval.embedding.dimensions 1024 in publisher.config.json overrides EMBEDDING_DIMENSIONS 512",
      );
      expect(text).toContain(
         'retrieval.embedding.baseUrl (host "file.example.com") in publisher.config.json overrides EMBEDDING_API_BASE (host "env.example.com")',
      );
      // Never the credentials in the URL, nor its path.
      expect(text).not.toContain("pw");
      expect(text).not.toContain("/v1/x");
   });

   it("says nothing when the file and the variable agree, or the variable is unset", () => {
      process.env.EMBEDDING_API_KEY = "k";
      expect(run({ provider: "openai", model: "m" })).toEqual([]);
      process.env.EMBEDDING_MODEL = "m";
      process.env.EMBEDDING_DIMENSIONS = "512";
      expect(
         run({
            provider: "openai",
            model: "m",
            dimensions: 512,
         }),
      ).toEqual([]);
   });
});
