// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { embeddingStartupNotices } from "./config";
import type { RetrievalEmbeddingConfig } from "./retrieval_config";
import { Settings } from "./settings";
import { withSettings } from "./test_helpers/settings_env";

/**
 * The notices for `file`, the block as the parser returns it (the prefixes
 * are always present), with `env` as the variables. The block is the settings'
 * config file too, as at startup: model, baseUrl and dimensions are read
 * through the settings, which keep what a variable the file overrides held.
 */
const run = (
   file?: Partial<RetrievalEmbeddingConfig>,
   env: Record<string, string> = {},
) =>
   withSettings({ env, file: file && { retrieval: { embedding: file } } }, () =>
      embeddingStartupNotices(
         file && ({ queryPrefix: "", documentPrefix: "", ...file } as never),
      ),
   );

describe("embeddingStartupNotices", () => {
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
      expect(
         run({ provider: "openai", model: "m" }, { EMBEDDING_API_KEY: "k" }),
      ).toEqual([]);
   });

   it("does not warn for providers that need no key", () => {
      expect(run({ provider: "ollama", model: "m" })).toEqual([]);
      expect(run({ provider: "vertex", model: "m" })).toEqual([]);
   });

   it("says nothing with no embedding block", () => {
      expect(run(undefined)).toEqual([]);
   });

   it("says when a variable overrides a different model in the file, and that a model change re-embeds", () => {
      const notices = run(
         {
            provider: "openai",
            model: "file-model",
         },
         {
            EMBEDDING_API_KEY: "k",
            PUBLISHER_RETRIEVAL_EMBEDDING_MODEL: "env-model",
         },
      );
      expect(notices).toHaveLength(1);
      expect(notices[0].level).toBe("info");
      expect(notices[0].message).toBe(
         'PUBLISHER_RETRIEVAL_EMBEDDING_MODEL "env-model" overrides retrieval.embedding.model "file-model" in publisher.config.json. Changing the model or its dimensions re-embeds every package.',
      );
   });

   it("says the same for dimensions, and the host only for a base URL", () => {
      const notices = run(
         {
            provider: "openai-compatible",
            model: "m",
            dimensions: 1024,
            baseUrl: "https://file.example.com/v1",
         },
         {
            EMBEDDING_API_KEY: "k",
            PUBLISHER_RETRIEVAL_EMBEDDING_DIMENSIONS: "512",
            PUBLISHER_RETRIEVAL_EMBEDDING_BASE_URL:
               "https://user:pw@env.example.com/v1/x",
         },
      );
      const text = notices.map((n) => n.message).join("\n");
      expect(text).toContain(
         "PUBLISHER_RETRIEVAL_EMBEDDING_DIMENSIONS 512 overrides retrieval.embedding.dimensions 1024 in publisher.config.json",
      );
      expect(text).toContain(
         'PUBLISHER_RETRIEVAL_EMBEDDING_BASE_URL (host "env.example.com") overrides retrieval.embedding.baseUrl (host "file.example.com") in publisher.config.json',
      );
      // Never the credentials in the URL, nor its path.
      expect(text).not.toContain("pw");
      expect(text).not.toContain("/v1/x");
   });

   it("has nothing to say for an old variable that differs from the file, which fails startup instead", () => {
      withSettings(
         {
            env: { EMBEDDING_API_KEY: "k", EMBEDDING_MODEL: "env-model" },
            file: {
               retrieval: {
                  embedding: { provider: "openai", model: "file-model" },
               },
            },
         },
         () => {
            expect(
               Settings.current()
                  .errors()
                  .map((error) => error.message),
            ).toEqual([
               expect.stringContaining(
                  "retrieval.embedding.model is given different values",
               ) as unknown as string,
            ]);
         },
      );
   });

   it("says nothing when the file and a variable agree, or no variable is set", () => {
      expect(
         run({ provider: "openai", model: "m" }, { EMBEDDING_API_KEY: "k" }),
      ).toEqual([]);
      expect(
         run(
            {
               provider: "openai",
               model: "m",
               dimensions: 512,
            },
            {
               EMBEDDING_API_KEY: "k",
               PUBLISHER_RETRIEVAL_EMBEDDING_MODEL: "m",
               PUBLISHER_RETRIEVAL_EMBEDDING_DIMENSIONS: "512",
            },
         ),
      ).toEqual([]);
   });
});
