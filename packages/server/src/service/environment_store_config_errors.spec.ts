// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdirSync, rmSync, writeFileSync } from "fs";
import * as os from "os";
import * as path from "path";
import { PublisherConfigError } from "../errors";
import { withSettings, type SettingsInput } from "../test_helpers/settings_env";
import { EnvironmentStore } from "./environment_store";

/**
 * A config file Publisher cannot honour must be raised, not absorbed.
 *
 * The absorbed form returned an empty manifest, so one typo produced a server
 * that printed PUBLISHER_READY, reported "serving" with an empty loadErrors,
 * and served nothing. loadErrors cannot carry this failure -- it is keyed by
 * environment, and a config that will not parse yields no environments to key
 * by -- so the manifest read is where it has to surface.
 */
describe("reloadEnvironmentManifest on an unreadable config", () => {
   let root: string;

   beforeEach(() => {
      root = path.join(os.tmpdir(), `cfg-err-${Date.now()}-${Math.random()}`);
      mkdirSync(root, { recursive: true });
   });

   afterEach(() => {
      rmSync(root, { recursive: true, force: true });
      delete process.env.CFG_TEST_LOCATION;
   });

   const writeConfig = (body: string) =>
      writeFileSync(path.join(root, "publisher.config.json"), body);

   it("raises on malformed JSON, naming the file and the cause", async () => {
      writeConfig('{ "environments": [ }');
      const call = EnvironmentStore.reloadEnvironmentManifest(root);
      await expect(call).rejects.toThrow(PublisherConfigError);
      await expect(call).rejects.toThrow(/publisher\.config\.json/);
      // The remedy, because the alternative is an operator re-reading a file
      // that looks fine to them.
      await expect(call).rejects.toThrow(/Fix the file, or move it aside/);
   });

   it("raises on a ${VAR} reference to an unset variable, naming the variable", async () => {
      writeConfig(
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: "e",
                  packages: [{ name: "p", location: "${CFG_TEST_LOCATION}/p" }],
                  connections: [],
               },
            ],
         }),
      );
      const call = EnvironmentStore.reloadEnvironmentManifest(root);
      await expect(call).rejects.toThrow(PublisherConfigError);
      await expect(call).rejects.toThrow(/CFG_TEST_LOCATION/);
   });

   it("substitutes a ${VAR} that IS set", async () => {
      process.env.CFG_TEST_LOCATION = "/tmp/somewhere";
      writeConfig(
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: "e",
                  packages: [{ name: "p", location: "${CFG_TEST_LOCATION}/p" }],
                  connections: [],
               },
            ],
         }),
      );
      const manifest = await EnvironmentStore.reloadEnvironmentManifest(root);
      expect(manifest.environments[0].packages[0].location).toBe(
         "/tmp/somewhere/p",
      );
   });

   describe("a retrieval block that does not validate", () => {
      const anthropic = {
         environments: [],
         retrieval: { llm: { provider: "anthropic", model: "m" } },
      };
      const reload = (input: SettingsInput) =>
         withSettings(input, async ({ serverRoot }) => {
            try {
               await EnvironmentStore.reloadEnvironmentManifest(serverRoot);
            } catch (error) {
               return error;
            }
            throw new Error("expected the reload to fail");
         });

      it("names the variable that makes it fail as the fix, since moving the file aside leaves it set", async () => {
         const error = await reload({
            env: { PUBLISHER_RETRIEVAL_LLM_PROVIDER: "vertex" },
            file: anthropic,
         });
         expect(error).toBeInstanceOf(PublisherConfigError);
         const message = (error as Error).message;
         expect(message).toStartWith("The retrieval settings are invalid: ");
         expect(message).toEndWith(
            ". Fix PUBLISHER_RETRIEVAL_LLM_PROVIDER, or the file; moving the file aside leaves it set.",
         );
         expect(message).not.toContain("move it aside to fall back");
         expect(message).not.toContain("..");
      });

      it("names a flag the same way", async () => {
         const error = await reload({
            argv: ["--retrieval-llm-provider", "vertex"],
            file: anthropic,
         });
         expect((error as Error).message).toEndWith(
            ". Fix --retrieval-llm-provider, or the file; moving the file aside leaves it set.",
         );
      });

      it("blames the file when the file alone gives it", async () => {
         const error = await reload({
            file: {
               environments: [],
               retrieval: { llm: { provider: "vertex", model: "m" } },
            },
         });
         const message = (error as Error).message;
         expect(message).toStartWith("Could not read publisher.config.json: ");
         expect(message).toEndWith(
            ". Fix the file, or move it aside to fall back to the bundled default.",
         );
         expect(message).not.toContain("..");
      });
   });

   it("does NOT raise when the config file is absent", async () => {
      // Absent is not an operator error: Publisher falls back to the bundled
      // DuckDB-only default. This is the behaviour the raise must not take
      // with it, and it is the line between "no config" and "bad config".
      mkdirSync(path.join(root, "alpha"));
      const manifest = await EnvironmentStore.reloadEnvironmentManifest(root);
      expect(manifest.environments).toEqual([]);
   });
});
