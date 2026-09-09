// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdirSync, rmSync, writeFileSync } from "fs";
import * as os from "os";
import * as path from "path";
import { PublisherConfigError } from "../errors";
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

   it("does NOT raise when the config file is absent", async () => {
      // Absent is not an operator error: Publisher falls back to the bundled
      // DuckDB-only default. This is the behaviour the raise must not take
      // with it, and it is the line between "no config" and "bad config".
      mkdirSync(path.join(root, "alpha"));
      const manifest = await EnvironmentStore.reloadEnvironmentManifest(root);
      expect(manifest.environments).toEqual([]);
   });
});
