// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { existsSync, mkdirSync, rmSync, writeFileSync } from "fs";
import * as path from "path";
import { getUnresolvedPublisherConfigPath } from "../config";
import { TEMP_DIR_PATH } from "../constants";
import { logger } from "../logger";
import { EnvironmentStore } from "./environment_store";

/**
 * A server that loads nothing reports `serving` with `environments=0
 * packages=0 load_errors=0`, and every one of those numbers is correct:
 * nothing failed because nothing was configured. Before this notice existed,
 * no output named the config path that had been checked, so a Docker image
 * booted without its config mount looked healthy and served an empty catalog.
 *
 * These specs assert REPORTING rather than resolution: the helper's return
 * value is easy to get right and still reach nobody. The parser-level cases
 * live in the first describe, the boot-path ones in the second.
 *
 * No module mocking here, deliberately. environment_store.spec.ts mocks
 * `../config` in a beforeEach, and these cases need the real resolver.
 */

const serverRootPath = path.join(TEMP_DIR_PATH, "unconfigured-spec-root");

const resetRoot = () => {
   if (existsSync(serverRootPath)) {
      rmSync(serverRootPath, { recursive: true, force: true });
   }
   mkdirSync(serverRootPath, { recursive: true });
};

describe("getUnresolvedPublisherConfigPath", () => {
   const priorConfigPath = process.env.PUBLISHER_CONFIG_PATH;

   beforeEach(() => {
      resetRoot();
      delete process.env.PUBLISHER_CONFIG_PATH;
   });

   afterEach(() => {
      rmSync(serverRootPath, { recursive: true, force: true });
      if (priorConfigPath === undefined) {
         delete process.env.PUBLISHER_CONFIG_PATH;
      } else {
         process.env.PUBLISHER_CONFIG_PATH = priorConfigPath;
      }
   });

   it("names the path it looked for when no config is present", () => {
      expect(getUnresolvedPublisherConfigPath(serverRootPath)).toBe(
         path.join(serverRootPath, "publisher.config.json"),
      );
   });

   it("returns null once a config exists at the server root", () => {
      writeFileSync(
         path.join(serverRootPath, "publisher.config.json"),
         JSON.stringify({ frozenConfig: false, environments: [] }),
      );
      expect(getUnresolvedPublisherConfigPath(serverRootPath)).toBeNull();
   });

   it("stays quiet for an explicit --config, even a missing one", () => {
      // getPublisherConfig already logs that case at error. Reporting the same
      // mistake twice in two different shapes helps nobody.
      process.env.PUBLISHER_CONFIG_PATH = path.join(
         serverRootPath,
         "nowhere.json",
      );
      expect(getUnresolvedPublisherConfigPath(serverRootPath)).toBeNull();
   });
});

describe("unconfigured boot notice", () => {
   let infoSpy: ReturnType<typeof spyOn>;
   let infoLines: string[];

   const noticeLines = () =>
      infoLines.filter((line) => line.includes("Serving with no environments"));

   beforeEach(() => {
      resetRoot();
      delete process.env.PUBLISHER_CONFIG_PATH;
      infoLines = [];
      infoSpy = spyOn(logger, "info").mockImplementation((message: unknown) => {
         infoLines.push(String(message));
         return logger;
      });
   });

   afterEach(() => {
      infoSpy.mockRestore();
      rmSync(serverRootPath, { recursive: true, force: true });
   });

   it("names the missing config once when nothing loaded", async () => {
      const store = new EnvironmentStore(serverRootPath);
      await store.finishedInitialization;

      const notices = noticeLines();
      expect(notices).toHaveLength(1);
      expect(notices[0]).toContain(
         path.join(serverRootPath, "publisher.config.json"),
      );
      // The two ways out, both of which a reader needs to act on it.
      expect(notices[0]).toContain("--config");
      expect(notices[0]).toContain("runtime");
   });

   it("can still create an environment at runtime, as the notice claims", async () => {
      // The notice, the code comment beside it and docs/deployment.md all tell
      // the reader that an unconfigured server is a supported way to run
      // because environments can be created afterwards. That claim rests on
      // `frozenConfig` defaulting to false when no config file exists, which
      // nothing else pins. If a future change made runtime creation require a
      // config file, all three would quietly become wrong.
      const store = new EnvironmentStore(serverRootPath);
      await store.finishedInitialization;
      expect(noticeLines()).toHaveLength(1);

      await store.addEnvironment({ name: "created-at-runtime" });

      const environments = await store.listEnvironments();
      expect(environments.map((environment) => environment.name)).toContain(
         "created-at-runtime",
      );
   });

   it("stays quiet when a config resolved, however empty", async () => {
      // An operator who configured no environments meant it. The notice is for
      // the reader who thinks they configured some and did not.
      writeFileSync(
         path.join(serverRootPath, "publisher.config.json"),
         JSON.stringify({ frozenConfig: false, environments: [] }),
      );

      const store = new EnvironmentStore(serverRootPath);
      await store.finishedInitialization;

      expect(noticeLines()).toHaveLength(0);
   });
});
