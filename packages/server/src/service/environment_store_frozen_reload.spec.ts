// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// `frozenConfig` refuses writes, not reads. A GET that loads or reloads an
// environment from the config (`?reload=true`) re-reads what the config already
// says, so it must answer; a create or PATCH over the API must still be refused.
import { afterEach, beforeEach, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { FrozenConfigError } from "../errors";
import { installSettings } from "../test_helpers/settings_env";
import { startedStore } from "../test_helpers/started_store";
import { EnvironmentStore } from "./environment_store";
import { parseEnvironmentName } from "./identity";

const ENV = "frozen_env";

let root: string;
let store: EnvironmentStore | undefined;
let restoreSettings: (() => void) | undefined;

beforeEach(() => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "env-frozen-reload-")),
   );
   fs.mkdirSync(path.join(root, "server"));
});

afterEach(async () => {
   await closeStore();
   restoreSettings?.();
   restoreSettings = undefined;
   fs.rmSync(root, { recursive: true, force: true });
});

async function openStore(frozen: boolean): Promise<EnvironmentStore> {
   restoreSettings?.();
   restoreSettings = installSettings({
      cwd: path.join(root, "server"),
      ...(frozen ? { file: { frozenConfig: true, environments: [] } } : {}),
   }).restore;
   store = startedStore(new EnvironmentStore(path.join(root, "server")));
   await store.finishedInitialization;
   return store;
}

async function closeStore() {
   if (!store) return;
   const current = store;
   store = undefined;
   for (const env of current.getLoadedEnvironments()) {
      await env.closeAllConnections();
   }
   await (
      current as unknown as { storageManager: { close(): Promise<void> } }
   ).storageManager.close();
}

it("answers a GET that reloads an environment under frozenConfig, and still refuses a create", async () => {
   // Create the environment on an unfrozen server, then restart frozen.
   const first = await openStore(false);
   await first.addEnvironment({ name: ENV, connections: [] } as never);
   await closeStore();

   const frozen = await openStore(true);
   expect(frozen.publisherConfigIsFrozen).toBe(true);

   const reloaded = await frozen.getEnvironment(
      parseEnvironmentName(ENV),
      true,
   );
   expect(reloaded.metadata.name).toBe(ENV);

   await expect(
      frozen.addEnvironment({ name: "created_over_api" } as never),
   ).rejects.toBeInstanceOf(FrozenConfigError);
});
