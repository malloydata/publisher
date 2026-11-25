// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A store built outside the composition root, which never calls start(), must
// still initialize on first use rather than leave every lookup pending.
import { afterEach, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { EnvironmentStore } from "./environment_store";
import { installSettings } from "../test_helpers/settings_env";

let root: string | undefined;
let store: EnvironmentStore | undefined;
let restoreSettings: (() => void) | undefined;

afterEach(async () => {
   if (store) {
      await (
         store as unknown as { storageManager: { close(): Promise<void> } }
      ).storageManager.close();
   }
   restoreSettings?.();
   if (root) fs.rmSync(root, { recursive: true, force: true });
   store = undefined;
   restoreSettings = undefined;
   root = undefined;
});

it("initializes on first use when nothing called start()", async () => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "lazy-start-")),
   );
   restoreSettings = installSettings({ cwd: root }).restore;
   store = new EnvironmentStore(root);

   const pending = Symbol("pending");
   const answer = await Promise.race([
      store.listEnvironments(),
      new Promise((resolve) => setTimeout(() => resolve(pending), 10_000)),
   ]);
   expect(answer).not.toBe(pending);
});
