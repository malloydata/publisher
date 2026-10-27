// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import fs from "fs";
import os from "os";
import path from "path";
import { PUBLISHER_CONFIG_NAME } from "../constants";
import { Settings } from "../settings";

/**
 * One startup's worth of settings sources: variables, flags and the config
 * file. `file` is written to a fresh server root's `publisher.config.json`, as
 * JSON or, given a string, as raw text (so a case can be unreadable). `cwd`
 * points the settings at a server root a spec already made instead.
 */
export interface SettingsInput {
   env?: Record<string, string>;
   argv?: string[];
   file?: Record<string, unknown> | string;
   cwd?: string;
}

export interface SettingsContext {
   /** The directory the settings resolve `root` against: the one holding `file`. */
   serverRoot: string;
}

/**
 * Make the settings `input` describes the current ones, as `server.ts` does at
 * startup, and return what puts the previous ones back. The variables are only
 * these (and this process's NODE_ENV, which tests rely on), never the
 * developer's shell. For a `beforeEach` / `afterEach` pair.
 */
export function installSettings(input: SettingsInput = {}): {
   serverRoot: string;
   restore: () => void;
} {
   const ownRoot = input.cwd === undefined;
   const serverRoot =
      input.cwd ?? fs.mkdtempSync(path.join(os.tmpdir(), "settings-"));
   if (input.file !== undefined) {
      fs.writeFileSync(
         path.join(serverRoot, PUBLISHER_CONFIG_NAME),
         typeof input.file === "string"
            ? input.file
            : JSON.stringify(input.file),
      );
   }
   const previous = Settings.install(
      Settings.load({
         argv: input.argv ?? [],
         env: { NODE_ENV: process.env.NODE_ENV, ...input.env },
         cwd: serverRoot,
      }),
   );
   return {
      serverRoot,
      restore: () => {
         Settings.install(previous);
         if (ownRoot) fs.rmSync(serverRoot, { recursive: true, force: true });
      },
   };
}

/**
 * Run `fn` with the settings `input` describes, then put the previous ones
 * back, after `fn`'s promise settles when it returns one.
 */
export function withSettings<T>(
   input: SettingsInput,
   fn: (context: SettingsContext) => T,
): T {
   const { serverRoot, restore } = installSettings(input);
   let result: T;
   try {
      result = fn({ serverRoot });
   } catch (error) {
      restore();
      throw error;
   }
   if (result instanceof Promise) {
      return result.finally(restore) as T;
   }
   restore();
   return result;
}
