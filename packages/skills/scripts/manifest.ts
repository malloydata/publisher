// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The manifest that decides which skills a Publisher deployment ships.
 *
 * Every channel resolves this file; none globs `skills/`. Before it existed,
 * the npm pack, the MCP prompt bundle, the `.claude/skills` symlinks, and the
 * scaffolder each took "everything under skills/ minus credible-*", so adding a
 * skill silently shipped it everywhere and there was nowhere to say otherwise.
 * A manifest is only worth having if it is the single answer to "what ships".
 * The pack and the bundle agree with it by construction, filtering on it to
 * choose what to copy; `manifest.spec.ts` asserts the two that could still
 * drift -- the manifest against `skills/`, and against the `.claude/skills`
 * symlinks.
 *
 * Build-time only, which is why this lives in scripts/ rather than src/. The
 * packed npm module cannot read the repo root, and does not need to: the pack
 * filters `skills/` down to the manifest, so what ships IS the manifest and
 * `listSkills()` can keep reading the directory.
 */
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

export interface SkillManifest {
   name: string;
   description: string;
   trigger_hint?: string;
   /** Skills a host places where it discovers them automatically. */
   auto_discovered: string[];
   /**
    * Skills a host keeps on-demand. Empty for Publisher, deliberately: agents
    * discover a second directory poorly, and an SDK Skill tool cannot invoke
    * from one at all.
    */
   supporting: string[];
   /**
    * Named subsets a consumer can exclude, so someone who wants fewer skills
    * gets a filter rather than a second directory. Every member must also
    * appear in auto_discovered or supporting.
    */
   groups?: Record<string, string[]>;
}

const packageDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));

/** Repo root, resolved from this file rather than from the process cwd. */
export const repoRoot: string = path.join(packageDir, "..", "..");

export const manifestPath: string = path.join(
   repoRoot,
   "manifests",
   "publisher-local.json",
);

export function readManifest(file: string = manifestPath): SkillManifest {
   const parsed = JSON.parse(fs.readFileSync(file, "utf8")) as SkillManifest;
   if (!Array.isArray(parsed.auto_discovered)) {
      throw new Error(`${file}: auto_discovered must be an array`);
   }
   if (!Array.isArray(parsed.supporting)) {
      throw new Error(`${file}: supporting must be an array`);
   }
   return parsed;
}

/**
 * Every skill the manifest ships, sorted by codepoint so the result does not
 * depend on the runtime's locale (the same reason listSkills() sorts this way).
 */
export function manifestSkillNames(
   manifest: SkillManifest = readManifest(),
): string[] {
   return [...manifest.auto_discovered, ...manifest.supporting].sort((a, b) =>
      a < b ? -1 : a > b ? 1 : 0,
   );
}
