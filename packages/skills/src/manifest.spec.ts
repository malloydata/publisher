// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The manifest is only worth having if it is the single answer to "what ships".
 * These are the checks that make that true: the manifest describes real skills,
 * the skills it names are internally consistent, and every channel agrees with
 * it.
 *
 * Ported from ms2data/agent-skills `tests/test_manifests.py`, which enforces
 * the same contract on the other side of the vendoring boundary. Keeping the
 * two suites recognisably parallel is deliberate: a rule that holds in one repo
 * and not the other is how the corpora drift apart in the first place.
 */
import { describe, expect, it } from "bun:test";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import { isCredible } from "../scripts/exclusions";
import {
   manifestPath,
   manifestSkillNames,
   readManifest,
   repoRoot,
} from "../scripts/manifest";

const sourceSkillsDir = path.join(repoRoot, "skills");
const manifest = readManifest();
const shipped = manifestSkillNames();

/** `skill:<name>`, capturing any `/subpath` so the anti-pattern can be flagged. */
const SKILL_REF = /skill:([a-z0-9][a-z0-9-]*)(\/[^\s`)\]]*)?/g;
/** An install-path prefix binds a body to one host's on-disk layout. */
const ABSOLUTE_INSTALL_PATH = /\.(?:cursor|credible|claude)\/skills\//;
/** A same-skill resource reference, which must resolve inside that skill. */
const RELATIVE_REF = /(?<![\w/.`-])reference\/[\w./-]+\.md/g;

function skillDir(name: string): string {
   return path.join(sourceSkillsDir, name);
}

function markdownFiles(name: string): string[] {
   const dir = skillDir(name);
   const out: string[] = [];
   const walk = (current: string): void => {
      for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
         const full = path.join(current, entry.name);
         if (entry.isDirectory()) walk(full);
         else if (entry.name.endsWith(".md")) out.push(full);
      }
   };
   walk(dir);
   return out.sort();
}

function frontmatter(name: string): Record<string, string> {
   const text = fs
      .readFileSync(path.join(skillDir(name), "SKILL.md"), "utf8")
      .replace(/\r\n/g, "\n");
   const block = text.match(/^---\n([\s\S]*?)\n---/)?.[1];
   if (block === undefined) return {};
   const fields: Record<string, string> = {};
   for (const line of block.split("\n")) {
      const match = line.match(/^([a-z_]+):\s*(.+)$/);
      if (match) fields[match[1]] = match[2].trim().replace(/^["']|["']$/g, "");
   }
   return fields;
}

describe("publisher-local manifest", () => {
   it("names itself after its filename", () => {
      expect(manifest.name).toBe(path.basename(manifestPath, ".json"));
   });

   it("carries a description a host can show", () => {
      expect(typeof manifest.description).toBe("string");
      expect(manifest.description.trim().length).toBeGreaterThan(0);
   });

   it("lists no skill twice", () => {
      expect(shipped.length).toBe(new Set(shipped).size);
   });

   it("keeps supporting empty", () => {
      // A supporting skill lands in a second directory that agents discover
      // poorly and an SDK Skill tool cannot invoke from at all. Publisher ships
      // one flat set; the `groups` filter is how someone takes less.
      expect(manifest.supporting).toEqual([]);
   });

   it("names only skills that exist", () => {
      const missing = shipped.filter(
         (name) => !fs.existsSync(path.join(skillDir(name), "SKILL.md")),
      );
      expect(missing).toEqual([]);
   });

   it("ships every skill in the tree, so nothing is silently left behind", () => {
      // The reverse of the check above, and the one that catches the real
      // mistake: adding a skill and forgetting to register it, which used to be
      // impossible because everything shipped by default.
      const onDisk = fs
         .readdirSync(sourceSkillsDir, { withFileTypes: true })
         .filter(
            (entry) =>
               entry.isDirectory() &&
               !isCredible(entry.name) &&
               fs.existsSync(
                  path.join(sourceSkillsDir, entry.name, "SKILL.md"),
               ),
         )
         .map((entry) => entry.name);
      expect(onDisk.filter((name) => !shipped.includes(name))).toEqual([]);
   });

   /**
    * The point of a role group is that installing it gives an agent that role's
    * doctrine and not the other's. An outward `skill:` reference defeats that
    * twice over: a consumer that installs the group alone hits an instruction
    * it cannot follow, and one that resolves the reference ends up holding the
    * doctrine the group exists to withhold. `analysis` leaked into modeling
    * this way (`malloy-getting-started` -> `malloy-gotchas-modeling`,
    * `malloy-analysis-report` -> `malloy-model`), which is why both now name
    * those skills in prose instead.
    *
    * `malloy` is the documented exception: it is the index of every Malloy
    * skill, so its table necessarily has a row per skill, and 10+ skills point
    * at it in turn, so it cannot be dropped from a group either. A catalogue
    * row is not an instruction to go read something, which is what this test
    * is really about.
    */
   it("keeps each group's references inside it, the malloy index aside", () => {
      const escapes: string[] = [];
      for (const [group, members] of Object.entries(manifest.groups ?? {})) {
         const inGroup = new Set(members);
         for (const member of members) {
            if (member === "malloy") continue;
            for (const file of markdownFiles(member)) {
               const body = fs.readFileSync(file, "utf8");
               for (const [, target] of body.matchAll(SKILL_REF)) {
                  if (target === member || inGroup.has(target)) continue;
                  if (!shipped.includes(target)) continue;
                  escapes.push(`${group}: ${member} -> ${target}`);
               }
            }
         }
      }
      expect([...new Set(escapes)].sort()).toEqual([]);
   });

   it("puts every group member in the shipped set", () => {
      const strays: string[] = [];
      for (const [group, members] of Object.entries(manifest.groups ?? {})) {
         for (const member of members) {
            if (!shipped.includes(member)) strays.push(`${group}: ${member}`);
         }
      }
      expect(strays).toEqual([]);
   });
});

describe("shipped skills", () => {
   it.each(shipped)("%s: frontmatter names its own directory", (name) => {
      const fields = frontmatter(name);
      expect(fields.name).toBe(name);
      expect((fields.description ?? "").trim().length).toBeGreaterThan(0);
   });

   it.each(shipped)("%s: declares no version of its own", (name) => {
      // The pack stamps `version:` at pack time and refuses a second one, so a
      // version here would fail the publish rather than this test. Catch it in
      // the suite instead, where the message says what to do.
      expect(frontmatter(name).version).toBeUndefined();
   });

   it.each(shipped)("%s: hardcodes no host install path", (name) => {
      const offenders = markdownFiles(name).filter((file) =>
         ABSOLUTE_INSTALL_PATH.test(fs.readFileSync(file, "utf8")),
      );
      expect(offenders.map((f) => path.relative(repoRoot, f))).toEqual([]);
   });

   it.each(shipped)("%s: resolves its own reference/ paths", (name) => {
      const dangling: string[] = [];
      for (const file of markdownFiles(name)) {
         const body = fs.readFileSync(file, "utf8");
         for (const match of body.matchAll(RELATIVE_REF)) {
            if (!fs.existsSync(path.join(skillDir(name), match[0]))) {
               dangling.push(`${path.relative(repoRoot, file)} -> ${match[0]}`);
            }
         }
      }
      expect(dangling).toEqual([]);
   });
});

describe("cross-skill references", () => {
   it("are closed over the manifest", () => {
      // A `skill:` reference to something this manifest does not ship is
      // "Unknown skill" at runtime for anyone who installs from it.
      const problems: string[] = [];
      for (const name of shipped) {
         for (const file of markdownFiles(name)) {
            const body = fs.readFileSync(file, "utf8");
            for (const [, target] of body.matchAll(SKILL_REF)) {
               if (target !== name && !shipped.includes(target)) {
                  problems.push(
                     `${path.relative(repoRoot, file)} invokes skill:${target}`,
                  );
               }
            }
         }
      }
      expect([...new Set(problems)]).toEqual([]);
   });

   it("invoke a skill by name, never by subpath", () => {
      // A host resolves `skill:<name>` to that skill's SKILL.md, so a subpath
      // appends past a file. Point at the skill and let it surface its own
      // reference files.
      const problems: string[] = [];
      for (const name of shipped) {
         for (const file of markdownFiles(name)) {
            const body = fs.readFileSync(file, "utf8");
            for (const [, target, subpath] of body.matchAll(SKILL_REF)) {
               if (subpath) {
                  problems.push(
                     `${path.relative(repoRoot, file)}: skill:${target}${subpath}`,
                  );
               }
            }
         }
      }
      expect(problems).toEqual([]);
   });
});

describe("the .claude/skills symlinks", () => {
   it("match the manifest", () => {
      // This is how contributors get the skills in their own agent, and it
      // drifted by hand before the manifest existed.
      //
      // Asks git rather than the filesystem, for the same reason index.spec.ts
      // does. `cross-platform-tests.yml` runs this suite on windows-latest,
      // where Git materializes a mode-120000 entry as a plain text file
      // holding the target path unless `core.symlinks` is on; `isSymbolicLink`
      // on the worktree is `false` for all 32 in that state, and so is "has a
      // SKILL.md". The index records the mode either way, so this reads the
      // same on every platform. It also ignores a contributor's untracked
      // local additions under `.claude/skills`, which are a supported state.
      //
      // Mode 120000 is load-bearing, not incidental: `publisher-release` is a
      // real committed directory here rather than a link into `skills/`, being
      // a repo-operations skill that ships through no channel, so filtering on
      // the mode is what keeps it out of the comparison.
      const linked = execFileSync(
         "git",
         ["ls-files", "-s", "--", ".claude/skills"],
         { cwd: repoRoot, encoding: "utf8" },
      )
         .split("\n")
         .filter(Boolean)
         .map((line) => line.split(/\s+/))
         .filter(([mode]) => mode === "120000")
         .map((fields) => path.basename(fields[fields.length - 1] as string))
         .sort();
      expect(linked).toEqual(shipped);
   });
});
