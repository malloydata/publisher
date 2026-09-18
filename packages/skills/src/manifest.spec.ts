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
/** The routing skill an agent reads to find a sibling. */
const INDEX_SKILL = "malloy";

/**
 * A description is the only text a host reads before deciding whether to load a
 * skill, so it has two budgets, set by the two things that consume it.
 *
 * `DESCRIPTION_CEILING` is the loader budget: Claude Code accepts roughly 1 KiB
 * of frontmatter description. Nothing here enforced it, and nothing here is
 * near it (the longest is ~633), so this is a regrowth guard rather than a
 * constraint anyone is currently fighting.
 *
 * `PACKAGED_DESCRIPTION_BUDGET` is tighter and only applies to the shared
 * skills that a downstream plugin build packages. That build rewrites the
 * `description:` line in place at 200 characters and appends an ellipsis --
 * silently, at build time, on the surface where the description matters most.
 * `malloy-analysis` shipped for several releases as "...and answer delivery.
 * Use..." with the clause saying WHEN to load it cut off. Above 200 the tail is
 * written for an audience that never reads it, so it is asserted here, where an
 * author sees it, rather than applied downstream where nobody does.
 */
const DESCRIPTION_CEILING = 1024;
const PACKAGED_DESCRIPTION_BUDGET = 200;

/**
 * The shared skills a downstream plugin packages today (ms2data/agent-skills
 * `manifests/analysis-plugin.json`, minus its `credible-*` entry, which never
 * lands here).
 *
 * Listed rather than derived because this repo cannot see that manifest. The
 * companion test in agent-skills is scoped to the manifest itself, so a skill
 * ADDED to the plugin is caught there, at the moment it is added; this list
 * holds the ones already in it from growing back past the budget on the side
 * where they are authored. The two are complementary, not duplicates.
 */
const PACKAGED_SKILLS = [
   "malloy-analysis",
   "malloy-analysis-pitfalls",
   "malloy-charts",
   "malloy-gotchas-queries",
   "malloy-gotchas-rendering",
   "malloy-patterns",
   "malloy-phrase-detection",
   "malloy-queries",
] as const;

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

/**
 * The frontmatter block, or undefined when the file has none.
 */
function frontmatterBlock(name: string): string | undefined {
   return fs
      .readFileSync(path.join(skillDir(name), "SKILL.md"), "utf8")
      .replace(/\r\n/g, "\n")
      .match(/^---\n([\s\S]*?)\n---/)?.[1];
}

/**
 * The frontmatter as a host reads it: parsed by a real YAML parser.
 *
 * Deliberately not a line regex. A hand-rolled `^([a-z_]+):\s*(.+)$` returns a
 * value for a scalar YAML refuses -- an unquoted `: ` mid-description ends the
 * plain scalar and makes the whole block `mapping values are not allowed here`
 * -- so every assertion built on it passes on a skill no host can load. Five
 * skills shipped that way before this parsed. `Bun.YAML` is the oracle here
 * rather than a reimplementation of one.
 *
 * Throws on an unparseable block, which is the point: "parses as YAML" below
 * names the failure, and the other frontmatter tests go red alongside it.
 */
function frontmatter(name: string): Record<string, string> {
   const block = frontmatterBlock(name);
   if (block === undefined) return {};
   const parsed = Bun.YAML.parse(block);
   if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error(`${name}: frontmatter is not a mapping`);
   }
   const fields: Record<string, string> = {};
   for (const [key, value] of Object.entries(parsed)) {
      if (typeof value === "string") fields[key] = value.trim();
   }
   return fields;
}

/**
 * The `description:` line's value as the packaging build sees it.
 *
 * Deliberately not `frontmatter()`: that strips outer quotes, and the build
 * that truncates does not -- it rewrites the raw rest of the line. A quoted
 * description two characters over would read as compliant here and still ship
 * cut. The same reason agent-skills reads the raw line rather than the
 * YAML-parsed value, where a `#` in a description ends the scalar early.
 */
function rawDescriptionLength(name: string): number {
   const text = fs
      .readFileSync(path.join(skillDir(name), "SKILL.md"), "utf8")
      .replace(/\r\n/g, "\n");
   return text.match(/^description:[ \t]*(.+)$/m)?.[1].length ?? 0;
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
    * The `malloy` index is the case that forces the distinction: it is the
    * catalogue of every skill, so it names skills outside whatever group it
    * ships in. It states them as plain names rather than `skill:` references,
    * which is the same thing `malloy-getting-started` does for
    * `malloy-gotchas-modeling`. A catalogue row is not an instruction to go
    * read something, and now it does not look like one either, so the index
    * needs no exemption from this test.
    */
   it("keeps each group's references inside it", () => {
      const escapes: string[] = [];
      for (const [group, members] of Object.entries(manifest.groups ?? {})) {
         const inGroup = new Set(members);
         for (const member of members) {
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
   it.each(shipped)("%s: frontmatter parses as YAML", (name) => {
      // The property that decides whether the skill loads at all. A host parses
      // this block; if it raises, the skill is rejected rather than loaded, and
      // nothing downstream reports why.
      const block = frontmatterBlock(name);
      expect(block).toBeDefined();
      expect(() => Bun.YAML.parse(block as string)).not.toThrow();
   });

   it.each(shipped)("%s: frontmatter names its own directory", (name) => {
      const fields = frontmatter(name);
      expect(fields.name).toBe(name);
      expect((fields.description ?? "").trim().length).toBeGreaterThan(0);
   });

   it.each(shipped)("%s: description fits the loader budget", (name) => {
      const description = frontmatter(name).description ?? "";
      expect({ name, length: description.length }).toEqual({
         name,
         length: Math.min(description.length, DESCRIPTION_CEILING),
      });
   });

   // Spread: PACKAGED_SKILLS is `as const`, and it.each takes a mutable array.
   it.each([...PACKAGED_SKILLS])(
      "%s: description survives the plugin build unchanged",
      (name) => {
         expect(shipped).toContain(name);
         const length = rawDescriptionLength(name);
         expect({ name, length }).toEqual({
            name,
            length: Math.min(length, PACKAGED_DESCRIPTION_BUDGET),
         });
      },
   );

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

   it("are complete: the index accounts for every skill that ships", () => {
      // The closure test above asks that the index point at nothing missing.
      // This asks the other direction, which nothing else covers: that nothing
      // shipped is missing FROM the index. The index is how an agent that
      // already has one skill open finds a sibling, so a skill it never names
      // is installed and effectively undiscoverable -- and silently, because
      // the skill loads fine when asked for by name and nothing ever asks.
      //
      // A MENTION, not a `skill:` reference, deliberately. A group must be
      // closed under its own `skill:` references so that excluding it cannot
      // strand a pointer, and the index sits in `modeling` while some skills
      // it should still account for sit in `analysis` and `eval`. Naming those
      // in prose is how the index stays complete without dragging three groups
      // into one. Requiring the invocable form here instead turns a correct
      // index into a group-closure failure, which is what happened when this
      // test was first written.
      const indexBody = fs.readFileSync(
         path.join(skillDir(INDEX_SKILL), "SKILL.md"),
         "utf8",
      );
      const unaccounted = shipped.filter(
         (name) =>
            name !== INDEX_SKILL &&
            !new RegExp(`\`(?:skill:)?${name}\``).test(indexBody),
      );
      expect(unaccounted).toEqual([]);
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
