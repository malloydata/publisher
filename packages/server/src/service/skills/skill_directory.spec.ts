// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A skill is its whole directory: Markdown inline, every other file listed and
 * hashed, and what `.skillignore` leaves out nowhere at all. The fixture is
 * written by __fixtures__/skill-directory/fixture.ts, and the definition it
 * resolves to is pinned in expected-definition.json beside it.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { createHash } from "crypto";
import fs from "fs";
import os from "os";
import path from "path";
import { computeSourceContentSha } from "../package_revision";
import {
   FILES,
   LINK,
   PNG,
   SKILL,
   SKILLIGNORE,
   writeAgent,
   writeSkill,
} from "./__fixtures__/skill-directory/fixture";
import {
   PACKAGE_AGENT_SKILLS_MAX_BYTES,
   readAgentAsset,
   readPackageAgents,
   readSkillsDir,
   type PackageAgent,
} from "./package_skills";
import { skillFiles } from "./skill_files";

const EXPECTED = path.join(
   import.meta.dir,
   "__fixtures__/skill-directory/expected-definition.json",
);
const isWindows = process.platform === "win32";
/** A second skills root, holding one skill of its own. */
const SKILLS_ROOT_B_PARENT = "agents/analyst/more";
const SKILLS_ROOT_B = `${SKILLS_ROOT_B_PARENT}/second`;

const hasControlChar = (text: string) =>
   [...text].some((c) => c.charCodeAt(0) < 32 || c.charCodeAt(0) === 127);

const sha = (data: string | Buffer) =>
   createHash("sha256").update(data).digest("hex");

/** The agent as the wire carries it, less what names the load it came from. */
const wireOf = ({
   definitionSha,
   skillMeta: _skillMeta,
   catalog: _catalog,
   ...definition
}: PackageAgent) => ({ ...definition, source: { definitionSha } });

describe("a skill is its whole directory", () => {
   let pkg: string;
   let agents: ReturnType<typeof writeAgent>;
   const skillDir = () => path.join(pkg, SKILL);
   const write = (relative: string, contents: string | Buffer) => {
      const file = path.join(skillDir(), relative);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, contents);
   };
   const read = (manifest: unknown = agents) =>
      readPackageAgents(pkg, manifest);
   const agentOf = (result = read()) => result.agents.get("analyst")!;
   const definitionSha = () => agentOf().definitionSha;
   const contentSha = () => {
      const result = read();
      return computeSourceContentSha(
         pkg,
         result.paths,
         undefined,
         result.contentShas,
      );
   };

   beforeEach(() => {
      pkg = fs.mkdtempSync(path.join(os.tmpdir(), "skill-directory-"));
      agents = writeAgent(pkg);
   });

   afterEach(() => {
      fs.rmSync(pkg, { recursive: true, force: true });
   });

   it("inlines Markdown outside assets/, lists every other file, and skips each file decision 1 refuses, with a warning that names it", () => {
      const agent = agentOf();
      expect(agent.skills.map((f) => f.relative_filepath)).toEqual([
         "pricing/NOTES.md",
         "pricing/SKILL.md",
         "pricing/reference/50%-rule.md",
         "pricing/references/a.md",
      ]);
      expect(agent.assets!.map((a) => a.relative_filepath)).toEqual([
         "pricing/assets/logo.png",
         "pricing/docs/Guide.MD",
         "pricing/fixtures/keep.csv",
         "pricing/scripts/run.py",
         "pricing/templates/t.html",
      ]);
      expect(agent.asset_roots).toEqual({ pricing: SKILL });

      // Not Markdown by the case-sensitive rule the agent computer reads, so listed.
      expect(
         agent.assets!.some((a) => a.relative_filepath.endsWith(".MD")),
      ).toBe(true);
      expect(agent.warnings).toEqual([
         `skill finding: Package skill '${SKILL}': .skillignore matches SKILL.md, which a skill always keeps`,
         `skill finding: Package skill reference '${SKILL}/templates/CLAUDE.md' is named like a file the agent computer reads with authority, so it is not served`,
         `asset '${SKILL}/${LINK}' is not served: it is a link`,
      ]);
   });

   it("hashes every listed file by its raw bytes, and holds the digest of every served file in the catalog", () => {
      const agent = agentOf();
      for (const asset of agent.assets!) {
         const relative = asset.relative_filepath.slice("pricing/".length);
         const bytes = fs.readFileSync(path.join(skillDir(), relative));
         expect(asset.sha256).toBe(sha(bytes));
         expect(asset.bytes).toBe(bytes.length);
         expect(read().contentShas.get(`${SKILL}/${relative}`)).toBe(
            sha(bytes),
         );
      }
      expect(
         agent.assets!.find((a) => a.relative_filepath.endsWith(".png")),
      ).toMatchObject({ bytes: PNG.length, sha256: sha(PNG) });

      const [entry] = agent.catalog;
      expect(Object.keys(entry!.files)).toEqual([
         "NOTES.md",
         "SKILL.md",
         "assets/logo.png",
         "docs/Guide.MD",
         "fixtures/keep.csv",
         "reference/50%-rule.md",
         "references/a.md",
         "scripts/run.py",
         "templates/t.html",
      ]);
      for (const [relative, { digest, size }] of Object.entries(entry!.files)) {
         const bytes = fs.readFileSync(path.join(skillDir(), relative));
         expect(digest).toBe(`sha256:${sha(bytes)}`);
         expect(size).toBe(bytes.length);
      }
   });

   it("keeps Candidates, Served and Ignored apart", () => {
      const agent = agentOf();
      const candidates = skillFiles(skillDir()).files;
      const served = [
         ...agent.skills.map((f) => f.relative_filepath),
         ...agent.assets!.map((a) => a.relative_filepath),
      ].map((p) => p.slice("pricing/".length));
      const warned = read().warnings.join("\n");

      // Served is the candidates less what decision 1 skips, each skip named.
      const skipped = candidates.filter((c) => !served.includes(c));
      expect(skipped).toEqual(["scripts/run-link.sh", "templates/CLAUDE.md"]);
      for (const file of skipped) expect(warned).toContain(file);
      expect(served.sort()).toEqual(
         candidates.filter((c) => !skipped.includes(c)),
      );

      // Ignored is on disk and outside the candidates: not listed, not
      // served, not hashed and not warned about, but for the `.skillignore`
      // itself, whose applied text the hashes cover.
      const onDisk = fs
         .readdirSync(skillDir(), { recursive: true })
         .map((p) => String(p).split(path.sep).join("/"))
         .filter((p) => !fs.statSync(path.join(skillDir(), p)).isDirectory());
      const ignored = onDisk.filter((p) => !candidates.includes(p));
      expect(ignored.sort()).toEqual([
         ".DS_Store",
         ".env",
         ".skillignore",
         "fixtures/data.csv",
         "scripts/run_test.py",
      ]);
      const hashed = read().paths;
      for (const file of ignored.filter((p) => p !== ".skillignore")) {
         expect(served).not.toContain(file);
         expect(hashed.some((p) => p.endsWith(`/${file}`))).toBe(false);
         expect(warned).not.toContain(file);
      }
      expect(hashed).toContain(`${SKILL}/.skillignore`);
   });

   it("resolves to the definition the service's golden test pins", () => {
      const json = `${JSON.stringify(wireOf(agentOf()), null, 3)}\n`;
      if (process.env.UPDATE_SKILL_DIRECTORY_GOLDEN) {
         fs.writeFileSync(EXPECTED, json);
      }
      expect(json).toBe(fs.readFileSync(EXPECTED, "utf8"));
   });

   it("moves definitionSha and the content hash with an edit to a listed file or to .skillignore, and with nothing that is ignored", () => {
      const base = { definition: definitionSha(), content: contentSha() };

      write("scripts/run.py", "print('changed')\n");
      const afterScript = {
         definition: definitionSha(),
         content: contentSha(),
      };
      expect(afterScript.definition).not.toBe(base.definition);
      expect(afterScript.content).not.toBe(base.content);

      write("scripts/run_test.py", "assert False\n");
      write("fixtures/data.csv", "changed\n");
      write(".env", "SECRET=2\n");
      expect(definitionSha()).toBe(afterScript.definition);
      expect(contentSha()).toBe(afterScript.content);

      write(".skillignore", `${SKILLIGNORE}*.csv\n`);
      expect(definitionSha()).not.toBe(afterScript.definition);
      expect(contentSha()).not.toBe(afterScript.content);
   });

   it("serves a skill of today's shape, SKILL.md with reference/*.md and assets/**, as it always did", () => {
      fs.rmSync(skillDir(), { recursive: true });
      write("SKILL.md", "---\nname: pricing\ndescription: d\n---\n\nbody\n");
      write("reference/glossary.md", "# Glossary\n");
      write("assets/index.html", "<h1>hi</h1>");
      write("assets/css/app.css", "body{}");
      const agent = agentOf();
      expect(agent.skills.map((f) => f.relative_filepath)).toEqual([
         "pricing/SKILL.md",
         "pricing/reference/glossary.md",
      ]);
      expect(agent.assets!.map((a) => a.relative_filepath)).toEqual([
         "pricing/assets/css/app.css",
         "pricing/assets/index.html",
      ]);
      expect(agent.warnings).toEqual([]);
   });

   it("moves definitionSha for a change to an inline file that is not SKILL.md", () => {
      const base = definitionSha();
      write("references/a.md", "# A\n\nChanged.\n");
      expect(definitionSha()).not.toBe(base);
   });

   it("serves a listed text file, and refuses a binary one with a message naming its size and not its bytes", () => {
      const agent = agentOf();
      expect(readAgentAsset(pkg, agent, "pricing/scripts/run.py")).toEqual({
         text: FILES["scripts/run.py"] as string,
      });
      const refused = readAgentAsset(pkg, agent, "pricing/assets/logo.png");
      expect(refused).toEqual({
         message: expect.stringContaining(`${PNG.length} bytes`),
      });
      expect(JSON.stringify(refused)).not.toContain(PNG.toString("base64"));
      // Listed, and still a 404 for what is not.
      expect(
         readAgentAsset(pkg, agent, "pricing/fixtures/data.csv"),
      ).toBeUndefined();
   });

   it("keeps a file whose name only ends in .MD out of the inline Markdown", () => {
      write("docs/other.Md", "# Other\n");
      const agent = agentOf();
      expect(agent.skills.map((f) => f.relative_filepath)).not.toContain(
         "pricing/docs/other.Md",
      );
      expect(agent.assets!.map((a) => a.relative_filepath)).toContain(
         "pricing/docs/other.Md",
      );
   });

   it("reads the block scalar description to its true length, and the frontmatter as JSON", () => {
      const agent = agentOf();
      expect(agent.skillMeta.get("pricing/SKILL.md")).toEqual({
         name: "pricing",
         description:
            "How we price deals: start from list, then apply the discount rules.",
      });
      expect(agent.catalog[0]).toMatchObject({
         name: "pricing",
         frontmatter: {
            name: "pricing",
            description:
               "How we price deals: start from list, then apply the discount rules.",
            license: "MIT",
            metadata: { owner: "revenue", reviewed: "2026-11-01" },
         },
      });
   });

   it("keeps a SKILL.md that is a directory out of service, and serves none of what is inside it", () => {
      fs.rmSync(path.join(skillDir(), "SKILL.md"));
      fs.mkdirSync(path.join(skillDir(), "SKILL.md"));
      write("SKILL.md/inside.md", "# Inside\n");
      write("SKILL.md/inside.py", "print()\n");

      const dropped = read();
      expect(dropped.agents.has("analyst")).toBe(false);
      expect(dropped.warnings.join("\n")).toContain("not a regular file");

      fs.mkdirSync(path.join(pkg, "skills"));
      fs.cpSync(skillDir(), path.join(pkg, "skills/pricing"), {
         recursive: true,
      });
      const { skills, files, catalog, paths, blocking } = readSkillsDir(pkg);
      expect(skills).toEqual([]);
      expect(files).toEqual([]);
      expect(catalog).toEqual([]);
      expect(paths).toEqual([]);
      expect(blocking.join("\n")).toContain("not a regular file");
   });

   describe("what is refused outright, whatever .skillignore says", () => {
      it("refuses an agent whose ignored assets/ is a link out of the package", () => {
         const outside = fs.mkdtempSync(path.join(os.tmpdir(), "outside-"));
         try {
            fs.writeFileSync(path.join(outside, "x.txt"), "x");
            fs.rmSync(path.join(skillDir(), "assets"), { recursive: true });
            fs.symlinkSync(outside, path.join(skillDir(), "assets"));
            write(".skillignore", "assets/\n");
            const result = read();
            expect(result.agents.has("analyst")).toBe(false);
            expect(result.warnings[0]).toContain(`${SKILL}/assets`);
         } finally {
            fs.rmSync(outside, { recursive: true, force: true });
         }
      });

      it("refuses an agent with a publisher.json inside a folder .skillignore ignores", () => {
         write("fixtures/deep/Publisher.JSON", "{}");
         write(".skillignore", "fixtures/\n");
         const result = read();
         expect(result.agents.has("analyst")).toBe(false);
         expect(result.warnings[0]).toContain("fixtures/deep/Publisher.JSON");
      });

      it("cannot bring a dotfile back with a negation", () => {
         write(".skillignore", "!.env\n!.DS_Store\n");
         const agent = agentOf();
         expect(agent.assets!.map((a) => a.relative_filepath)).not.toContain(
            "pricing/.env",
         );
         expect(
            agent.assets!.every((a) => !a.relative_filepath.includes("/.")),
         ).toBe(true);
      });
   });

   describe("names a skill cannot take", () => {
      it("skips a reserved basename anywhere below the skill root, in any case, inline or listed", () => {
         // The fixture's own .skillignore would leave a nested SKILL.md out before it was ever a candidate.
         write(".skillignore", "# nothing\n");
         write("deep/SKILL.md", "# Nested\n");
         write("other/Skill.MD", "# Nested mixed case\n");
         write("deep/AGENTS.md", "# Agents\n");
         write("deep/claude.local.md", "# Local\n");
         write("scripts/Publisher.json.bak", "kept: not reserved\n");
         write("assets/CLAUDE.md", "# In assets\n");
         const result = read();
         const agent = agentOf(result);
         const served = [
            ...agent.skills.map((f) => f.relative_filepath),
            ...agent.assets!.map((a) => a.relative_filepath),
         ];
         for (const refused of [
            "deep/SKILL.md",
            "other/Skill.MD",
            "deep/AGENTS.md",
            "deep/claude.local.md",
            "assets/CLAUDE.md",
         ]) {
            expect(served).not.toContain(`pricing/${refused}`);
            expect(agent.warnings.join("\n")).toContain(`${SKILL}/${refused}`);
         }
         expect(served).toContain("pricing/scripts/Publisher.json.bak");
         expect(served).toContain("pricing/SKILL.md");
      });

      it.skipIf(isWindows)(
         "skips a file with a control character in its path, escaped in the warning, and keeps the agent",
         () => {
            write("scripts/bad\nname.py", "print()\n");
            write("references/bad\u0001name.md", "# Bad\n");
            write("bad\u007fdir/x.py", "print()\n");
            const result = read();
            const agent = agentOf(result);
            const served = [
               ...agent.skills.map((f) => f.relative_filepath),
               ...agent.assets!.map((a) => a.relative_filepath),
            ];
            expect(served.some(hasControlChar)).toBe(false);
            expect(agent.warnings.join("\n")).toContain(
               `${SKILL}/scripts/bad\\u000aname.py`,
            );
            expect(agent.warnings.join("\n")).toContain(
               "has a control character",
            );
            expect(agent.warnings.some(hasControlChar)).toBe(false);
         },
      );
   });

   describe("the inline budget", () => {
      it("lists Markdown that does not fit, first fit in path order, and never drops the agent or a SKILL.md", () => {
         const big = (n: number) => `# Big\n\n${"x".repeat(n)}\n`;
         // 100 KB of SKILL.md in a second skill is charged before any reference.
         fs.mkdirSync(path.join(pkg, SKILLS_ROOT_B), { recursive: true });
         fs.writeFileSync(
            path.join(pkg, SKILLS_ROOT_B, "SKILL.md"),
            `---\nname: second\ndescription: d\n---\n\n${"y".repeat(100_000)}\n`,
         );
         write("references/big.md", big(200_000));
         write("references/small.md", "# Small\n");
         const result = read({
            analyst: {
               ...agents.analyst,
               skills: [...agents.analyst.skills, SKILLS_ROOT_B_PARENT],
            },
         });
         const agent = agentOf(result);

         expect(agent.skills.map((f) => f.relative_filepath)).toEqual(
            expect.arrayContaining([
               "second/SKILL.md",
               "pricing/SKILL.md",
               "pricing/references/small.md",
            ]),
         );
         expect(agent.skills.map((f) => f.relative_filepath)).not.toContain(
            "pricing/references/big.md",
         );
         expect(agent.assets!.map((a) => a.relative_filepath)).toContain(
            "pricing/references/big.md",
         );
         expect(agent.warnings.join("\n")).toContain(
            `Package skill file '${SKILL}/references/big.md' would take the agent's skill Markdown over the ${PACKAGE_AGENT_SKILLS_MAX_BYTES} byte cap`,
         );
         // SKILL.md is re-emitted, so its bytes are a little off the raw ones the cap counts.
         const inlineBytes = agent.skills.reduce(
            (n, f) => n + Buffer.byteLength(f.file_contents),
            0,
         );
         expect(inlineBytes).toBeLessThanOrEqual(
            PACKAGE_AGENT_SKILLS_MAX_BYTES + 1024,
         );
      });

      it("still drops an agent whose SKILL.md files alone pass the cap", () => {
         write(
            "SKILL.md",
            `---\nname: pricing\ndescription: d\n---\n\n${"x".repeat(PACKAGE_AGENT_SKILLS_MAX_BYTES - 100)}\n`,
         );
         fs.mkdirSync(path.join(pkg, SKILLS_ROOT_B), { recursive: true });
         fs.writeFileSync(
            path.join(pkg, SKILLS_ROOT_B, "SKILL.md"),
            `---\nname: second\ndescription: d\n---\n\n${"y".repeat(1000)}\n`,
         );
         const result = read({
            analyst: {
               ...agents.analyst,
               skills: [...agents.analyst.skills, SKILLS_ROOT_B_PARENT],
            },
         });
         expect(result.agents.has("analyst")).toBe(false);
         expect(result.warnings[0]).toContain("byte cap");
      });
   });
});

describe("a package's own skills", () => {
   let pkg: string;
   const skillsDir = () => path.join(pkg, "skills");
   const writeSkillMd = (dir: string, contents: string) => {
      fs.mkdirSync(path.join(skillsDir(), dir), { recursive: true });
      fs.writeFileSync(path.join(skillsDir(), dir, "SKILL.md"), contents);
   };

   beforeEach(() => {
      pkg = fs.mkdtempSync(path.join(os.tmpdir(), "package-skills-dir-"));
   });

   afterEach(() => {
      fs.rmSync(pkg, { recursive: true, force: true });
   });

   it("builds a catalog entry per skill: its frontmatter as JSON and every served file's digest and size", () => {
      writeSkill(pkg, "skills");
      const { catalog, warnings, blocking } = readSkillsDir(pkg);
      expect(blocking).toEqual([]);
      expect(catalog).toHaveLength(1);
      const [entry] = catalog;
      expect(entry!.name).toBe("pricing");
      expect(entry!.frontmatter).toMatchObject({
         license: "MIT",
         metadata: { owner: "revenue", reviewed: "2026-11-01" },
      });
      expect(Object.keys(entry!.files)).toEqual([
         "NOTES.md",
         "SKILL.md",
         "assets/logo.png",
         "docs/Guide.MD",
         "fixtures/keep.csv",
         "reference/50%-rule.md",
         "references/a.md",
         "scripts/run.py",
         "templates/t.html",
      ]);
      expect(entry!.files["assets/logo.png"]).toEqual({
         digest: `sha256:${sha(PNG)}`,
         size: PNG.length,
      });
      expect(warnings.join("\n")).toContain("scripts/run-link.sh");
   });

   it("keeps get_skill's view of reference/*.md, and nothing else, until it is addressed by path", () => {
      writeSkill(pkg, "skills");
      const { skills, files } = readSkillsDir(pkg);
      expect(skills.map((s) => s.name)).toEqual([
         "pricing",
         "pricing/50%-rule",
      ]);
      expect(files.map((f) => f.path)).toEqual([
         "skills/pricing/SKILL.md",
         "skills/pricing/NOTES.md",
         "skills/pricing/reference/50%-rule.md",
         "skills/pricing/references/a.md",
      ]);
   });

   it("lists and hashes against budgets of its own: 200 files", () => {
      writeSkillMd("many", "---\nname: many\ndescription: d\n---\n\nbody\n");
      for (let i = 0; i < 201; i++) {
         fs.mkdirSync(path.join(skillsDir(), "many/scripts"), {
            recursive: true,
         });
         fs.writeFileSync(path.join(skillsDir(), `many/scripts/f${i}.py`), "x");
      }
      const { catalog, warnings, contentShas } = readSkillsDir(pkg);
      expect(Object.keys(catalog[0]!.files)).toHaveLength(1 + 200);
      expect(contentShas.size).toBe(200);
      expect(warnings.join("\n")).toContain(
         "the package's skills already have 200 assets",
      );
   });

   it("lists and hashes against budgets of its own: 4 MiB", () => {
      writeSkillMd("fat", "---\nname: fat\ndescription: d\n---\n\nbody\n");
      fs.mkdirSync(path.join(skillsDir(), "fat/data"), { recursive: true });
      for (let i = 0; i < 5; i++) {
         fs.writeFileSync(
            path.join(skillsDir(), `fat/data/f${i}.bin`),
            Buffer.alloc(900_000, 1),
         );
      }
      const { catalog, warnings } = readSkillsDir(pkg);
      expect(Object.keys(catalog[0]!.files)).toHaveLength(1 + 4);
      expect(warnings.join("\n")).toContain("f4.bin");
      expect(warnings.join("\n")).toContain("per-package skills cap");
   });

   it("leaves out what a *-and-!SKILL.md allowlist leaves out, and says nothing", () => {
      writeSkillMd("only", "---\nname: only\ndescription: d\n---\n\nbody\n");
      fs.writeFileSync(path.join(skillsDir(), "only/extra.py"), "x");
      fs.writeFileSync(
         path.join(skillsDir(), "only/.skillignore"),
         "*\n!SKILL.md\n",
      );
      const { catalog, warnings } = readSkillsDir(pkg);
      expect(Object.keys(catalog[0]!.files)).toEqual(["SKILL.md"]);
      expect(warnings).toEqual([]);
   });

   it("does not serve a skill whose SKILL.md is a link out of the package, nor its directory's files", () => {
      const outside = fs.mkdtempSync(path.join(os.tmpdir(), "outside-md-"));
      try {
         fs.writeFileSync(path.join(outside, "secret.md"), "SECRET");
         fs.mkdirSync(path.join(skillsDir(), "bad"), { recursive: true });
         fs.symlinkSync(
            path.join(outside, "secret.md"),
            path.join(skillsDir(), "bad/SKILL.md"),
         );
         fs.writeFileSync(path.join(skillsDir(), "bad/run.py"), "x");
         const { catalog, files, paths, contentShas, blocking } =
            readSkillsDir(pkg);
         expect([catalog, files, paths]).toEqual([[], [], []]);
         expect(contentShas.size).toBe(0);
         expect(blocking.join("\n")).toContain("outside the package");
      } finally {
         fs.rmSync(outside, { recursive: true, force: true });
      }
   });

   describe("frontmatter", () => {
      it("reads a block scalar description to its true length", () => {
         writeSkillMd(
            "folded",
            "---\nname: folded\ndescription: >-\n  one\n  two\n---\n\nbody\n",
         );
         const { skills, warnings } = readSkillsDir(pkg);
         expect(skills[0]!.description).toBe("one two");
         expect(warnings).toEqual([]);
      });

      it("warns, and reads name and description line by line, when the YAML does not parse", () => {
         writeSkillMd(
            "loose",
            "---\nname: loose\ndescription: Use when: a thing happens.\n---\n\nbody\n",
         );
         const { skills, catalog, warnings, blocking } = readSkillsDir(pkg);
         expect(skills[0]!.description).toBe("Use when: a thing happens.");
         expect(catalog[0]!.frontmatter).toEqual({});
         expect(blocking).toEqual([]);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("skills/loose/SKILL.md");
         expect(warnings[0]).toContain("not valid YAML");
      });

      it("warns about a name outside the spec's character set, and still serves a skill whose directory has that name", () => {
         writeSkillMd(
            "My_Skill",
            "---\nname: My_Skill\ndescription: d\n---\n\nbody\n",
         );
         const { skills, warnings, blocking } = readSkillsDir(pkg);
         expect(skills.map((s) => s.name)).toEqual(["My_Skill"]);
         expect(blocking).toEqual([]);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("skills/My_Skill/SKILL.md");
         expect(warnings[0]).toContain('"My_Skill"');
      });
   });
});
