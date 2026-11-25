// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A skill is its whole directory: Markdown inline, every other file listed and
 * hashed, and what `.skillignore` leaves out nowhere at all. The fixture is
 * written by __fixtures__/skill-directory/fixture.ts, and the definition it
 * resolves to is pinned in expected-definition.json beside it.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
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
   SKILLS_ROOT,
   writeAgent,
   writeSkill,
} from "./__fixtures__/skill-directory/fixture";
import {
   PACKAGE_AGENT_SKILLS_MAX_BYTES,
   readAgentAsset,
   readPackageAgents,
   readSkillsDir,
   skillFileList,
   type PackageAgent,
} from "./package_skills";
import { REFERENCE_MAX_EXAMINED, skillFiles } from "./skill_files";

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

   it("inlines Markdown outside assets/, lists every other file, and skips each file a skill may not hold, with a warning that names it", () => {
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

      // Served is the candidates less what a skill may not hold, each skip named.
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

      // A comment leaves the same files, so only the hashed text can have moved.
      const files = () => [
         ...agentOf().skills.map((f) => f.relative_filepath),
         ...agentOf().assets!.map((a) => a.relative_filepath),
      ];
      const before = files();
      write(".skillignore", `${SKILLIGNORE}# a note\n`);
      expect(files()).toEqual(before);
      expect(definitionSha()).not.toBe(afterScript.definition);
      expect(contentSha()).not.toBe(afterScript.content);
   });

   it("hashes an agent's content without reading an inline Markdown file again, and still moves with an edit to one", () => {
      const before = contentSha();
      const result = read();
      const reads = spyOn(fs, "readFileSync");
      try {
         computeSourceContentSha(
            pkg,
            result.paths,
            undefined,
            result.contentShas,
         );
         const hashed = reads.mock.calls.map((c) => String(c[0]));
         const inline = agentOf(result).skills.map((f) => f.relative_filepath);
         expect(inline.length).toBeGreaterThan(0);
         for (const file of inline) {
            expect(hashed.some((p) => p.endsWith(`/${file}`))).toBe(false);
         }
      } finally {
         reads.mockRestore();
      }
      write("NOTES.md", "# Notes\n\nChanged.\n");
      expect(contentSha()).not.toBe(before);
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
      const { skills, files, paths, blocking } = readSkillsDir(pkg);
      expect(skills).toEqual([]);
      expect(files).toEqual([]);
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

      it("refuses an agent with a publisher.json inside an assets/ folder .skillignore ignores", () => {
         write("assets/hidden/Publisher.JSON", "{}");
         write(".skillignore", "assets/hidden/\n");
         const result = read();
         expect(result.agents.has("analyst")).toBe(false);
         expect(result.warnings[0]).toContain("assets/hidden/Publisher.JSON");
      });

      it("refuses an agent with a publisher.json in a reference/ that .skillignore ignores", () => {
         write("reference/publisher.json", "{}");
         write(".skillignore", "reference/\n");
         const result = read();
         expect(result.agents.has("analyst")).toBe(false);
         expect(result.warnings[0]).toContain("reference/publisher.json");
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

   describe("a publisher.json", () => {
      it("refuses the agent in the skills root, in a skill directory, in reference/ and anywhere below assets/", () => {
         for (const where of [
            path.join(pkg, SKILLS_ROOT, "publisher.json"),
            path.join(skillDir(), "publisher.json"),
            path.join(skillDir(), "reference/publisher.json"),
            path.join(skillDir(), "assets/a/b/publisher.json"),
         ]) {
            fs.mkdirSync(path.dirname(where), { recursive: true });
            fs.writeFileSync(where, "{}");
            const result = read();
            expect(result.agents.has("analyst")).toBe(false);
            expect(result.warnings[0]).toContain("nested publisher.json");
            fs.rmSync(where);
         }
         expect(agentOf()).toBeDefined();
      });

      it("is skipped with a warning anywhere else below a skill, and the agent is served", () => {
         const nested = [
            "templates/publisher.json",
            "reference/sub/publisher.json",
            "scripts/deep/er/Publisher.JSON",
         ];
         for (const file of nested) write(file, "{}");
         const agent = agentOf();
         const served = [
            ...agent.skills.map((f) => f.relative_filepath),
            ...agent.assets!.map((a) => a.relative_filepath),
         ];
         for (const file of nested) {
            expect(served).not.toContain(`pricing/${file}`);
            expect(agent.warnings.join("\n")).toContain(`${SKILL}/${file}`);
         }
         expect(served).toContain("pricing/scripts/run.py");
      });

      it("is neither refused nor named when it sits in a folder .skillignore ignores", () => {
         write("fixtures/deep/Publisher.JSON", "{}");
         const result = read();
         expect(agentOf(result)).toBeDefined();
         expect(result.warnings.join("\n")).not.toContain("Publisher.JSON");
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
            `Package skill file '${SKILL}/references/big.md' would take the skill Markdown over the ${PACKAGE_AGENT_SKILLS_MAX_BYTES} byte cap`,
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
      const { skills, warnings, blocking } = readSkillsDir(pkg);
      const catalog = skills.map((s) => s.catalog);
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

   it("is one skill, with its inline Markdown named in the content hash", () => {
      writeSkill(pkg, "skills");
      const { skills, files } = readSkillsDir(pkg);
      expect(skills.map((s) => s.name)).toEqual(["pricing"]);
      expect(files.map((f) => f.path)).toEqual([
         "skills/pricing/SKILL.md",
         "skills/pricing/NOTES.md",
         "skills/pricing/reference/50%-rule.md",
         "skills/pricing/references/a.md",
      ]);
   });

   it("hashes a package's skills without reading an inline Markdown file again", () => {
      writeSkill(pkg, "skills");
      const result = readSkillsDir(pkg);
      const read = spyOn(fs, "readFileSync");
      try {
         computeSourceContentSha(
            pkg,
            result.paths,
            undefined,
            result.contentShas,
         );
         const hashed = read.mock.calls.map((c) => String(c[0]));
         for (const f of result.files) {
            expect(hashed.some((p) => p.endsWith(f.path))).toBe(false);
         }
         expect(result.files.length).toBeGreaterThan(0);
      } finally {
         read.mockRestore();
      }
   });

   it("reads every file it lists by its path: Markdown verbatim, a script as text, a binary as a message", () => {
      writeSkill(pkg, "skills");
      const [skill] = readSkillsDir(pkg).skills;
      expect(skill!.read("SKILL.md")).toEqual({
         text: String(FILES["SKILL.md"]),
      });
      expect(skill!.read("reference/50%-rule.md")).toEqual({
         text: String(FILES["reference/50%-rule.md"]),
      });
      expect(skill!.read("scripts/run.py")).toEqual({
         text: String(FILES["scripts/run.py"]),
      });
      expect(skill!.read("docs/Guide.MD")).toEqual({
         text: String(FILES["docs/Guide.MD"]),
      });
      expect(skill!.read("assets/logo.png")).toEqual({ binary: true });
   });

   it("answers a listed file that changed on disk since the load as changed, never its new bytes", () => {
      writeSkill(pkg, "skills");
      const [skill] = readSkillsDir(pkg).skills;
      fs.writeFileSync(path.join(skillsDir(), "pricing/scripts/run.py"), "x");
      expect(skill!.read("scripts/run.py")).toEqual({ changed: true });
   });

   it("lists and serves a root file named __proto__", () => {
      writeSkillMd("odd", "---\nname: odd\ndescription: d\n---\n\nbody\n");
      fs.writeFileSync(path.join(skillsDir(), "odd/__proto__"), "hi\n");
      const [skill] = readSkillsDir(pkg).skills;
      expect(Object.hasOwn(skill!.catalog.files, "__proto__")).toBe(true);
      expect(skillFileList(skill!).map((f) => f.path)).toEqual([
         "SKILL.md",
         "__proto__",
      ]);
      expect(skill!.read("__proto__")).toEqual({ text: "hi\n" });
   });

   it("lists a Markdown file that is not UTF-8 text as a binary file, never as lossy text", () => {
      writeSkillMd("odd", "---\nname: odd\ndescription: d\n---\n\nbody\n");
      const dir = path.join(skillsDir(), "odd/reference");
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, "nul.md"), Buffer.from("a\0b"));
      fs.writeFileSync(path.join(dir, "latin.md"), Buffer.from([0x63, 0xe9]));
      const { skills, warnings } = readSkillsDir(pkg);
      const skill = skills[0]!;
      expect(Object.keys(skill.catalog.files)).toEqual([
         "SKILL.md",
         "reference/latin.md",
         "reference/nul.md",
      ]);
      expect(skill.catalog.files["reference/latin.md"]!.size).toBe(2);
      expect(skill.read("reference/nul.md")).toEqual({ binary: true });
      expect(skill.read("reference/latin.md")).toEqual({ binary: true });
      expect(warnings.join("\n")).toContain("not UTF-8 text");
   });

   it.skipIf(isWindows)(
      "does not read a listed file that became a link out of the skill",
      () => {
         writeSkill(pkg, "skills");
         const [skill] = readSkillsDir(pkg).skills;
         // The target holds run.py's own bytes, so its digest still matches and only the link refusal can stop the read.
         const secret = path.join(pkg, "secret.txt");
         fs.writeFileSync(secret, String(FILES["scripts/run.py"]));
         const file = path.join(skillsDir(), "pricing/scripts/run.py");
         fs.rmSync(file);
         fs.symlinkSync(secret, file);
         expect(skill!.read("scripts/run.py")).toEqual({ changed: true });
      },
   );

   it("lists exactly the files it serves: a skipped, ignored or built-in-excluded file is not in the catalog", () => {
      writeSkill(pkg, "skills");
      const [skill] = readSkillsDir(pkg).skills;
      for (const left of [
         "scripts/run_test.py",
         "fixtures/data.csv",
         LINK,
         "templates/CLAUDE.md",
         ".env",
         ".skillignore",
      ]) {
         expect(Object.hasOwn(skill!.catalog.files, left)).toBe(false);
      }
   });

   it("lists and hashes against budgets of its own: 200 files", () => {
      writeSkillMd("many", "---\nname: many\ndescription: d\n---\n\nbody\n");
      for (let i = 0; i < 201; i++) {
         fs.mkdirSync(path.join(skillsDir(), "many/scripts"), {
            recursive: true,
         });
         fs.writeFileSync(path.join(skillsDir(), `many/scripts/f${i}.py`), "x");
      }
      const { skills, warnings, contentShas } = readSkillsDir(pkg);
      const catalog = skills.map((s) => s.catalog);
      expect(Object.keys(catalog[0]!.files)).toHaveLength(1 + 200);
      expect(contentShas.size).toBe(1 + 200);
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
      const { skills, warnings } = readSkillsDir(pkg);
      const catalog = skills.map((s) => s.catalog);
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
      const { skills, warnings } = readSkillsDir(pkg);
      const catalog = skills.map((s) => s.catalog);
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
         const { skills, files, paths, contentShas, blocking } =
            readSkillsDir(pkg);
         expect([skills, files, paths]).toEqual([[], [], []]);
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
         const { skills, warnings, blocking } = readSkillsDir(pkg);
         expect(skills[0]!.description).toBe("Use when: a thing happens.");
         expect(skills[0]!.catalog.frontmatter).toEqual({});
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

describe("a skill read whole still serves what it served before", () => {
   let pkg: string;
   const ROOT = SKILLS_ROOT;
   const write = (relative: string, contents: string | Buffer = "x") => {
      const file = path.join(pkg, relative);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, contents);
   };
   const skill = (name: string, root: string = ROOT) =>
      write(
         `${root}/${name}/SKILL.md`,
         `---\nname: ${name}\ndescription: d\n---\n\nbody\n`,
      );
   const pad = (i: number) => String(i).padStart(3, "0");
   const manifest = (roots: string[] = [ROOT]) => ({
      analyst: {
         description: "d",
         instructions: "agents/analyst/instructions.md",
         skills: roots,
      },
   });
   const read = (agents: unknown = manifest()) =>
      readPackageAgents(pkg, agents);
   const agentOf = (result = read()) => result.agents.get("analyst")!;
   const inline = (agent: PackageAgent) =>
      agent.skills.map((f) => f.relative_filepath);
   const listed = (agent: PackageAgent) =>
      (agent.assets ?? []).map((a) => a.relative_filepath);
   /** Twice REFERENCE_MAX_EXAMINED files in `shared-big`, for a link to point at. */
   const fillBig = () => {
      fs.mkdirSync(path.join(pkg, "shared-big"));
      for (let i = 0; i < 2 * REFERENCE_MAX_EXAMINED; i++) {
         fs.writeFileSync(path.join(pkg, "shared-big", `n${i}.txt`), "x");
      }
   };
   // The most entries any one directory listing returned while `run` ran, whole or through opendir.
   const largestListing = (run: () => void) => {
      let largest = 0;
      const listings = spyOn(fs, "readdirSync");
      const open = fs.opendirSync;
      const opens = spyOn(fs, "opendirSync").mockImplementation(((
         ...args: Parameters<typeof fs.opendirSync>
      ) => {
         const opened = open(...args);
         const read = opened.readSync.bind(opened);
         let reads = 0;
         opened.readSync = () => {
            largest = Math.max(largest, ++reads);
            return read();
         };
         return opened;
      }) as typeof fs.opendirSync);
      try {
         run();
         for (const result of listings.mock.results) {
            if (result.type === "return" && Array.isArray(result.value)) {
               largest = Math.max(largest, result.value.length);
            }
         }
      } finally {
         listings.mockRestore();
         opens.mockRestore();
      }
      return largest;
   };

   beforeEach(() => {
      pkg = fs.mkdtempSync(path.join(os.tmpdir(), "skill-compat-"));
      write("agents/analyst/instructions.md", "Be careful.");
   });

   afterEach(() => {
      fs.rmSync(pkg, { recursive: true, force: true });
   });

   it("keeps reference/*.md when a thousand entries sit in a folder beside it", () => {
      skill("s");
      write(`${ROOT}/s/reference/keep.md`, "# keep\n");
      for (let i = 0; i < 1000; i++) write(`${ROOT}/s/examples/e${i}.txt`);
      const result = read();
      expect(inline(agentOf(result))).toContain("s/reference/keep.md");
      expect(result.warnings.join("\n")).toContain("more than 1000 entries");
   });

   it("keeps a later skill's assets/ when an earlier skill's scripts fill the file cap", () => {
      skill("aa");
      skill("bb");
      for (let i = 0; i < 200; i++) write(`${ROOT}/aa/scripts/f${pad(i)}.py`);
      write(`${ROOT}/bb/assets/keep.txt`, "keep");
      const agent = agentOf();
      expect(listed(agent)).toContain("bb/assets/keep.txt");
      expect(listed(agent)).toHaveLength(200);
   });

   it("keeps assets/ when a folder that sorts before it fills the file cap", () => {
      skill("s");
      for (let i = 0; i < 200; i++) write(`${ROOT}/s/Examples/f${pad(i)}.txt`);
      write(`${ROOT}/s/assets/keep.txt`, "keep");
      expect(listed(agentOf())).toContain("s/assets/keep.txt");
   });

   it("keeps a text asset when binaries beside it fill the file cap", () => {
      skill("s");
      for (let i = 0; i < 200; i++) {
         write(`${ROOT}/s/assets/a${pad(i)}.png`, Buffer.from([0x89, 0, 1, 2]));
      }
      write(`${ROOT}/s/assets/z.txt`, "keep");
      const agent = agentOf();
      expect(listed(agent)).toContain("s/assets/z.txt");
      expect(listed(agent)).toHaveLength(200);
   });

   it("keeps reference/*.md inline when a larger Markdown file beside SKILL.md would take its bytes", () => {
      skill("s");
      write(`${ROOT}/s/README.md`, `#\n${"r".repeat(230_000)}`);
      write(`${ROOT}/s/reference/keep.md`, `#\n${"k".repeat(50_000)}`);
      const result = read();
      const agent = agentOf(result);
      expect(inline(agent)).toContain("s/reference/keep.md");
      expect(inline(agent)).not.toContain("s/README.md");
      expect(listed(agent)).toContain("s/README.md");
      expect(result.warnings.join("\n")).toContain("s/README.md");
   });

   it("lists a Markdown file over the inline cap instead of dropping it", () => {
      skill("s");
      write(`${ROOT}/s/references/big.md`, `#\n${"x".repeat(300 * 1024)}`);
      const result = read();
      const agent = agentOf(result);
      expect(inline(agent)).not.toContain("s/references/big.md");
      expect(listed(agent)).toContain("s/references/big.md");
      expect(result.warnings.join("\n")).toContain(
         "s/references/big.md' would take the skill Markdown over",
      );
   });

   it("takes the newly covered entries of all an agent's skills from one budget of 1,000, and still serves each skill's own folders", () => {
      for (const name of ["a", "b", "c"]) {
         skill(name);
         write(`${ROOT}/${name}/reference/keep.md`, "# keep\n");
         write(`${ROOT}/${name}/assets/keep.txt`, "keep");
         for (let i = 0; i < 600; i++) {
            write(`${ROOT}/${name}/data/f${pad(i)}.txt`);
         }
      }
      const result = read();
      const agent = agentOf(result);
      for (const name of ["a", "b", "c"]) {
         expect(inline(agent)).toContain(`${name}/reference/keep.md`);
         expect(listed(agent)).toContain(`${name}/assets/keep.txt`);
      }
      const stopped = result.warnings.filter((w) =>
         w.includes("skill walk stopped"),
      );
      expect(stopped).toHaveLength(2);
      expect(stopped[0]).toContain(`${ROOT}/b'`);
      expect(stopped[0]).toContain("more than 1000 entries were walked");
      expect(stopped[1]).toContain(`${ROOT}/c'`);
      expect(stopped[1]).toContain(
         "the agent's walk budget of 1000 entries was already spent on earlier skills",
      );
   });

   describe("a reference/ that is a link", () => {
      it("is read through, flat Markdown only, when it leads to a directory in the package", () => {
         skill("s");
         write("shared-ref/a.md", "# A\n");
         write("shared-ref/b.txt", "not Markdown");
         write("shared-ref/sub/c.md", "# C\n");
         fs.symlinkSync(
            path.join(pkg, "shared-ref"),
            path.join(pkg, ROOT, "s/reference"),
         );
         const agent = agentOf();
         expect(inline(agent)).toEqual(["s/SKILL.md", "s/reference/a.md"]);
         expect(listed(agent)).toEqual([]);
         expect(
            Object.keys(readSkillsDir(pkg, ROOT).skills[0]!.catalog.files),
         ).toEqual(["SKILL.md", "reference/a.md"]);
      });

      it("is not served, with a warning, when it leads out of the package", () => {
         const outside = fs.mkdtempSync(path.join(os.tmpdir(), "ref-out-"));
         try {
            fs.writeFileSync(path.join(outside, "a.md"), "# Out\n");
            skill("s");
            fs.symlinkSync(outside, path.join(pkg, ROOT, "s/reference"));
            const result = read();
            expect(inline(agentOf(result))).toEqual(["s/SKILL.md"]);
            expect(result.warnings.join("\n")).toContain("s/reference");
            // What lies beyond the link is not looked at, so it is not named either.
            expect(result.warnings.join("\n")).not.toContain("a.md");
         } finally {
            fs.rmSync(outside, { recursive: true, force: true });
         }
      });

      it("is skipped with a warning, and the agent is served, when it leads nowhere", () => {
         skill("s");
         fs.symlinkSync(
            path.join(pkg, "gone"),
            path.join(pkg, ROOT, "s/reference"),
         );
         const result = read();
         expect(inline(agentOf(result))).toEqual(["s/SKILL.md"]);
         expect(result.warnings.join("\n")).toContain("s/reference");
      });

      it("is the only directory link that is read through", () => {
         skill("s");
         write("shared-scripts/run.py", "print()\n");
         fs.symlinkSync(
            path.join(pkg, "shared-scripts"),
            path.join(pkg, ROOT, "s/scripts"),
         );
         const result = read();
         expect(listed(agentOf(result))).toEqual([]);
         expect(result.warnings.join("\n")).toContain(
            "s/scripts' is not served: it is a link",
         );
      });
   });

   it("walks a root that eight agents share once, not once per agent", () => {
      for (let k = 0; k < 16; k++) {
         skill(`k${k}`);
         for (let i = 0; i < 990; i++) write(`${ROOT}/k${k}/data/f${i}.txt`);
      }
      const one = manifest().analyst;
      const eight = Object.fromEntries(
         Array.from({ length: 8 }, (_, i) => [`a${i}`, { ...one }]),
      );
      const listings = spyOn(fs, "readdirSync");
      try {
         read({ a0: one });
         const calls1 = listings.mock.calls.length;
         listings.mockClear();
         const result = read(eight);
         const calls8 = listings.mock.calls.length;
         expect(result.agents.size).toBe(8);
         // The spy must see the walk, or the comparison below is vacuous.
         expect(calls1).toBeGreaterThanOrEqual(32);
         expect(calls8).toBeLessThan(2 * calls1);
      } finally {
         listings.mockRestore();
      }
   });

   it("keeps a later agent's own files when an earlier agent's newly covered files fill the package's 8 MiB", () => {
      const nearCap = Buffer.alloc(1024 * 1024 - 1, "a");
      const agents: Record<string, unknown> = {};
      for (const name of ["a", "b", "c"]) {
         const root = `skills-${name}`;
         skill(`s${name}`, root);
         if (name === "c")
            write(`${root}/sc/assets/keep.txt`, "k".repeat(10_000));
         else {
            for (let i = 0; i < 5; i++)
               write(`${root}/s${name}/data/f${i}.txt`, nearCap);
         }
         agents[name] = { ...manifest([root]).analyst };
      }
      const result = read(agents);
      const data = (name: string) =>
         listed(result.agents.get(name)!).filter((f) => f.includes("/data/"));
      expect(listed(result.agents.get("c")!)).toContain("sc/assets/keep.txt");
      expect(data("a")).toHaveLength(4);
      expect(data("b")).toHaveLength(3);
      expect(result.warnings.join("\n")).toContain("per-package cap");
   });

   describe("a binary file under assets/", () => {
      // What the agent reads of a file in this call, by its file descriptor or whole.
      const bytesRead = (run: () => void) => {
         const whole = spyOn(fs, "readFileSync");
         const part = spyOn(fs, "readSync");
         try {
            run();
            const sum = (xs: number[]) => xs.reduce((a, b) => a + b, 0);
            return (
               sum(whole.mock.results.map((r) => (r.value as Buffer).length)) +
               sum(part.mock.results.map((r) => r.value as number))
            );
         } finally {
            whole.mockRestore();
            part.mockRestore();
         }
      };

      it("is not read in full until a budget takes it", () => {
         skill("s");
         const blob = Buffer.alloc(300_000, 7);
         blob[1] = 0;
         for (let i = 0; i < 100; i++) {
            write(`${ROOT}/s/assets/b${pad(i)}.bin`, blob);
         }
         const total = bytesRead(() => {
            expect(listed(agentOf()).length).toBeGreaterThan(0);
         });
         // The agent's 4 MiB holds thirteen of them; every other is looked at for 8 KiB and skipped.
         expect(total).toBeGreaterThan(300_000);
         expect(total).toBeLessThan(6_000_000);
      });

      it("is still told from text by a NUL after its first 8 KiB, so it never takes a text file's place", () => {
         skill("s");
         const late = Buffer.concat([Buffer.alloc(9000, "a"), Buffer.alloc(1)]);
         for (let i = 0; i < 200; i++) {
            write(`${ROOT}/s/assets/a${pad(i)}.bin`, late);
         }
         write(`${ROOT}/s/assets/z.txt`, "keep");
         const agent = agentOf();
         expect(listed(agent)).toContain("s/assets/z.txt");
         expect(listed(agent)).toHaveLength(200);
         expect(listed(agent)).not.toContain("s/assets/a199.bin");
      });
   });

   describe("a reference/ of many entries", () => {
      const fill = (dir: string, count: number) => {
         for (let i = 0; i < count; i++) write(`${dir}/r${i}.md`, "# R\n");
      };

      it("serves at most 1,000 Markdown files when it is a link to a large directory in the package", () => {
         skill("s");
         fill("shared-ref", 5000);
         fs.symlinkSync(
            path.join(pkg, "shared-ref"),
            path.join(pkg, ROOT, "s/reference"),
         );
         const result = read();
         const references = inline(agentOf(result)).filter((f) =>
            f.startsWith("s/reference/"),
         );
         expect(references).toHaveLength(1000);
         expect(
            result.warnings.filter(
               (w) =>
                  w.includes(`Package skill '${ROOT}/s'`) &&
                  w.includes("reference/ holds more than 1000 Markdown files"),
            ),
         ).toHaveLength(1);
      });

      it("serves at most 1,000 Markdown files when it is a real directory", () => {
         skill("s");
         fill(`${ROOT}/s/reference`, 1200);
         const result = read();
         expect(
            inline(agentOf(result)).filter((f) => f.startsWith("s/reference/")),
         ).toHaveLength(1000);
         expect(result.warnings.join("\n")).toContain(
            "reference/ holds more than 1000 Markdown files",
         );
      });

      it("keeps overview.md when a thousand figures sit beside it", () => {
         skill("s");
         for (let i = 0; i < 1000; i++) {
            write(
               `${ROOT}/s/reference/fig${pad(i)}.png`,
               Buffer.from([0x89, 0]),
            );
         }
         write(`${ROOT}/s/reference/overview.md`, "# Overview\n");
         expect(inline(agentOf())).toContain("s/reference/overview.md");
      });

      describe("that is a link to a directory in the package", () => {
         const linkReference = (target: string) =>
            fs.symlinkSync(
               path.join(pkg, target),
               path.join(pkg, ROOT, "s/reference"),
            );
         it("is listed for no more than 10,001 entries, to serve it or to look for a publisher.json", () => {
            skill("s");
            fillBig();
            linkReference("shared-big");
            const largest = largestListing(() => {
               expect(read().agents.has("analyst")).toBe(true);
            });
            // The spies must see the listing, or the bound below is vacuous.
            expect(largest).toBeGreaterThanOrEqual(REFERENCE_MAX_EXAMINED);
            expect(largest).toBeLessThanOrEqual(REFERENCE_MAX_EXAMINED + 1);
         });

         it("still refuses the agent when it holds a publisher.json", () => {
            skill("s");
            write("shared-ref/a.md", "# A\n");
            write("shared-ref/publisher.json", "{}");
            linkReference("shared-ref");
            const result = read();
            expect(result.agents.has("analyst")).toBe(false);
            expect(result.warnings.join("\n")).toContain(
               "nested publisher.json at 's/reference/publisher.json'",
            );
         });
      });
   });

   describe("an assets/ that is a link to a directory in the package", () => {
      const linkAssets = (skillName: string, target: string) =>
         fs.symlinkSync(
            path.join(pkg, target),
            path.join(pkg, ROOT, skillName, "assets"),
         );

      it("is listed for no more than 10,001 entries, and still refuses the agent", () => {
         skill("s");
         fillBig();
         linkAssets("s", "shared-big");
         let result: ReturnType<typeof read> | undefined;
         const largest = largestListing(() => {
            result = read();
         });
         expect(result!.agents.has("analyst")).toBe(false);
         expect(result!.warnings.join("\n")).toContain(
            `'${ROOT}/s/assets' is a link`,
         );
         // The spies must see the listing, or the bound below is vacuous.
         expect(largest).toBeGreaterThanOrEqual(REFERENCE_MAX_EXAMINED);
         expect(largest).toBeLessThanOrEqual(REFERENCE_MAX_EXAMINED + 1);
      });

      // A dot-named skill directory is never scanned, so its linked assets/ is refused only by the publisher.json check.
      it("still refuses the agent for a publisher.json behind it in a dot-named skill directory", () => {
         skill("s");
         write(`${ROOT}/.old/SKILL.md`, "x");
         write("shared-assets/publisher.json", "{}");
         linkAssets(".old", "shared-assets");
         const result = read();
         expect(result.agents.has("analyst")).toBe(false);
         expect(result.warnings.join("\n")).toContain(
            "nested publisher.json at '.old/assets/publisher.json'",
         );
      });
   });
});
