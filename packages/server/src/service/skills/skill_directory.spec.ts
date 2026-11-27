// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A skill is its whole directory: Markdown inline, every other file listed and
 * hashed, and what `.skillignore` leaves out nowhere at all. The fixture is
 * written by __fixtures__/skill-directory/fixture.ts, and the definition it
 * resolves to is pinned in expected-definition.json beside it.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { execFileSync } from "child_process";
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
   writeSkill,
   writeWorkflow,
} from "./__fixtures__/skill-directory/fixture";
import {
   readWorkflowFile,
   skillFileList,
   workflowSkillSet,
} from "./package_skills";
import {
   PACKAGE_ASSET_FILE_MAX_BYTES,
   PACKAGE_SKILL_FILE_MAX_BYTES,
   SKILL_SET_MARKDOWN_MAX_BYTES,
   readPackageWorkflows,
   type PackageWorkflow,
   type PackageWorkflows,
} from "./package_workflows";
import { REFERENCE_MAX_EXAMINED, skillFiles } from "./skill_files";

const EXPECTED = path.join(
   import.meta.dir,
   "__fixtures__/skill-directory/expected-definition.json",
);
const isWindows = process.platform === "win32";
/** A second skill set, holding one skill of its own. */
const SKILLS_ROOT_B = "workflows/analyst/skill_sets/second";

const hasControlChar = (text: string) =>
   [...text].some((c) => c.charCodeAt(0) < 32 || c.charCodeAt(0) === 127);

const sha = (data: string | Buffer) =>
   createHash("sha256").update(data).digest("hex");

/** The first set of a workflow as the wire carries it, with what the workflow and its set warn of. */
const viewOf = (workflow: PackageWorkflow) => {
   const entry = workflow.sets[0]!;
   const { skills, assets } = workflowSkillSet(entry, "full");
   return {
      // The full view's files; the view's own type is the union with the summary's.
      skills: skills as Array<{
         relative_filepath: string;
         file_contents: string;
      }>,
      assets: assets ?? [],
      warnings: [...workflow.shadowing, ...entry.set.findings],
      definitionSha: workflow.definitionSha,
      catalog: entry.skills.map((s) => s.catalog),
   };
};

/** The definition as the wire carries it, less what names the load it came from. */
const wireOf = (workflow: PackageWorkflow) => ({
   name: workflow.name,
   description: workflow.description,
   instructions: workflow.instructions?.text,
   skill_sets: workflow.sets.map((entry) => workflowSkillSet(entry, "full")),
   warnings: [
      ...workflow.shadowing,
      ...workflow.sets.flatMap(({ set }) => set.findings),
   ],
   source: { definitionSha: workflow.definitionSha },
});

describe("a skill is its whole directory", () => {
   let pkg: string;
   let workflows: ReturnType<typeof writeWorkflow>;
   const skillDir = () => path.join(pkg, SKILL);
   const write = (relative: string, contents: string | Buffer) => {
      const file = path.join(skillDir(), relative);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, contents);
   };
   const read = (manifest: unknown = workflows) =>
      readPackageWorkflows(pkg, manifest);
   const workflowOf = (result: PackageWorkflows = read()) =>
      result.workflows.get("analyst")!;
   const definitionSha = () => workflowOf().definitionSha;
   const contentSha = () => {
      const result = read();
      return computeSourceContentSha(result.contentShas);
   };

   beforeEach(() => {
      pkg = fs.mkdtempSync(path.join(os.tmpdir(), "skill-directory-"));
      workflows = writeWorkflow(pkg);
   });

   afterEach(() => {
      fs.rmSync(pkg, { recursive: true, force: true });
   });

   it("inlines Markdown outside assets/, lists every other file, and skips each file a skill may not hold, with a warning that names it", () => {
      const view = viewOf(workflowOf());
      expect(view.skills.map((f) => f.relative_filepath)).toEqual([
         "pricing/NOTES.md",
         "pricing/SKILL.md",
         "pricing/reference/rule-of-50.md",
         "pricing/references/a.md",
      ]);
      expect(view.assets.map((a) => a.relative_filepath)).toEqual([
         "pricing/assets/logo.png",
         "pricing/docs/Guide.MD",
         "pricing/fixtures/keep.csv",
         "pricing/scripts/run.py",
         "pricing/templates/t.html",
      ]);

      // Not Markdown by the case-sensitive rule the agent computer reads, so listed.
      expect(view.assets.some((a) => a.relative_filepath.endsWith(".MD"))).toBe(
         true,
      );
      const finding = (text: string) =>
         `Skill set '${SKILLS_ROOT}' skill finding: ${text}.`;
      expect(view.warnings).toEqual([
         finding(
            `Package skill '${SKILL}': .skillignore matches SKILL.md, which a skill always keeps`,
         ),
         finding(
            `Package skill reference '${SKILL}/templates/CLAUDE.md' is named like a file the agent computer reads with authority, so it is not served`,
         ),
         finding(`asset '${SKILL}/${LINK}' is not served: it is a link`),
      ]);
   });

   it("hashes every listed file by its raw bytes, and holds the digest of every served file in the catalog", () => {
      const view = viewOf(workflowOf());
      for (const asset of view.assets) {
         const relative = asset.relative_filepath.slice("pricing/".length);
         const bytes = fs.readFileSync(path.join(skillDir(), relative));
         expect(asset.sha256).toBe(sha(bytes));
         expect(asset.bytes).toBe(bytes.length);
         expect(read().contentShas.get(`${SKILL}/${relative}`)).toBe(
            sha(bytes),
         );
      }
      expect(
         view.assets.find((a) => a.relative_filepath.endsWith(".png")),
      ).toMatchObject({ bytes: PNG.length, sha256: sha(PNG) });

      const [entry] = view.catalog;
      expect(Object.keys(entry!.files)).toEqual([
         "NOTES.md",
         "SKILL.md",
         "assets/logo.png",
         "docs/Guide.MD",
         "fixtures/keep.csv",
         "reference/rule-of-50.md",
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
      const view = viewOf(workflowOf());
      const candidates = skillFiles(skillDir()).files;
      const served = [
         ...view.skills.map((f) => f.relative_filepath),
         ...view.assets.map((a) => a.relative_filepath),
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
      const json = `${JSON.stringify(wireOf(workflowOf()), null, 3)}\n`;
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
         ...viewOf(workflowOf()).skills.map((f) => f.relative_filepath),
         ...viewOf(workflowOf()).assets.map((a) => a.relative_filepath),
      ];
      const before = files();
      write(".skillignore", `${SKILLIGNORE}# a note\n`);
      expect(files()).toEqual(before);
      expect(definitionSha()).not.toBe(afterScript.definition);
      expect(contentSha()).not.toBe(afterScript.content);
   });

   it("hashes a workflow's content from the shas the walk took, reading no file, and still moves with an edit to one", () => {
      const before = contentSha();
      const result = read();
      const reads = spyOn(fs, "readFileSync");
      try {
         computeSourceContentSha(result.contentShas);
         const hashed = reads.mock.calls.map((c) => String(c[0]));
         const inline = viewOf(workflowOf(result)).skills.map(
            (f) => f.relative_filepath,
         );
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
      const view = viewOf(workflowOf());
      expect(view.skills.map((f) => f.relative_filepath)).toEqual([
         "pricing/SKILL.md",
         "pricing/reference/glossary.md",
      ]);
      expect(view.assets.map((a) => a.relative_filepath)).toEqual([
         "pricing/assets/css/app.css",
         "pricing/assets/index.html",
      ]);
      expect(view.warnings).toEqual([]);
   });

   it("moves definitionSha for a change to an inline file that is not SKILL.md", () => {
      const base = definitionSha();
      write("references/a.md", "# A\n\nChanged.\n");
      expect(definitionSha()).not.toBe(base);
   });

   it("serves a listed text file, and refuses a binary one with a message naming its size and not its bytes", async () => {
      const skill = workflowOf().sets[0]!.skills[0]!;
      expect(await readWorkflowFile(skill, "pricing/scripts/run.py")).toEqual({
         text: FILES["scripts/run.py"] as string,
      });
      const refused = await readWorkflowFile(skill, "pricing/assets/logo.png");
      expect(refused).toEqual({
         message: expect.stringContaining(`${PNG.length} bytes`),
      });
      expect(JSON.stringify(refused)).not.toContain(PNG.toString("base64"));
      // Listed, and still a 404 for what is not.
      expect(
         await readWorkflowFile(skill, "pricing/fixtures/data.csv"),
      ).toBeUndefined();
   });

   it("keeps a file whose name only ends in .MD out of the inline Markdown", () => {
      write("docs/other.Md", "# Other\n");
      const view = viewOf(workflowOf());
      expect(view.skills.map((f) => f.relative_filepath)).not.toContain(
         "pricing/docs/other.Md",
      );
      expect(view.assets.map((a) => a.relative_filepath)).toContain(
         "pricing/docs/other.Md",
      );
   });

   it("reads the block scalar description to its true length, and the frontmatter as JSON", () => {
      const view = viewOf(workflowOf());
      const summary = workflowSkillSet(workflowOf().sets[0]!, "summary");
      expect(
         summary.skills.find((f) => f.relative_filepath === "pricing/SKILL.md"),
      ).toMatchObject({
         name: "pricing",
         description:
            "How we price deals: start from list, then apply the discount rules.",
      });
      expect(view.catalog[0]).toMatchObject({
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
      expect(workflowOf(dropped).sets[0]!.skills).toEqual([]);
      expect(dropped.warnings.join("\n")).toContain("not a regular file");
      expect(dropped.warnings.join("\n")).toContain("holds no skills");
      expect(dropped.paths).toEqual(["workflows/analyst/instructions.md"]);
      expect([...dropped.contentShas.keys()]).toEqual(dropped.paths);
   });

   describe("what sets a skill aside, or refuses the workflow, whatever .skillignore says", () => {
      it("sets aside a skill whose ignored assets/ is a link out of the package, and serves the workflow", () => {
         const outside = fs.mkdtempSync(path.join(os.tmpdir(), "outside-"));
         try {
            fs.writeFileSync(path.join(outside, "x.txt"), "x");
            fs.rmSync(path.join(skillDir(), "assets"), { recursive: true });
            fs.symlinkSync(outside, path.join(skillDir(), "assets"));
            write(".skillignore", "assets/\n");
            const result = read();
            expect(workflowOf(result).sets[0]!.skills).toEqual([]);
            expect(result.warnings.join("\n")).toContain(
               `Skill set '${SKILLS_ROOT}' skill finding: Package skill '${SKILL}' is not served: '${SKILL}/assets' is a link`,
            );
            expect(result.warnings.join("\n")).not.toContain("x.txt");
         } finally {
            fs.rmSync(outside, { recursive: true, force: true });
         }
      });

      it("sets aside a skill with a publisher.json inside an assets/ folder .skillignore ignores, and serves the workflow", () => {
         write("assets/hidden/Publisher.JSON", "{}");
         write(".skillignore", "assets/hidden/\n");
         const result = read();
         expect(workflowOf(result).sets[0]!.skills).toEqual([]);
         expect(result.warnings.join("\n")).toContain(
            `Package skill '${SKILL}' is not served: '${SKILL}/assets/hidden/Publisher.JSON' is a publisher.json`,
         );
      });

      it("sets aside a skill with a publisher.json in a reference/ that .skillignore ignores, and serves the workflow", () => {
         write("reference/publisher.json", "{}");
         write(".skillignore", "reference/\n");
         const result = read();
         expect(workflowOf(result).sets[0]!.skills).toEqual([]);
         expect(result.warnings.join("\n")).toContain(
            `Package skill '${SKILL}' is not served: '${SKILL}/reference/publisher.json' is a publisher.json`,
         );
      });

      it("cannot bring a dotfile back with a negation", () => {
         write(".skillignore", "!.env\n!.DS_Store\n");
         const view = viewOf(workflowOf());
         expect(view.assets.map((a) => a.relative_filepath)).not.toContain(
            "pricing/.env",
         );
         expect(
            view.assets.every((a) => !a.relative_filepath.includes("/.")),
         ).toBe(true);
      });
   });

   describe("a publisher.json", () => {
      it("refuses the workflow for a publisher.json in the skill set root", () => {
         const where = path.join(pkg, SKILLS_ROOT, "publisher.json");
         fs.writeFileSync(where, "{}");
         const result = read();
         expect(result.workflows.has("analyst")).toBe(false);
         expect(result.warnings[0]).toContain("nested publisher.json");
         fs.rmSync(where);
         expect(viewOf(workflowOf())).toBeDefined();
      });

      it("sets the skill aside for a publisher.json in its directory, in reference/ and anywhere below assets/, and serves the workflow", () => {
         for (const inside of [
            "publisher.json",
            "reference/publisher.json",
            "assets/a/b/publisher.json",
         ]) {
            write(inside, "{}");
            const result = read();
            expect(workflowOf(result).sets[0]!.skills).toEqual([]);
            expect(result.warnings.join("\n")).toContain(
               `'${SKILL}/${inside}' is a publisher.json`,
            );
            fs.rmSync(path.join(skillDir(), inside));
         }
         expect(viewOf(workflowOf())).toBeDefined();
      });

      it("is skipped with a warning anywhere else below a skill, and the workflow is served", () => {
         const nested = [
            "templates/publisher.json",
            "reference/sub/publisher.json",
            "scripts/deep/er/Publisher.JSON",
         ];
         for (const file of nested) write(file, "{}");
         const view = viewOf(workflowOf());
         const served = [
            ...view.skills.map((f) => f.relative_filepath),
            ...view.assets.map((a) => a.relative_filepath),
         ];
         for (const file of nested) {
            expect(served).not.toContain(`pricing/${file}`);
            expect(view.warnings.join("\n")).toContain(`${SKILL}/${file}`);
         }
         expect(served).toContain("pricing/scripts/run.py");
      });

      it("is neither refused nor named when it sits in a folder .skillignore ignores", () => {
         write("fixtures/deep/Publisher.JSON", "{}");
         const result = read();
         expect(viewOf(workflowOf(result))).toBeDefined();
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
         const view = viewOf(workflowOf(result));
         const served = [
            ...view.skills.map((f) => f.relative_filepath),
            ...view.assets.map((a) => a.relative_filepath),
         ];
         for (const refused of [
            "deep/SKILL.md",
            "other/Skill.MD",
            "deep/AGENTS.md",
            "deep/claude.local.md",
            "assets/CLAUDE.md",
         ]) {
            expect(served).not.toContain(`pricing/${refused}`);
            expect(view.warnings.join("\n")).toContain(`${SKILL}/${refused}`);
         }
         expect(served).toContain("pricing/scripts/Publisher.json.bak");
         expect(served).toContain("pricing/SKILL.md");
      });

      it.skipIf(isWindows)(
         "skips a file with a control character in its path, escaped in the warning, and keeps the workflow",
         () => {
            write("scripts/bad\nname.py", "print()\n");
            write("references/bad\u0001name.md", "# Bad\n");
            write("bad\u007fdir/x.py", "print()\n");
            const result = read();
            const view = viewOf(workflowOf(result));
            const served = [
               ...view.skills.map((f) => f.relative_filepath),
               ...view.assets.map((a) => a.relative_filepath),
            ];
            expect(served.some(hasControlChar)).toBe(false);
            expect(view.warnings.join("\n")).toContain(
               `${SKILL}/scripts/bad\\u000aname.py`,
            );
            expect(view.warnings.join("\n")).toContain(
               "has a control character",
            );
            expect(view.warnings.some(hasControlChar)).toBe(false);
         },
      );
   });

   describe("the inline budget", () => {
      const writeSecondSkill = (root: string, filler: number) => {
         fs.mkdirSync(path.join(pkg, root, "second"), { recursive: true });
         fs.writeFileSync(
            path.join(pkg, root, "second/SKILL.md"),
            `---\nname: second\ndescription: d\n---\n\n${"y".repeat(filler)}\n`,
         );
      };

      it("lists Markdown that does not fit, first fit in path order, and never drops the workflow or a SKILL.md", () => {
         const big = (n: number) => `# Big\n\n${"x".repeat(n)}\n`;
         // 100 KB of SKILL.md in a second skill is charged before any reference.
         writeSecondSkill(SKILLS_ROOT, 100_000);
         write("references/big.md", big(200_000));
         write("references/small.md", "# Small\n");
         const result = read();
         const view = viewOf(workflowOf(result));

         expect(view.skills.map((f) => f.relative_filepath)).toEqual(
            expect.arrayContaining([
               "second/SKILL.md",
               "pricing/SKILL.md",
               "pricing/references/small.md",
            ]),
         );
         expect(view.skills.map((f) => f.relative_filepath)).not.toContain(
            "pricing/references/big.md",
         );
         expect(view.assets.map((a) => a.relative_filepath)).toContain(
            "pricing/references/big.md",
         );
         expect(view.warnings.join("\n")).toContain(
            `Package skill file '${SKILL}/references/big.md' would take the skill set's Markdown over the ${SKILL_SET_MARKDOWN_MAX_BYTES} byte cap`,
         );
         // SKILL.md is re-emitted, so its bytes are a little off the raw ones the cap counts.
         const inlineBytes = view.skills.reduce(
            (n, f) => n + Buffer.byteLength(f.file_contents),
            0,
         );
         expect(inlineBytes).toBeLessThanOrEqual(
            SKILL_SET_MARKDOWN_MAX_BYTES + 1024,
         );
      });

      it("sets aside, in path order, a skill whose SKILL.md would pass the cap, and still serves the workflow", () => {
         write(
            "SKILL.md",
            `---\nname: pricing\ndescription: d\n---\n\n${"x".repeat(SKILL_SET_MARKDOWN_MAX_BYTES - 100)}\n`,
         );
         writeSecondSkill(SKILLS_ROOT, 1000);
         const result = read();
         expect(
            workflowOf(result).sets[0]!.skills.map((s) => s.dirName),
         ).toEqual(["pricing"]);
         expect(result.warnings.join("\n")).toContain(
            `Package skill '${SKILLS_ROOT}/second/SKILL.md' would take the skill set's Markdown over the ${SKILL_SET_MARKDOWN_MAX_BYTES} byte cap, so it is not served`,
         );
      });

      it.skipIf(isWindows)(
         "charges nothing for a skill the strict assets/ rules set aside, so a later skill that fits without it is served",
         () => {
            write(
               "SKILL.md",
               `---\nname: pricing\ndescription: d\n---\n\n${"x".repeat(200_000)}\n`,
            );
            fs.symlinkSync(
               path.join(pkg, "publisher.json"),
               path.join(skillDir(), "assets/link.txt"),
            );
            writeSecondSkill(SKILLS_ROOT, 100_000);
            const result = read();
            expect(
               workflowOf(result).sets[0]!.skills.map((s) => s.dirName),
            ).toEqual(["second"]);
            const findings = result.warnings.join("\n");
            expect(findings).toContain(
               `Package skill '${SKILL}' is not served: asset '${SKILL}/assets/link.txt' is a link`,
            );
            expect(findings).not.toContain("byte cap");
         },
      );

      it("gives each skill set a budget of its own", () => {
         write(
            "SKILL.md",
            `---\nname: pricing\ndescription: d\n---\n\n${"x".repeat(SKILL_SET_MARKDOWN_MAX_BYTES - 1000)}\n`,
         );
         writeSecondSkill(SKILLS_ROOT_B, 100_000);
         const result = read({
            analyst: {
               ...workflows.analyst,
               skillSets: [SKILLS_ROOT, SKILLS_ROOT_B],
            },
         });
         expect(
            workflowOf(result).sets.map(({ skills }) =>
               skills.map((s) => s.dirName),
            ),
         ).toEqual([["pricing"], ["second"]]);
         expect(result.warnings.join("\n")).not.toContain("byte cap");
      });
   });
});

describe("a skill set's own skills", () => {
   let pkg: string;
   const setDir = () => path.join(pkg, "skills");
   const writeSkillMd = (dir: string, contents: string) => {
      fs.mkdirSync(path.join(setDir(), dir), { recursive: true });
      fs.writeFileSync(path.join(setDir(), dir, "SKILL.md"), contents);
   };
   /** The skill set at `skills/`, read as the default workflow lists it. */
   const readSet = () => {
      const result = readPackageWorkflows(pkg, {
         default: { skillSets: ["skills"] },
      });
      const set = result.sets.get("skills")!;
      return { result, set, skills: set.skills, warnings: set.findings };
   };

   beforeEach(() => {
      pkg = fs.mkdtempSync(path.join(os.tmpdir(), "package-skills-dir-"));
   });

   afterEach(() => {
      fs.rmSync(pkg, { recursive: true, force: true });
   });

   it("builds a catalog entry per skill: its frontmatter as JSON and every served file's digest and size", () => {
      writeSkill(pkg, "skills");
      const { skills, warnings } = readSet();
      const catalog = skills.map((s) => s.catalog);
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
         "reference/rule-of-50.md",
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
      const { result, skills } = readSet();
      expect(skills.map((s) => s.served.name)).toEqual(["pricing"]);
      const inline = skills.flatMap((s) =>
         s.inline.map((f) => `${s.rel}/${f.relative}`),
      );
      expect(inline).toEqual([
         "skills/pricing/SKILL.md",
         "skills/pricing/NOTES.md",
         "skills/pricing/reference/rule-of-50.md",
         "skills/pricing/references/a.md",
      ]);
      for (const file of inline) expect(result.paths).toContain(file);
   });

   it("hashes a set's skills from the shas the walk took, reading no file", () => {
      writeSkill(pkg, "skills");
      const { result, skills } = readSet();
      const read = spyOn(fs, "readFileSync");
      try {
         computeSourceContentSha(result.contentShas);
         const hashed = read.mock.calls.map((c) => String(c[0]));
         const inline = skills.flatMap((s) =>
            s.inline.map((f) => `${s.rel}/${f.relative}`),
         );
         for (const file of inline) {
            expect(hashed.some((p) => p.endsWith(file))).toBe(false);
         }
         expect(inline.length).toBeGreaterThan(0);
      } finally {
         read.mockRestore();
      }
   });

   it("reads every file it lists by its path: Markdown verbatim, a script as text, a binary as a message", async () => {
      writeSkill(pkg, "skills");
      const [skill] = readSet().skills;
      const { read } = skill!.served;
      expect(await read("SKILL.md")).toEqual({
         text: String(FILES["SKILL.md"]),
      });
      expect(await read("reference/rule-of-50.md")).toEqual({
         text: String(FILES["reference/rule-of-50.md"]),
      });
      expect(await read("scripts/run.py")).toEqual({
         text: String(FILES["scripts/run.py"]),
      });
      expect(await read("docs/Guide.MD")).toEqual({
         text: String(FILES["docs/Guide.MD"]),
      });
      expect(await read("assets/logo.png")).toEqual({ binary: true });
   });

   it("answers a listed file that changed on disk since the load as changed, never its new bytes", async () => {
      writeSkill(pkg, "skills");
      const [skill] = readSet().skills;
      fs.writeFileSync(path.join(setDir(), "pricing/scripts/run.py"), "x");
      expect(await skill!.served.read("scripts/run.py")).toEqual({
         changed: true,
      });
   });

   it("lists and serves a root file named __proto__", async () => {
      writeSkillMd("odd", "---\nname: odd\ndescription: d\n---\n\nbody\n");
      fs.writeFileSync(path.join(setDir(), "odd/__proto__"), "hi\n");
      const [skill] = readSet().skills;
      expect(Object.hasOwn(skill!.catalog.files, "__proto__")).toBe(true);
      expect(skillFileList(skill!.served).map((f) => f.path)).toEqual([
         "SKILL.md",
         "__proto__",
      ]);
      expect(await skill!.served.read("__proto__")).toEqual({ text: "hi\n" });
   });

   it("lists a Markdown file that is not UTF-8 text as a binary file, never as lossy text", async () => {
      writeSkillMd("odd", "---\nname: odd\ndescription: d\n---\n\nbody\n");
      const dir = path.join(setDir(), "odd/reference");
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, "nul.md"), Buffer.from("a\0b"));
      fs.writeFileSync(path.join(dir, "latin.md"), Buffer.from([0x63, 0xe9]));
      const { skills, warnings } = readSet();
      const skill = skills[0]!;
      expect(Object.keys(skill.catalog.files)).toEqual([
         "SKILL.md",
         "reference/latin.md",
         "reference/nul.md",
      ]);
      expect(skill.catalog.files["reference/latin.md"]!.size).toBe(2);
      expect(await skill.served.read("reference/nul.md")).toEqual({
         binary: true,
      });
      expect(await skill.served.read("reference/latin.md")).toEqual({
         binary: true,
      });
      expect(warnings.join("\n")).toContain("not UTF-8 text");
   });

   it.skipIf(isWindows)(
      "does not read a listed file that became a link out of the skill",
      async () => {
         writeSkill(pkg, "skills");
         const [skill] = readSet().skills;
         // The target holds run.py's own bytes, so its digest still matches and only the link refusal can stop the read.
         const secret = path.join(pkg, "secret.txt");
         fs.writeFileSync(secret, String(FILES["scripts/run.py"]));
         const file = path.join(setDir(), "pricing/scripts/run.py");
         fs.rmSync(file);
         fs.symlinkSync(secret, file);
         expect(await skill!.served.read("scripts/run.py")).toEqual({
            changed: true,
         });
      },
   );

   it.skipIf(isWindows)(
      "refuses a listed file swapped for a FIFO with no writer, promptly",
      async () => {
         writeSkill(pkg, "skills");
         const [skill] = readSet().skills;
         const file = path.join(setDir(), "pricing/scripts/run.py");
         fs.rmSync(file);
         execFileSync("mkfifo", [file]);
         const started = performance.now();
         expect(await skill!.served.read("scripts/run.py")).toEqual({
            changed: true,
         });
         expect(performance.now() - started).toBeLessThan(2000);
      },
   );

   /** What the callback's `fs.promises.open` calls did: each handle's `read` lengths and its whole-file reads, with `stat` reporting `statSize` when given. */
   const observingOpens = async (
      statSize: number | undefined,
      run: () => Promise<void>,
   ) => {
      const real = fs.promises.open.bind(fs.promises);
      const opened: Array<{ requested: number[]; wholeReads: number }> = [];
      const open = spyOn(fs.promises, "open").mockImplementation(((
         ...args: Parameters<typeof fs.promises.open>
      ) =>
         real(...args).then((handle) => {
            const seen = { requested: [] as number[], wholeReads: 0 };
            opened.push(seen);
            const stat = handle.stat.bind(handle);
            const read = handle.read.bind(handle) as (
               ...a: unknown[]
            ) => Promise<unknown>;
            const readFile = handle.readFile.bind(handle) as (
               ...a: unknown[]
            ) => Promise<unknown>;
            handle.stat = (async (...a: Parameters<typeof stat>) => {
               const stats = await stat(...a);
               if (statSize !== undefined) stats.size = statSize;
               return stats;
            }) as typeof handle.stat;
            handle.read = ((...a: unknown[]) => {
               seen.requested.push(a[2] as number);
               return read(...a);
            }) as typeof handle.read;
            handle.readFile = ((...a: unknown[]) => {
               seen.wholeReads++;
               return readFile(...a);
            }) as typeof handle.readFile;
            return handle;
         })) as typeof fs.promises.open);
      try {
         await run();
      } finally {
         open.mockRestore();
      }
      return opened;
   };

   it("refuses a listed file that grew past the cap since the listing", async () => {
      writeSkill(pkg, "skills");
      const [skill] = readSet().skills;
      fs.writeFileSync(
         path.join(setDir(), "pricing/scripts/run.py"),
         Buffer.alloc(PACKAGE_ASSET_FILE_MAX_BYTES + 10, 1),
      );
      let answer: unknown;
      const opened = await observingOpens(undefined, async () => {
         answer = await skill!.served.read("scripts/run.py");
      });
      expect(answer).toEqual({ changed: true });
      expect(opened).toHaveLength(1);
      expect(opened[0]!.wholeReads).toBe(0);
      expect(
         opened[0]!.requested.reduce((a, b) => a + b, 0),
      ).toBeLessThanOrEqual(PACKAGE_ASSET_FILE_MAX_BYTES);
   });

   it("never takes more than the cap and one byte from a file that grows after its size was read", async () => {
      writeSkill(pkg, "skills");
      const [skill] = readSet().skills;
      fs.writeFileSync(
         path.join(setDir(), "pricing/scripts/run.py"),
         Buffer.alloc(PACKAGE_ASSET_FILE_MAX_BYTES * 2, 1),
      );
      let answer: unknown;
      // The handle reports the listed size, as a file would that grew between the stat and the read.
      const opened = await observingOpens(16, async () => {
         answer = await skill!.served.read("scripts/run.py");
      });
      expect(answer).toEqual({ changed: true });
      expect(opened).toHaveLength(1);
      expect(opened[0]!.wholeReads).toBe(0);
      expect(
         opened[0]!.requested.reduce((a, b) => a + b, 0),
      ).toBeLessThanOrEqual(PACKAGE_ASSET_FILE_MAX_BYTES);
   });

   it.skipIf(isWindows)(
      "refuses a listed file whose parent directory was swapped for a link to another directory holding a file of the same name",
      async () => {
         writeSkill(pkg, "skills");
         const [skill] = readSet().skills;
         const scripts = path.join(setDir(), "pricing/scripts");
         const elsewhere = path.join(pkg, "elsewhere");
         fs.mkdirSync(elsewhere);
         fs.writeFileSync(path.join(elsewhere, "run.py"), "print('swapped')\n");
         fs.renameSync(scripts, `${scripts}.orig`);
         fs.symlinkSync(elsewhere, scripts);
         expect(await skill!.served.read("scripts/run.py")).toEqual({
            changed: true,
         });
      },
   );

   it("reads a file of a workflow's skill without emitting its SKILL.md again, and emits that once", async () => {
      writeSkill(pkg, "skills");
      const [skill] = readSet().skills;
      let metaReads = 0;
      const counted = {
         ...skill!,
         meta: new Proxy(skill!.meta, {
            get(target, key, receiver) {
               metaReads++;
               return Reflect.get(target, key, receiver);
            },
         }),
      };
      for (const file of [
         "pricing/scripts/run.py",
         "pricing/NOTES.md",
         "pricing/references/a.md",
      ]) {
         expect(await readWorkflowFile(counted, file)).toMatchObject({
            text: expect.any(String),
         });
      }
      expect(metaReads).toBe(0);
      await readWorkflowFile(counted, "pricing/SKILL.md");
      const emitted = metaReads;
      expect(emitted).toBeGreaterThan(0);
      await readWorkflowFile(counted, "pricing/SKILL.md");
      await readWorkflowFile(counted, "pricing/NOTES.md");
      expect(metaReads).toBe(emitted);
   });

   it("lists exactly the files it serves: a skipped, ignored or built-in-excluded file is not in the catalog", () => {
      writeSkill(pkg, "skills");
      const [skill] = readSet().skills;
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
         fs.mkdirSync(path.join(setDir(), "many/scripts"), {
            recursive: true,
         });
         fs.writeFileSync(path.join(setDir(), `many/scripts/f${i}.py`), "x");
      }
      const { result, skills, warnings } = readSet();
      const catalog = skills.map((s) => s.catalog);
      expect(Object.keys(catalog[0]!.files)).toHaveLength(1 + 200);
      expect(result.contentShas.size).toBe(1 + 200);
      expect(warnings.join("\n")).toContain(
         "the skill set already has 200 assets, the per-set file cap",
      );
   });

   it("lists and hashes against budgets of its own: 4 MiB", () => {
      writeSkillMd("fat", "---\nname: fat\ndescription: d\n---\n\nbody\n");
      fs.mkdirSync(path.join(setDir(), "fat/data"), { recursive: true });
      for (let i = 0; i < 5; i++) {
         fs.writeFileSync(
            path.join(setDir(), `fat/data/f${i}.bin`),
            Buffer.alloc(900_000, 1),
         );
      }
      const { skills, warnings } = readSet();
      const catalog = skills.map((s) => s.catalog);
      expect(Object.keys(catalog[0]!.files)).toHaveLength(1 + 4);
      expect(warnings.join("\n")).toContain("f4.bin");
      expect(warnings.join("\n")).toContain(
         "it would take the skill set's assets over the 4194304 byte per-set cap",
      );
   });

   it("leaves out what a *-and-!SKILL.md allowlist leaves out, and says nothing", () => {
      writeSkillMd("only", "---\nname: only\ndescription: d\n---\n\nbody\n");
      fs.writeFileSync(path.join(setDir(), "only/extra.py"), "x");
      fs.writeFileSync(
         path.join(setDir(), "only/.skillignore"),
         "*\n!SKILL.md\n",
      );
      const { skills, warnings } = readSet();
      const catalog = skills.map((s) => s.catalog);
      expect(Object.keys(catalog[0]!.files)).toEqual(["SKILL.md"]);
      expect(warnings).toEqual([]);
   });

   it("does not serve a skill whose SKILL.md is a link out of the package, nor its directory's files", () => {
      const outside = fs.mkdtempSync(path.join(os.tmpdir(), "outside-md-"));
      try {
         fs.writeFileSync(path.join(outside, "secret.md"), "SECRET");
         fs.mkdirSync(path.join(setDir(), "bad"), { recursive: true });
         fs.symlinkSync(
            path.join(outside, "secret.md"),
            path.join(setDir(), "bad/SKILL.md"),
         );
         fs.writeFileSync(path.join(setDir(), "bad/run.py"), "x");
         const { result, skills, warnings } = readSet();
         expect(skills).toEqual([]);
         expect(result.paths).toEqual([]);
         expect(result.contentShas.size).toBe(0);
         expect(warnings.join("\n")).toContain("outside the package");
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
         const { skills, warnings } = readSet();
         expect(skills[0]!.served.description).toBe("one two");
         expect(warnings).toEqual([]);
      });

      it("warns, and reads name and description line by line, when the YAML does not parse", () => {
         writeSkillMd(
            "loose",
            "---\nname: loose\ndescription: Use when: a thing happens.\n---\n\nbody\n",
         );
         const { skills, warnings } = readSet();
         expect(skills[0]!.served.description).toBe(
            "Use when: a thing happens.",
         );
         expect(skills[0]!.catalog.frontmatter).toEqual({});
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("skills/loose/SKILL.md");
         expect(warnings[0]).toContain("not valid YAML");
      });

      it("warns about a name outside the spec's character set, and still serves a skill whose directory has that name", () => {
         writeSkillMd(
            "My_Skill",
            "---\nname: My_Skill\ndescription: d\n---\n\nbody\n",
         );
         const { skills, warnings } = readSet();
         expect(skills.map((s) => s.served.name)).toEqual(["My_Skill"]);
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
         instructions: "workflows/analyst/instructions.md",
         skillSets: roots,
      },
   });
   const read = (workflows: unknown = manifest()) =>
      readPackageWorkflows(pkg, workflows);
   /** The analyst workflow's first skill set, as the wire carries it. */
   const viewOf = (result = read()) =>
      workflowSkillSet(result.workflows.get("analyst")!.sets[0]!, "full");
   type SetView = ReturnType<typeof viewOf>;
   const inline = (view: SetView) =>
      view.skills.map((f) => f.relative_filepath);
   const listed = (view: SetView) =>
      (view.assets ?? []).map((a) => a.relative_filepath);
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
      write("workflows/analyst/instructions.md", "Be careful.");
   });

   afterEach(() => {
      fs.rmSync(pkg, { recursive: true, force: true });
   });

   it("keeps reference/*.md when a thousand entries sit in a folder beside it", () => {
      skill("s");
      write(`${ROOT}/s/reference/keep.md`, "# keep\n");
      for (let i = 0; i < 1000; i++) write(`${ROOT}/s/examples/e${i}.txt`);
      const result = read();
      expect(inline(viewOf(result))).toContain("s/reference/keep.md");
      expect(result.warnings.join("\n")).toContain("more than 1000 entries");
   });

   it("keeps a later skill's assets/ when an earlier skill's scripts fill the file cap", () => {
      skill("aa");
      skill("bb");
      for (let i = 0; i < 200; i++) write(`${ROOT}/aa/scripts/f${pad(i)}.py`);
      write(`${ROOT}/bb/assets/keep.txt`, "keep");
      const view = viewOf();
      expect(listed(view)).toContain("bb/assets/keep.txt");
      expect(listed(view)).toHaveLength(200);
   });

   it("keeps assets/ when a folder that sorts before it fills the file cap", () => {
      skill("s");
      for (let i = 0; i < 200; i++) write(`${ROOT}/s/Examples/f${pad(i)}.txt`);
      write(`${ROOT}/s/assets/keep.txt`, "keep");
      expect(listed(viewOf())).toContain("s/assets/keep.txt");
   });

   it("keeps a text asset when binaries beside it fill the file cap", () => {
      skill("s");
      for (let i = 0; i < 200; i++) {
         write(`${ROOT}/s/assets/a${pad(i)}.png`, Buffer.from([0x89, 0, 1, 2]));
      }
      write(`${ROOT}/s/assets/z.txt`, "keep");
      const view = viewOf();
      expect(listed(view)).toContain("s/assets/z.txt");
      expect(listed(view)).toHaveLength(200);
   });

   it("keeps reference/*.md inline when a larger Markdown file beside SKILL.md would take its bytes", () => {
      skill("s");
      write(`${ROOT}/s/README.md`, `#\n${"r".repeat(230_000)}`);
      write(`${ROOT}/s/reference/keep.md`, `#\n${"k".repeat(50_000)}`);
      const result = read();
      const view = viewOf(result);
      expect(inline(view)).toContain("s/reference/keep.md");
      expect(inline(view)).not.toContain("s/README.md");
      expect(listed(view)).toContain("s/README.md");
      expect(result.warnings.join("\n")).toContain("s/README.md");
   });

   it("lists a Markdown file over the inline cap instead of dropping it", () => {
      skill("s");
      write(`${ROOT}/s/references/big.md`, `#\n${"x".repeat(300 * 1024)}`);
      const result = read();
      const view = viewOf(result);
      expect(inline(view)).not.toContain("s/references/big.md");
      expect(listed(view)).toContain("s/references/big.md");
      expect(result.warnings.join("\n")).toContain(
         `s/references/big.md' is ${Buffer.byteLength(`#\n${"x".repeat(300 * 1024)}`)} bytes, over the ${PACKAGE_SKILL_FILE_MAX_BYTES} byte per-file cap, so it is listed instead of inlined`,
      );
   });

   it("takes the newly covered entries of all a skill set's skills from one budget of 1,000, and still serves each skill's own folders", () => {
      for (const name of ["a", "b", "c"]) {
         skill(name);
         write(`${ROOT}/${name}/reference/keep.md`, "# keep\n");
         write(`${ROOT}/${name}/assets/keep.txt`, "keep");
         for (let i = 0; i < 600; i++) {
            write(`${ROOT}/${name}/data/f${pad(i)}.txt`);
         }
      }
      const result = read();
      const view = viewOf(result);
      for (const name of ["a", "b", "c"]) {
         expect(inline(view)).toContain(`${name}/reference/keep.md`);
         expect(listed(view)).toContain(`${name}/assets/keep.txt`);
      }
      const stopped = result.warnings.filter((w) =>
         w.includes("skill walk stopped"),
      );
      expect(stopped).toHaveLength(2);
      expect(stopped[0]).toContain(`${ROOT}/b'`);
      expect(stopped[0]).toContain("more than 1000 entries were walked");
      expect(stopped[1]).toContain(`${ROOT}/c'`);
      expect(stopped[1]).toContain(
         "the skill set's walk budget of 1000 entries was already spent on earlier skills",
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
         const view = viewOf();
         expect(inline(view)).toEqual(["s/SKILL.md", "s/reference/a.md"]);
         expect(listed(view)).toEqual([]);
         expect(
            Object.keys(
               readPackageWorkflows(pkg, manifest()).sets.get(ROOT)!.skills[0]!
                  .catalog.files,
            ),
         ).toEqual(["SKILL.md", "reference/a.md"]);
      });

      it("is not served, with a warning, when it leads out of the package", () => {
         const outside = fs.mkdtempSync(path.join(os.tmpdir(), "ref-out-"));
         try {
            fs.writeFileSync(path.join(outside, "a.md"), "# Out\n");
            skill("s");
            fs.symlinkSync(outside, path.join(pkg, ROOT, "s/reference"));
            const result = read();
            expect(inline(viewOf(result))).toEqual(["s/SKILL.md"]);
            expect(result.warnings.join("\n")).toContain("s/reference");
            // What lies beyond the link is not looked at, so it is not named either.
            expect(result.warnings.join("\n")).not.toContain("a.md");
         } finally {
            fs.rmSync(outside, { recursive: true, force: true });
         }
      });

      it("is skipped with a warning, and the workflow is served, when it leads nowhere", () => {
         skill("s");
         fs.symlinkSync(
            path.join(pkg, "gone"),
            path.join(pkg, ROOT, "s/reference"),
         );
         const result = read();
         expect(inline(viewOf(result))).toEqual(["s/SKILL.md"]);
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
         expect(listed(viewOf(result))).toEqual([]);
         expect(result.warnings.join("\n")).toContain(
            "s/scripts' is not served: it is a link",
         );
      });
   });

   it("walks a skill set that eight workflows share once, not once per workflow", () => {
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
         expect(result.workflows.size).toBe(8);
         expect(result.sets.size).toBe(1);
         // The spy must see the walk, or the comparison below is vacuous.
         expect(calls1).toBeGreaterThanOrEqual(32);
         // Each workflow adds a few directory reads for its own declaration; walking the set eight times would be near 8x.
         expect(calls8).toBeLessThan(4 * calls1);
      } finally {
         listings.mockRestore();
      }
   });

   it("charges the package's 8 MiB to the skill sets in sorted path order, whatever order a workflow lists them", () => {
      const nearCap = Buffer.alloc(1024 * 1024 - 1, "a");
      const roots = ["skills-a", "skills-b", "skills-c"];
      for (const name of ["a", "b", "c"]) {
         const root = `skills-${name}`;
         skill(`s${name}`, root);
         if (name === "c")
            write(`${root}/sc/assets/keep.txt`, "k".repeat(10_000));
         else {
            for (let i = 0; i < 4; i++)
               write(`${root}/s${name}/data/f${i}.txt`, nearCap);
         }
      }
      const forward = read(manifest(roots));
      const backward = read(manifest([...roots].reverse()));
      for (const result of [forward, backward]) {
         const files = (root: string) =>
            result.sets
               .get(root)!
               .skills.flatMap((s) => s.listed)
               .map((f) => f.relative_filepath);
         expect(files("skills-a")).toHaveLength(4);
         expect(files("skills-b")).toHaveLength(4);
         expect(files("skills-c")).toEqual([]);
         expect(result.warnings.join("\n")).toContain("per-package cap");
      }
      expect(backward.sets.get("skills-c")!.digest).toBe(
         forward.sets.get("skills-c")!.digest,
      );
   });

   describe("a binary file under assets/", () => {
      // What a read takes of a file in this call, by its file descriptor or whole.
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
            expect(listed(viewOf()).length).toBeGreaterThan(0);
         });
         // The set's 4 MiB holds thirteen of them; every other is looked at for 8 KiB and skipped.
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
         const view = viewOf();
         expect(listed(view)).toContain("s/assets/z.txt");
         expect(listed(view)).toHaveLength(200);
         expect(listed(view)).not.toContain("s/assets/a199.bin");
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
         const references = inline(viewOf(result)).filter((f) =>
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
            inline(viewOf(result)).filter((f) => f.startsWith("s/reference/")),
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
         expect(inline(viewOf())).toContain("s/reference/overview.md");
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
               expect(read().workflows.has("analyst")).toBe(true);
            });
            // The spies must see the listing, or the bound below is vacuous.
            expect(largest).toBeGreaterThanOrEqual(REFERENCE_MAX_EXAMINED);
            expect(largest).toBeLessThanOrEqual(REFERENCE_MAX_EXAMINED + 1);
         });

         it("still sets the skill aside when it holds a publisher.json", () => {
            skill("s");
            write("shared-ref/a.md", "# A\n");
            write("shared-ref/publisher.json", "{}");
            linkReference("shared-ref");
            const result = read();
            expect(result.workflows.get("analyst")!.sets[0]!.skills).toEqual(
               [],
            );
            expect(result.warnings.join("\n")).toContain(
               `'${ROOT}/s/reference/publisher.json' is a publisher.json`,
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

      it("is listed for no more than 10,001 entries, and still sets the skill aside", () => {
         skill("s");
         fillBig();
         linkAssets("s", "shared-big");
         let result: ReturnType<typeof read> | undefined;
         const largest = largestListing(() => {
            result = read();
         });
         expect(result!.workflows.get("analyst")!.sets[0]!.skills).toEqual([]);
         expect(result!.warnings.join("\n")).toContain(
            `'${ROOT}/s/assets' is a link`,
         );
         // The spies must see the listing, or the bound below is vacuous.
         expect(largest).toBeGreaterThanOrEqual(REFERENCE_MAX_EXAMINED);
         expect(largest).toBeLessThanOrEqual(REFERENCE_MAX_EXAMINED + 1);
      });

      // A dot-named directory is not a skill and is never scanned, so what sits in it neither refuses the workflow nor sets anything aside.
      it("does not read a publisher.json behind it in a dot-named directory, and serves the workflow", () => {
         skill("s");
         write(`${ROOT}/.old/SKILL.md`, "x");
         write("shared-assets/publisher.json", "{}");
         linkAssets(".old", "shared-assets");
         const result = read();
         expect(result.workflows.has("analyst")).toBe(true);
         expect(result.warnings.join("\n")).not.toContain(".old");
      });
   });
});
