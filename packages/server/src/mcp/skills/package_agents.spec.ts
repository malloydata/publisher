// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import { createHash } from "crypto";
import path from "path";
import {
   PACKAGE_AGENT_SKILLS_MAX_BYTES,
   PACKAGE_AGENT_TEXT_MAX_BYTES,
   readPackageAgents,
   summarizeAgentSkillFile,
} from "./package_skills";

describe("readPackageAgents", () => {
   let pkg: string;
   let outside: string;

   beforeEach(() => {
      pkg = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-agents-"));
      outside = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-agents-out-"));
   });

   afterEach(() => {
      fs.rmSync(pkg, { recursive: true, force: true });
      fs.rmSync(outside, { recursive: true, force: true });
   });

   const write = (rel: string, text: string) => {
      fs.mkdirSync(path.dirname(path.join(pkg, rel)), { recursive: true });
      fs.writeFileSync(path.join(pkg, rel), text);
   };

   /** A complete, valid agent on disk; returns its manifest entry. */
   const analyst = (extra: Record<string, unknown> = {}) => {
      write("agents/analyst/instructions.md", "Be careful with revenue.");
      write(
         "agents/analyst/skills/conventions/SKILL.md",
         "---\nname: conventions\ndescription: House rules.\nallowed-tools: Bash\nx-owner: me\n---\n\nUse net revenue.\n",
      );
      write(
         "agents/analyst/skills/conventions/reference/glossary.md",
         "# Glossary\n\nnet = after refunds\n",
      );
      write("agents/analyst/tasks/weekly.md", "Summarize last week.");
      return {
         description: "Answers revenue questions",
         instructions: "agents/analyst/instructions.md",
         skills: ["agents/analyst/skills"],
         schedules: [
            { cron: "0 13 * * MON", task: "agents/analyst/tasks/weekly.md" },
         ],
         ...extra,
      };
   };

   const read = (agents: unknown) => readPackageAgents(pkg, agents);

   const dropped = (agents: unknown, name = "analyst") => {
      const result = read(agents);
      expect(result.agents.has(name)).toBe(false);
      expect(result.warnings).toHaveLength(1);
      expect(result.warnings[0]).toContain(`Package agent '${name}'`);
      expect(result.warnings[0]).toContain("Fix:");
      return result.warnings[0]!;
   };

   it("returns nothing and no warning when the manifest declares no agents", () => {
      expect(read(undefined)).toEqual({
         agents: new Map(),
         paths: [],
         contentShas: new Map(),
         warnings: [],
      });
   });

   describe("budget", () => {
      it("serves a valid budget, and none when the manifest sets none", () => {
         const budget = {
            turns: 40,
            wall_clock_minutes: 90,
            llm_tokens: 2_000_000,
            job_minutes: 30,
         };
         const agents = read({ analyst: analyst({ budget }) });
         expect(agents.warnings).toEqual([]);
         expect(agents.agents.get("analyst")!.budget).toEqual(budget);
         expect(
            read({ analyst: analyst() }).agents.get("analyst")!.budget,
         ).toBe(undefined);
      });

      it.each(["turns", "wall_clock_minutes", "llm_tokens", "job_minutes"])(
         "serves a budget that sets only %s, as written",
         (key) => {
            const result = read({ analyst: analyst({ budget: { [key]: 5 } }) });
            expect(result.warnings).toEqual([]);
            expect(result.agents.get("analyst")!.budget).toEqual({ [key]: 5 });
         },
      );

      it("moves definitionSha when the budget changes", () => {
         const sha = (extra: Record<string, unknown>) =>
            read({ analyst: analyst(extra) }).agents.get("analyst")!
               .definitionSha;
         expect(sha({ budget: { turns: 5 } })).not.toBe(
            sha({ budget: { turns: 6 } }),
         );
         expect(sha({ budget: { turns: 5 } })).not.toBe(sha({}));
      });

      it.each([
         ["zero", { turns: 0 }],
         ["negative", { llm_tokens: -1 }],
         ["fractional", { wall_clock_minutes: 1.5 }],
         ["a string", { job_minutes: "30" }],
         ["null", { turns: null }],
         ["not finite", { turns: Infinity }],
      ])("drops an agent whose budget value is %s", (_label, budget) => {
         expect(dropped({ analyst: analyst({ budget }) })).toContain(
            "positive integer",
         );
      });

      it.each([
         ["a list", [1]],
         ["a string", "big"],
         ["null", null],
      ])("drops an agent whose budget is %s", (_label, budget) => {
         expect(dropped({ analyst: analyst({ budget }) })).toContain(
            "'budget' must be an object",
         );
      });

      it("drops an agent whose budget sets an unknown sub-key, naming it", () => {
         const warning = dropped({
            analyst: analyst({ budget: { turns: 5, cost_usd: 3 } }),
         });
         expect(warning).toContain("cost_usd");
         expect(warning).toContain("turns");
      });

      it.each([{}, { "x-why": "none yet" }])(
         "serves no budget for an empty one (%j)",
         (budget) => {
            const result = read({ analyst: analyst({ budget }) });
            expect(result.warnings).toEqual([]);
            const agent = result.agents.get("analyst")!;
            expect(agent.budget).toBeUndefined();
            expect(agent.definitionSha).toBe(
               read({ analyst: analyst() }).agents.get("analyst")!
                  .definitionSha,
            );
         },
      );

      it("ignores an x- note inside a budget", () => {
         const result = read({
            analyst: analyst({ budget: { turns: 5, "x-why": "cheap" } }),
         });
         expect(result.warnings).toEqual([]);
         expect(result.agents.get("analyst")!.budget).toEqual({ turns: 5 });
      });
   });

   it("resolves a full agent to the contract shape", () => {
      const result = read({ analyst: analyst() });
      expect(result.warnings).toEqual([]);
      const agent = result.agents.get("analyst")!;
      expect(agent).toMatchObject({
         name: "analyst",
         description: "Answers revenue questions",
         model: "inherit",
         instructions: "Be careful with revenue.",
         warnings: [],
         schedules: [
            {
               cron: "0 13 * * MON",
               task: "agents/analyst/tasks/weekly.md",
               taskContent: "Summarize last week.",
            },
         ],
      });
      expect(agent.definitionSha).toMatch(/^[0-9a-f]{64}$/);
      expect(result.paths.sort()).toEqual([
         "agents/analyst/instructions.md",
         "agents/analyst/skills/conventions/SKILL.md",
         "agents/analyst/skills/conventions/reference/glossary.md",
         "agents/analyst/tasks/weekly.md",
      ]);
   });

   it("re-emits SKILL.md with only name and description and serves references raw, sorted", () => {
      const { skills } = read({ analyst: analyst() }).agents.get("analyst")!;
      expect(skills.map((s) => s.relative_filepath)).toEqual([
         "conventions/SKILL.md",
         "conventions/reference/glossary.md",
      ]);
      expect(skills[0]!.file_contents).toBe(
         '---\nname: "conventions"\ndescription: "House rules."\n---\n\nUse net revenue.\n',
      );
      expect(skills[1]!.file_contents).toBe(
         "# Glossary\n\nnet = after refunds\n",
      );
   });

   it("honors model and ignores x- keys on the agent and on a schedule", () => {
      const entry = analyst({
         model: "sonnet",
         "x-owner": "data team",
         schedules: [
            {
               cron: "0 13 * * MON",
               task: "agents/analyst/tasks/weekly.md",
               "x-note": "hi",
            },
         ],
      });
      const result = read({ analyst: entry });
      expect(result.warnings).toEqual([]);
      expect(result.agents.get("analyst")!.model).toBe("sonnet");
   });

   it("drops only the bad agent and keeps the others", () => {
      const good = analyst();
      const result = read({ analyst: good, "Bad Name": good, other: good });
      expect([...result.agents.keys()]).toEqual(["analyst", "other"]);
      expect(result.warnings).toHaveLength(1);
      expect(result.warnings[0]).toContain("'Bad Name'");
   });

   describe("manifest shape", () => {
      it.each([[[]], ["x"], [5], [null]])(
         "warns and serves nothing when agents is %p",
         (value) => {
            const result = read(value);
            expect(result.agents.size).toBe(0);
            expect(result.warnings[0]).toContain("must be an object");
            expect(result.warnings[0]).toContain("Fix:");
         },
      );

      it.each([
         ["Upper"],
         ["-lead"],
         ["trail-"],
         ["dou--ble"],
         ["under_score"],
         ["a".repeat(65)],
         [""],
      ])("drops an agent named %p", (name) => {
         const result = read({ [name]: analyst() });
         expect(result.agents.size).toBe(0);
         expect(result.warnings).toHaveLength(1);
      });

      it("does not treat __proto__ as an agent", () => {
         const raw = JSON.parse(`{"__proto__": {"description": "d"}}`);
         expect(read(raw).agents.size).toBe(0);
      });

      it.each([
         ["tools", ["Bash"]],
         ["mcp-servers", {}],
         ["hooks", {}],
         ["permission-mode", "bypass"],
         ["base", "other"],
         ["surprise", 1],
      ])("drops an agent that sets %p", (key, value) => {
         expect(dropped({ analyst: analyst({ [key]: value }) })).toContain(
            `'${key}'`,
         );
      });

      it("drops an agent whose schedule sets an unknown key", () => {
         dropped({
            analyst: analyst({
               schedules: [
                  {
                     cron: "0 13 * * MON",
                     task: "agents/analyst/tasks/weekly.md",
                     timezone: "UTC",
                  },
               ],
            }),
         });
      });

      it.each([["0 13 * *"], ["*/5 * * * * *"], ["0 6 L * *"], ["nonsense"]])(
         "drops an agent with cron %p",
         (cron) => {
            dropped({
               analyst: analyst({
                  schedules: [{ cron, task: "agents/analyst/tasks/weekly.md" }],
               }),
            });
         },
      );

      it.each([
         ["description", undefined],
         ["description", ""],
         ["description", 3],
         ["instructions", undefined],
         ["model", 3],
         ["model", ""],
         ["skills", "agents/analyst/skills"],
         ["schedules", {}],
      ])("drops an agent whose %s is %p", (key, value) => {
         dropped({ analyst: analyst({ [key]: value }) });
      });
   });

   describe("paths", () => {
      it.each([
         ["an absolute path", "/etc/passwd.md"],
         ["a parent escape", "../outside.md"],
         ["a NUL byte", "agents/a\0.md"],
         ["a non-markdown file", "publisher.json"],
         ["a data file", "data.csv"],
         ["a missing file", "agents/analyst/missing.md"],
      ])("drops an agent whose instructions is %s", (_label, instructions) => {
         write("publisher.json", "{}");
         write("data.csv", "a,b");
         dropped({ analyst: analyst({ instructions }) });
      });

      it("drops an agent whose instructions is a link out of the package", () => {
         write("agents/analyst/x.md", "x");
         fs.writeFileSync(path.join(outside, "secret.md"), "SECRET");
         const entry = analyst();
         fs.rmSync(path.join(pkg, entry.instructions));
         fs.symlinkSync(
            path.join(outside, "secret.md"),
            path.join(pkg, entry.instructions),
         );
         expect(dropped({ analyst: entry })).toContain("outside the package");
      });

      it("drops an agent whose instructions is a .md link to publisher.json", () => {
         write("publisher.json", '{"name":"x"}');
         const entry = analyst();
         fs.rmSync(path.join(pkg, entry.instructions));
         fs.symlinkSync(
            path.join(pkg, "publisher.json"),
            path.join(pkg, entry.instructions),
         );
         expect(dropped({ analyst: entry })).toContain("not Markdown");
      });

      it("drops an agent whose task is a link out of the package", () => {
         const entry = analyst();
         fs.writeFileSync(path.join(outside, "secret.md"), "SECRET");
         const task = path.join(pkg, entry.schedules[0]!.task);
         fs.rmSync(task);
         fs.symlinkSync(path.join(outside, "secret.md"), task);
         expect(dropped({ analyst: entry })).toContain("outside the package");
      });

      it("drops an agent whose skills directory is a link out of the package", () => {
         const entry = analyst();
         fs.mkdirSync(path.join(outside, "s"));
         fs.writeFileSync(
            path.join(outside, "s", "SKILL.md"),
            "---\nname: s\ndescription: leaked\n---\nSECRET",
         );
         fs.rmSync(path.join(pkg, "agents/analyst/skills"), {
            recursive: true,
         });
         fs.symlinkSync(outside, path.join(pkg, "agents/analyst/skills"));
         expect(dropped({ analyst: entry })).toContain("outside the package");
      });

      it("drops an agent whose skill directory is a link out", () => {
         const entry = analyst();
         fs.mkdirSync(path.join(outside, "linked"));
         fs.writeFileSync(
            path.join(outside, "linked", "SKILL.md"),
            "---\nname: linked\ndescription: leaked\n---\nSECRET",
         );
         fs.symlinkSync(
            path.join(outside, "linked"),
            path.join(pkg, "agents/analyst/skills/linked"),
         );
         dropped({ analyst: entry });
      });

      it("drops an agent whose skills entry is missing, absolute or the package root", () => {
         for (const skills of [["nope"], ["/tmp"], ["."], ["../x"]]) {
            const result = read({ analyst: analyst({ skills }) });
            expect(result.agents.size).toBe(0);
         }
      });

      it("drops an agent whose instructions are over the cap, and serves one at the cap", () => {
         const entry = analyst();
         write(entry.instructions, "x".repeat(PACKAGE_AGENT_TEXT_MAX_BYTES));
         expect(read({ analyst: entry }).agents.size).toBe(1);
         write(
            entry.instructions,
            "x".repeat(PACKAGE_AGENT_TEXT_MAX_BYTES + 1),
         );
         expect(dropped({ analyst: entry })).toContain("byte cap");
      });

      it("drops an agent whose task is over the cap", () => {
         const entry = analyst();
         write(
            entry.schedules[0]!.task,
            "x".repeat(PACKAGE_AGENT_TEXT_MAX_BYTES + 1),
         );
         dropped({ analyst: entry });
      });

      it("drops an agent with a nested publisher.json beside its instructions", () => {
         const entry = analyst();
         write("agents/analyst/publisher.json", "{}");
         expect(dropped({ analyst: entry })).toContain("publisher.json");
      });

      it("drops an agent with a nested publisher.json deep in a skills directory", () => {
         const entry = analyst();
         write(
            "agents/analyst/skills/conventions/reference/publisher.json",
            "{}",
         );
         expect(dropped({ analyst: entry })).toContain("publisher.json");
      });

      it("does not walk a skills directory that is a link, so a publisher.json beyond it is not seen", () => {
         const entry = analyst();
         write(
            "shared/conventions/SKILL.md",
            "---\nname: conventions\ndescription: d\n---\nbody",
         );
         write("shared/conventions/other/publisher.json", "{}");
         fs.rmSync(path.join(pkg, "agents/analyst/skills"), {
            recursive: true,
         });
         fs.symlinkSync(
            path.join(pkg, "shared"),
            path.join(pkg, "agents/analyst/skills"),
         );
         expect(read({ analyst: entry }).agents.has("analyst")).toBe(true);
      });

      it("does not say whether a publisher.json exists beyond a link out of the package", () => {
         const entry = analyst();
         fs.writeFileSync(path.join(outside, "publisher.json"), "{}");
         fs.rmSync(path.join(pkg, "agents/analyst/skills"), {
            recursive: true,
         });
         fs.symlinkSync(outside, path.join(pkg, "agents/analyst/skills"));
         const warning = dropped({ analyst: entry });
         expect(warning).toContain("outside the package");
         expect(warning).not.toContain("nested");
      });

      it("serves an agent at the package root next to the package's own publisher.json", () => {
         write("publisher.json", "{}");
         write("brief.md", "hello");
         const result = read({
            root: { description: "d", instructions: "brief.md" },
         });
         expect(result.agents.get("root")!.instructions).toBe("hello");
      });

      it("accepts ./ and doubled slashes in a path, and hashes the same files", () => {
         const entry = analyst();
         const plain = read({ analyst: entry });
         const dotted = read({
            analyst: {
               ...entry,
               instructions: "./agents//analyst/instructions.md",
               skills: ["agents/analyst/skills/"],
            },
         });
         expect(dotted.paths.sort()).toEqual(plain.paths.sort());
      });
   });

   describe("skills", () => {
      it("drops an agent when two skills roots both hold the same skill", () => {
         const entry = analyst();
         write(
            "shared/conventions/SKILL.md",
            "---\nname: conventions\ndescription: d\n---\nbody",
         );
         expect(
            dropped({
               analyst: { ...entry, skills: [...entry.skills, "shared"] },
            }),
         ).toContain("collide");
      });

      it("merges distinct skills from several roots, sorted", () => {
         const entry = analyst();
         write("shared/aaa/SKILL.md", "---\nname: aaa\ndescription: d\n---\nb");
         const { skills } = read({
            analyst: { ...entry, skills: [...entry.skills, "shared"] },
         }).agents.get("analyst")!;
         expect(skills.map((s) => s.relative_filepath)).toEqual([
            "aaa/SKILL.md",
            "conventions/SKILL.md",
            "conventions/reference/glossary.md",
         ]);
      });

      it("drops an agent whose skill files exceed the total cap rather than truncating", () => {
         const entry = analyst();
         const chunk = "x".repeat(PACKAGE_AGENT_SKILLS_MAX_BYTES / 2 - 100);
         for (const name of ["a", "b", "c"]) {
            write(
               `agents/analyst/skills/${name}/SKILL.md`,
               `---\nname: ${name}\ndescription: d\n---\n${chunk}`,
            );
         }
         expect(dropped({ analyst: entry })).toContain("byte cap");
      });

      it("drops an agent whose skill has no readable SKILL.md", () => {
         const entry = analyst();
         fs.mkdirSync(path.join(pkg, "agents/analyst/skills/empty"));
         dropped({ analyst: entry });
      });

      it("serves an agent whose skill has no description, flagged on the agent", () => {
         write(
            "agents/analyst/skills/quiet/SKILL.md",
            "---\nname: quiet\n---\n\nbody\n",
         );
         const result = read({ analyst: analyst() });
         expect(result.agents.has("analyst")).toBe(true);
         expect(result.agents.get("analyst")!.warnings.join("\n")).toContain(
            "no description",
         );
         expect(result.warnings.join("\n")).toContain("'quiet'");
      });

      it("serves an agent whose skill reference cannot be listed, flagged on the agent", () => {
         write(
            "agents/analyst/skills/odd/SKILL.md",
            "---\nname: odd\ndescription: d\n---\n\nbody\n",
         );
         write("agents/analyst/skills/odd/reference", "not a directory");
         const result = read({ analyst: analyst() });
         expect(result.agents.has("analyst")).toBe(true);
         expect(result.warnings.join("\n")).toContain("cannot be listed");
      });

      it("drops an agent whose skill name is not its directory name", () => {
         write(
            "agents/analyst/skills/other-dir/SKILL.md",
            "---\nname: elsewhere\ndescription: d\n---\n\nbody\n",
         );
         expect(dropped({ analyst: analyst() })).toContain("'elsewhere'");
      });

      it("drops an agent whose skill directory is a link to a directory holding a publisher.json", () => {
         write(
            "shared/linked/SKILL.md",
            "---\nname: linked\ndescription: d\n---\n\nbody\n",
         );
         write("shared/linked/publisher.json", "{}");
         const entry = analyst();
         fs.symlinkSync(
            path.join(pkg, "shared/linked"),
            path.join(pkg, "agents/analyst/skills/linked"),
         );
         expect(dropped({ analyst: entry })).toContain("nested publisher.json");
      });

      it("serves an agent whose skills directory holds no skills, with a warning on the agent", () => {
         fs.mkdirSync(path.join(pkg, "agents/analyst/skills"), {
            recursive: true,
         });
         write("agents/analyst/instructions.md", "i");
         const result = read({
            analyst: {
               description: "d",
               instructions: "agents/analyst/instructions.md",
               skills: ["agents/analyst/skills"],
            },
         });
         expect(result.agents.get("analyst")!.warnings).toHaveLength(1);
         expect(result.warnings[0]).toContain("holds no skills");
      });
   });

   it("summarizes a skill with its parsed name and description, not the quoted emission", () => {
      write("agents/analyst/instructions.md", "i");
      write(
         "agents/analyst/skills/quoted/SKILL.md",
         '---\nname: quoted\ndescription: Say "net" and use C:\\data\n---\n\nbody\n',
      );
      const agent = read({
         analyst: {
            description: "d",
            instructions: "agents/analyst/instructions.md",
            skills: ["agents/analyst/skills"],
         },
      }).agents.get("analyst")!;
      const [file] = agent.skills;
      expect(summarizeAgentSkillFile(file!, agent.skillMeta)).toEqual({
         relative_filepath: "quoted/SKILL.md",
         name: "quoted",
         description: 'Say "net" and use C:\\data',
         bytes: Buffer.byteLength(file!.file_contents),
      });
   });

   it("drops an agent whose path uses a backslash", () => {
      const entry = analyst({ skills: ["agents\\analyst\\skills"] });
      expect(dropped({ analyst: entry })).toContain("backslash");
   });

   describe("definitionSha", () => {
      const sha = (agents: unknown, name = "analyst") =>
         read(agents).agents.get(name)!.definitionSha;

      it("is stable across reads and moves on every part of the definition", () => {
         const entry = analyst();
         const base = sha({ analyst: entry });
         expect(sha({ analyst: entry })).toBe(base);

         expect(sha({ analyst: { ...entry, model: "sonnet" } })).not.toBe(base);
         expect(sha({ analyst: { ...entry, description: "other" } })).not.toBe(
            base,
         );
         expect(
            sha({
               analyst: {
                  ...entry,
                  schedules: [
                     {
                        cron: "0 14 * * MON",
                        task: entry.schedules[0]!.task,
                     },
                  ],
               },
            }),
         ).not.toBe(base);
         expect(sha({ analyst: { ...entry, skills: [] } })).not.toBe(base);
         expect(sha({ analyst: { ...entry, "x-note": "n" } })).toBe(base);

         write("agents/analyst/tasks/weekly.md", "changed");
         const afterTask = sha({ analyst: entry });
         expect(afterTask).not.toBe(base);
         write("agents/analyst/instructions.md", "changed");
         const afterInstructions = sha({ analyst: entry });
         expect(afterInstructions).not.toBe(afterTask);
         write(
            "agents/analyst/skills/conventions/SKILL.md",
            "---\nname: conventions\ndescription: d\n---\nz",
         );
         expect(sha({ analyst: entry })).not.toBe(afterInstructions);
      });

      it("is per agent: editing one agent does not move another", () => {
         const a = analyst();
         write("agents/other/instructions.md", "other brief");
         const b = {
            description: "Other",
            instructions: "agents/other/instructions.md",
         };
         const before = sha({ analyst: a, other: b }, "other");
         write("agents/analyst/instructions.md", "edited");
         expect(sha({ analyst: a, other: b }, "other")).toBe(before);
      });
   });

   describe("assets", () => {
      const ASSETS = "agents/analyst/skills/conventions/assets";
      const MAX = 1024 * 1024;
      const LISTED = "conventions/assets";
      const agentOf = (result: ReturnType<typeof read>) =>
         result.agents.get("analyst")!;
      const listed = (result: ReturnType<typeof read>) =>
         (agentOf(result).assets ?? []).map((a) => a.relative_filepath);
      const warned = (result: ReturnType<typeof read>, ...needles: string[]) =>
         result.warnings.some((w) => needles.every((n) => w.includes(n)));
      const writeBytes = (rel: string, bytes: Buffer) => {
         fs.mkdirSync(path.dirname(path.join(pkg, rel)), { recursive: true });
         fs.writeFileSync(path.join(pkg, rel), bytes);
      };
      /** Every drop starts the way the service matches on it. */
      const droppedWith = (needle: string) => {
         const warning = dropped({ analyst: analyst() });
         expect(
            warning.startsWith("Package agent 'analyst' is not served:"),
         ).toBe(true);
         expect(warning).toContain(needle);
      };

      it("lists text assets with their size and sha, roots and package paths, and ignores dotfiles", () => {
         write(`${ASSETS}/index.html`, "<h1>hi</h1>");
         write(`${ASSETS}/css/app.css`, "body{}");
         write(`${ASSETS}/.hidden/x.txt`, "ignored");
         write(`${ASSETS}/.DS_Store`, "ignored");
         const result = read({ analyst: analyst() });

         expect(result.warnings).toEqual([]);
         expect(agentOf(result).assets).toEqual([
            {
               relative_filepath: `${LISTED}/css/app.css`,
               bytes: 6,
               sha256: createHash("sha256").update("body{}").digest("hex"),
            },
            {
               relative_filepath: `${LISTED}/index.html`,
               bytes: 11,
               sha256: createHash("sha256").update("<h1>hi</h1>").digest("hex"),
            },
         ]);
         expect(agentOf(result).asset_roots).toEqual({
            conventions: "agents/analyst/skills/conventions",
         });
         expect(result.paths).toContain(`${ASSETS}/css/app.css`);
         expect(result.contentShas.get(`${ASSETS}/index.html`)).toBe(
            agentOf(result).assets![1]!.sha256,
         );
         // Assets are listed, never served as skill text.
         expect(
            agentOf(result).skills.some((f) =>
               f.relative_filepath.includes("assets"),
            ),
         ).toBe(false);
      });

      it("serves an agent with no assets, or an empty assets directory, with neither key", () => {
         fs.mkdirSync(path.join(pkg, ASSETS), { recursive: true });
         const result = read({ analyst: analyst() });
         expect(result.warnings).toEqual([]);
         expect("assets" in agentOf(result)).toBe(false);
         expect("asset_roots" in agentOf(result)).toBe(false);
         expect(result.contentShas.size).toBe(0);
      });

      it("moves definitionSha when an asset changes and not otherwise", () => {
         const sha = () => agentOf(read({ analyst: analyst() })).definitionSha;
         const bare = sha();
         write(`${ASSETS}/a.txt`, "one");
         const one = sha();
         expect(one).not.toBe(bare);
         expect(sha()).toBe(one);
         write(`${ASSETS}/a.txt`, "two");
         expect(sha()).not.toBe(one);
      });

      it("accepts a file one byte under 1 MiB and refuses one at 1 MiB", () => {
         writeBytes(`${ASSETS}/under.txt`, Buffer.alloc(MAX - 1, "a"));
         writeBytes(`${ASSETS}/exact.txt`, Buffer.alloc(MAX, "a"));
         const result = read({ analyst: analyst() });
         expect(listed(result)).toEqual([`${LISTED}/under.txt`]);
         expect(warned(result, "exact.txt", `${MAX} byte file cap`)).toBe(true);
      });

      describe("skipped, with the agent still served", () => {
         it("skips non-UTF-8 and NUL-bearing files", () => {
            writeBytes(
               `${ASSETS}/logo.png`,
               Buffer.from([0x89, 0x50, 0xff, 0xfe]),
            );
            writeBytes(`${ASSETS}/nul.txt`, Buffer.from("a\0b"));
            write(`${ASSETS}/ok.txt`, "fine");
            const result = read({ analyst: analyst() });
            expect(listed(result)).toEqual([`${LISTED}/ok.txt`]);
            expect(warned(result, "logo.png", "not valid UTF-8")).toBe(true);
            expect(warned(result, "nul.txt", "NUL")).toBe(true);
         });

         it("keeps a leading BOM so the listed sha is the file's", () => {
            const bytes = Buffer.from("﻿hello");
            writeBytes(`${ASSETS}/bom.txt`, bytes);
            const result = read({ analyst: analyst() });
            expect(agentOf(result).assets![0]!.sha256).toBe(
               createHash("sha256").update(bytes).digest("hex"),
            );
         });

         it("skips Malloy files in any case", () => {
            write(`${ASSETS}/m.malloy`, "source: x is a");
            write(`${ASSETS}/n.MALLOYNB`, "{}");
            const result = read({ analyst: analyst() });
            expect(listed(result)).toEqual([]);
            expect(warned(result, "m.malloy", "Malloy")).toBe(true);
            expect(warned(result, "n.MALLOYNB", "Malloy")).toBe(true);
         });

         it("skips a bad segment name and a path over 200 characters", () => {
            write(`${ASSETS}/has space.txt`, "x");
            write(`${ASSETS}/_ok-1.2.txt`, "x");
            write(`${ASSETS}/-dash.txt`, "x");
            write(`${ASSETS}/${"a".repeat(200)}.txt`, "x");
            const result = read({ analyst: analyst() });
            expect(listed(result)).toEqual([`${LISTED}/_ok-1.2.txt`]);
            expect(warned(result, "has space.txt", "name")).toBe(true);
            expect(warned(result, "-dash.txt", "name")).toBe(true);
            expect(warned(result, "200 characters")).toBe(true);
         });

         it("stops at 200 files and 4 MiB per agent", () => {
            for (let i = 0; i < 201; i++) write(`${ASSETS}/f${i}.txt`, "x");
            const files = read({ analyst: analyst() });
            expect(listed(files)).toHaveLength(200);
            expect(warned(files, "per-agent file cap")).toBe(true);

            fs.rmSync(path.join(pkg, ASSETS), { recursive: true });
            for (let i = 0; i < 5; i++) {
               writeBytes(`${ASSETS}/big${i}.txt`, Buffer.alloc(900_000, "a"));
            }
            const bytes = read({ analyst: analyst() });
            expect(listed(bytes)).toHaveLength(4);
            expect(warned(bytes, "big4.txt", "per-agent cap")).toBe(true);
         });

         it("walks eight levels and warns about a ninth", () => {
            const seven = Array.from({ length: 7 }, (_, i) => `d${i}`).join(
               "/",
            );
            write(`${ASSETS}/${seven}/ok.txt`, "x");
            write(`${ASSETS}/${seven}/d7/deep.txt`, "x");
            const result = read({ analyst: analyst() });
            expect(listed(result)).toEqual([`${LISTED}/${seven}/ok.txt`]);
            expect(warned(result, "deeper than 8")).toBe(true);
         });

         it("keeps an agent served when its assets hold more than 1,000 entries", () => {
            for (let i = 0; i < 1001; i++) write(`${ASSETS}/n${i}.txt`, "x");
            const result = read({ analyst: analyst() });
            expect(result.agents.has("analyst")).toBe(true);
            expect(listed(result).length).toBeLessThanOrEqual(1000);
            expect(warned(result, "1000 entries")).toBe(true);
         });

         it("names the first twenty skips and counts the rest", () => {
            for (let i = 0; i < 30; i++) write(`${ASSETS}/m${i}.malloy`, "x");
            const result = read({ analyst: analyst() });
            expect(
               result.warnings.filter((w) => w.includes(".malloy")),
            ).toHaveLength(20);
            expect(warned(result, "10 further assets")).toBe(true);
         });
      });

      describe("drops the agent", () => {
         it("on a symlinked file, with the file named", () => {
            write("outside.txt", "x");
            fs.mkdirSync(path.join(pkg, ASSETS), { recursive: true });
            fs.symlinkSync(
               path.join(pkg, "outside.txt"),
               path.join(pkg, ASSETS, "link.txt"),
            );
            droppedWith("link.txt");
         });

         it("on a symlinked file that points outside the package", () => {
            fs.writeFileSync(path.join(outside, "secret.txt"), "s");
            fs.mkdirSync(path.join(pkg, ASSETS), { recursive: true });
            fs.symlinkSync(
               path.join(outside, "secret.txt"),
               path.join(pkg, ASSETS, "secret.txt"),
            );
            droppedWith("secret.txt");
         });

         it("on a symlink in a nested directory", () => {
            write(`${ASSETS}/sub/real.txt`, "x");
            fs.symlinkSync(
               path.join(pkg, ASSETS, "sub/real.txt"),
               path.join(pkg, ASSETS, "sub/alias.txt"),
            );
            droppedWith("alias.txt");
         });

         it("when assets is itself a symlink, out of the package or within it", () => {
            analyst();
            fs.mkdirSync(path.join(outside, "files"));
            fs.writeFileSync(path.join(outside, "files/x.txt"), "x");
            fs.symlinkSync(path.join(outside, "files"), path.join(pkg, ASSETS));
            droppedWith("conventions/assets");

            fs.rmSync(path.join(pkg, ASSETS));
            write("elsewhere/x.txt", "x");
            fs.symlinkSync(path.join(pkg, "elsewhere"), path.join(pkg, ASSETS));
            droppedWith("conventions/assets");
         });

         it("on a nested publisher.json or SKILL.md, in any case", () => {
            for (const name of [
               "publisher.json",
               "Publisher.JSON",
               "SKILL.md",
               "skill.md",
            ]) {
               fs.rmSync(path.join(pkg, ASSETS), {
                  recursive: true,
                  force: true,
               });
               write(`${ASSETS}/sub/${name}`, "{}");
               droppedWith(name);
            }
         });

         it("on '..' in a name", () => {
            write(`${ASSETS}/a..b.txt`, "x");
            droppedWith("a..b.txt");
         });

         it("on two assets that differ only by case", () => {
            write(`${ASSETS}/Logo.svg`, "x");
            write(`${ASSETS}/logo.svg`, "y");
            droppedWith("only by case");
         });

         it("on two directories that differ only by case", () => {
            write(`${ASSETS}/Dir/a.txt`, "x");
            write(`${ASSETS}/dir/b.txt`, "y");
            droppedWith("only by case");
         });
      });

      describe("package budget", () => {
         const writeSkill = (dir: string, files: number) => {
            write(
               `${dir}/s/SKILL.md`,
               "---\nname: s\ndescription: d\n---\n\nbody\n",
            );
            for (let i = 0; i < files; i++) {
               writeBytes(
                  `${dir}/s/assets/f${i}.txt`,
                  Buffer.alloc(1_000_000, "a"),
               );
            }
         };
         const agentAt = (dir: string) => {
            write(`${dir}-instructions.md`, "brief");
            return {
               description: "d",
               instructions: `${dir}-instructions.md`,
               skills: [dir],
            };
         };

         it("charges agents in manifest order, once per shared path", () => {
            writeSkill("a", 4);
            writeSkill("b", 4);
            writeSkill("c", 4);
            const result = read({
               a: agentAt("a"),
               b: agentAt("b"),
               c: agentAt("c"),
               // Shares a's directory, so none of it is charged again.
               d: agentAt("a"),
            });

            expect(result.agents.get("a")!.assets).toHaveLength(4);
            expect(result.agents.get("b")!.assets).toHaveLength(4);
            expect(result.agents.get("c")!.assets).toBeUndefined();
            expect(result.agents.get("d")!.assets).toHaveLength(4);
            expect(
               result.warnings.some(
                  (w) =>
                     w.startsWith("Package agent 'c'") &&
                     w.includes("8388608 byte per-package cap"),
               ),
            ).toBe(true);
         });

         it("does not charge an agent that is dropped", () => {
            writeSkill("a", 4);
            writeSkill("b", 4);
            writeSkill("c", 4);
            // a is dropped after its files were walked.
            write("a/s/assets/sub/publisher.json", "{}");
            const result = read({
               a: agentAt("a"),
               b: agentAt("b"),
               c: agentAt("c"),
            });
            expect(result.agents.has("a")).toBe(false);
            expect(result.agents.get("b")!.assets).toHaveLength(4);
            expect(result.agents.get("c")!.assets).toHaveLength(4);
         });
      });
   });

   it("never throws on hostile values", () => {
      const hostile: unknown[] = [
         { a: null },
         { a: [] },
         { a: { description: "d", instructions: { path: "x" } } },
         { a: { description: "d", instructions: ["x.md"] } },
         { a: { description: "d", instructions: "x.md", skills: [null] } },
         { a: { description: "d", instructions: "x.md", skills: [{}] } },
         { a: { description: "d", instructions: "x.md", schedules: [null] } },
         { a: { description: "d", instructions: "x.md", schedules: [[]] } },
         {
            a: {
               description: "d",
               instructions: "x.md",
               schedules: [{ cron: 5, task: 5 }],
            },
         },
      ];
      for (const raw of hostile) {
         const result = read(raw);
         expect(result.agents.size).toBe(0);
         expect(result.warnings.length).toBeGreaterThan(0);
      }
   });
});
