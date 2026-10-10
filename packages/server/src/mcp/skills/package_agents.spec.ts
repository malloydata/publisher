// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import {
   PACKAGE_AGENT_SKILLS_MAX_BYTES,
   PACKAGE_AGENT_TEXT_MAX_BYTES,
   readPackageAgents,
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
         warnings: [],
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
            "---\nname: c\ndescription: d\n---\nz",
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
