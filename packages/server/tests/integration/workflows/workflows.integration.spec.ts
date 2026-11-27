// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * E2E coverage for the skill and workflow routes against the real Express app:
 * name resolution (bundled skills, the default workflow's skills, shadowing),
 * wildcard paths with slashes and percent-encoding, the `?skills=` and
 * `?skillSet=` validation, and the 404s.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const ENV_NAME = "workflows-test-env";
const PACKAGE_NAME = "workflows-test";
const fixtureDir = path.resolve(__dirname, "../../fixtures/workflows-test");

describe("Package skills and workflows (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   const url = (sub: string) =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}${sub}`;
   const get = async (sub: string) => {
      const res = await fetch(url(sub));
      return { status: res.status, body: await res.json() };
   };

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [{ name: PACKAGE_NAME, location: fixtureDir }],
            connections: [],
         }),
      });
      if (!createRes.ok) {
         throw new Error(
            `Failed to create test environment (${createRes.status}): ${await createRes.text()}`,
         );
      }

      const deadline = Date.now() + 30_000;
      for (;;) {
         try {
            if ((await fetch(url(""))).ok) break;
         } catch {
            // not ready yet
         }
         if (Date.now() > deadline) {
            throw new Error("Package did not become available in time");
         }
         await new Promise((r) => setTimeout(r, 500));
      }
   });

   afterAll(async () => {
      if (baseUrl) {
         try {
            await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
               method: "DELETE",
            });
         } catch {
            // best-effort
         }
      }
      await env?.stop();
      env = null;
   });

   describe("skills", () => {
      const skillDir = path.join(fixtureDir, "skill_sets/house/house-rules");
      const text = (relative: string) =>
         fs.readFileSync(path.join(skillDir, relative), "utf8");

      it("lists bundled skills and the default workflow's, the package's replacing a bundled name", async () => {
         const { status, body } = await get("/skills");
         expect(status).toBe(200);
         const byName = new Map(
            (body as Array<{ name: string; origin: string }>).map((s) => [
               s.name,
               s.origin,
            ]),
         );
         expect(byName.get("malloy-getting-started")).toBe("bundled");
         expect(byName.get("malloy-analysis")).toBe("package");
         expect(byName.get("house-rules")).toBe("package");
         // Only the default workflow's skills are advertised, not the analyst's.
         expect(byName.has("conventions")).toBe(false);
         // A skill's files are not skills.
         expect([...byName.keys()].some((n) => n.includes("/"))).toBe(false);
         expect(JSON.stringify(body)).not.toContain("OUR ANALYSIS GUIDE");
      });

      it("returns a skill's SKILL.md verbatim, with its frontmatter and every file, the package's copy replacing the bundled one", async () => {
         const { status, body } = await get("/skills/malloy-analysis");
         expect(status).toBe(200);
         expect(body).toMatchObject({
            name: "malloy-analysis",
            path: "SKILL.md",
            origin: "package",
            frontmatter: {
               name: "malloy-analysis",
               description: "The package's own analysis guide.",
            },
            files: [{ path: "SKILL.md", size: expect.any(Number) }],
         });
         expect(body.content).toContain("OUR ANALYSIS GUIDE");

         const house = (await get("/skills/house-rules")).body;
         expect(house.content).toBe(text("SKILL.md"));
         expect(house.content).not.toContain("get_skill");
         expect(house.files.map((f: { path: string }) => f.path)).toEqual([
            "SKILL.md",
            "assets/blob.bin",
            "reference/margin.md",
            "scripts/check.py",
         ]);
      });

      it("serves a bundled skill the package does not shadow as its SKILL.md alone", async () => {
         const { status, body } = await get("/skills/malloy-getting-started");
         expect(status).toBe(200);
         expect(body.origin).toBe("bundled");
         expect(body.path).toBe("SKILL.md");
      });

      it("answers 404 for a name nothing defines, a skill only another workflow lists, and an unknown package", async () => {
         expect((await get("/skills/nope")).status).toBe(404);
         expect((await get("/skills/conventions")).status).toBe(404);
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/missing/skills`,
         );
         expect(res.status).toBe(404);
      });

      it("has no per-file route under a skill: a file's path after the name answers 404", async () => {
         for (const sub of [
            "/skills/house-rules/SKILL.md",
            "/skills/house-rules/reference/margin.md",
         ]) {
            expect((await fetch(url(sub))).status).toBe(404);
         }
      });
   });

   describe("workflows", () => {
      const summary = (set: { name: string; path: string }) => ({
         ...set,
         digest: expect.stringMatching(/^[0-9a-f]{64}$/),
      });
      const analysis = {
         name: "analysis",
         path: "workflows/analyst/skill_sets/analysis",
      };
      const house = { name: "house", path: "skill_sets/house" };

      it("lists the workflows the manifest declares, in its order, without their text", async () => {
         const { status, body } = await get("/workflows");
         expect(status).toBe(200);
         expect(body).toEqual([
            {
               name: "default",
               description: "The package's advertised skills",
               has_instructions: false,
               skill_sets: [summary(house)],
            },
            {
               name: "analyst",
               description: "Answers questions using the house conventions",
               has_instructions: true,
               skill_sets: [summary(analysis), summary(house)],
            },
            {
               name: "weekly-summary",
               description: "Summarizes last week's numbers",
               has_instructions: true,
               skill_sets: [],
            },
         ]);
         // A set two workflows list is one set: the same digest in both.
         expect(body[0].skill_sets[0].digest).toBe(
            body[1].skill_sets[1].digest,
         );
      });

      it("returns the resolved workflow with every skill set's files by default", async () => {
         const { status, body } = await get("/workflows/analyst");
         expect(status).toBe(200);
         // Windows checks the fixture out with CRLF.
         expect(body.instructions.replace(/\r\n/g, "\n")).toBe(
            "Be careful with revenue.\n",
         );
         expect(body.skill_sets.map((s: { name: string }) => s.name)).toEqual([
            "analysis",
            "house",
         ]);
         const paths = (set: number) =>
            body.skill_sets[set].skills.map(
               (f: { relative_filepath: string }) => f.relative_filepath,
            );
         expect(paths(0)).toEqual([
            "conventions/SKILL.md",
            "conventions/reference/glossary.md",
            "conventions/reference/rule-of-50.md",
         ]);
         expect(paths(1)).toEqual([
            "house-rules/SKILL.md",
            "house-rules/reference/margin.md",
            "malloy-analysis/SKILL.md",
         ]);
         expect(
            body.skill_sets[1].assets.map(
               (a: { relative_filepath: string }) => a.relative_filepath,
            ),
         ).toEqual([
            "house-rules/assets/blob.bin",
            "house-rules/scripts/check.py",
         ]);
         expect(body.skill_sets[0].skills[0].file_contents).toContain(
            "Use net revenue.",
         );
         expect(body.warnings).toEqual([]);
         expect(body.source.sourceContentSha).toMatch(/^[0-9a-f]{64}$/);
         expect(body.source.definitionSha).toMatch(/^[0-9a-f]{64}$/);
      });

      it("does not serve a workflow whose skill set is missing, and says why in the package's warnings, while the others serve", async () => {
         expect((await get("/workflows/broken")).status).toBe(404);
         const { body } = await get("");
         const refusals = (
            body.warnings as Array<{
               message: string;
               code?: string;
               subject?: string;
            }>
         ).filter((w) => w.code === "workflow-not-served");
         expect(refusals).toHaveLength(1);
         expect(refusals[0]!.subject).toBe("broken");
         expect(
            refusals[0]!.message.startsWith(
               "Workflow 'broken' is not served: skill set directory 'skill_sets/missing' does not exist",
            ),
         ).toBe(true);
         expect((await get("/workflows/analyst")).status).toBe(200);
      });

      it("gives a workflow the package refuses WORKFLOW_NOT_SERVED and its refusal, and one it never declared WORKFLOW_NOT_DECLARED", async () => {
         const refused = await get("/workflows/broken");
         expect(refused.status).toBe(404);
         expect(refused.body.reason).toBe("WORKFLOW_NOT_SERVED");
         expect(refused.body.message).toStartWith(
            "Workflow 'broken' is not served: skill set directory 'skill_sets/missing' does not exist",
         );
         const undeclared = await get("/workflows/nope");
         expect(undeclared.status).toBe(404);
         expect(undeclared.body.reason).toBe("WORKFLOW_NOT_DECLARED");
         expect(undeclared.body.message).toContain(
            "No workflow named 'nope' is declared in package",
         );
      });

      it("gives a skill set the workflow lacks a 404 with no reason, since the workflow itself is served", async () => {
         const { status, body } = await get("/workflows/analyst?skillSet=nope");
         expect(status).toBe(404);
         expect(body.reason).toBeUndefined();
      });

      it("serves workflows beside an agents key, which it no longer reads, and says so", async () => {
         const { body } = await get("");
         expect(
            (body.warnings as Array<{ message: string }>).some((w) =>
               w.message.includes('"agents" is no longer read'),
            ),
         ).toBe(true);
         expect((await get("/agents")).status).toBe(404);
      });

      it("serves a workflow that is only instructions", async () => {
         const { status, body } = await get("/workflows/weekly-summary");
         expect(status).toBe(200);
         expect(body.instructions).toContain("Summarize last week's revenue");
         expect(body.skill_sets).toEqual([]);
      });

      it("summarizes skills without their text, under the same pin", async () => {
         const full = await get("/workflows/analyst");
         const { status, body } = await get(
            "/workflows/analyst?skills=summary",
         );
         expect(status).toBe(200);
         expect(JSON.stringify(body)).not.toContain("file_contents");
         expect(body.skill_sets[0].skills[0]).toMatchObject({
            relative_filepath: "conventions/SKILL.md",
            name: "conventions",
            description: "House rules for the analyst.",
         });
         expect(body.source).toEqual(full.body.source);
      });

      it("returns one skill set and no instructions for ?skillSet=", async () => {
         const full = await get("/workflows/analyst");
         const { status, body } = await get(
            "/workflows/analyst?skillSet=house",
         );
         expect(status).toBe(200);
         expect(body.instructions).toBeUndefined();
         expect(body.has_instructions).toBe(true);
         expect(body.skill_sets).toEqual([full.body.skill_sets[1]]);
         expect(body.source).toEqual(full.body.source);
      });

      it("rejects a skills value that is not full or summary, including an empty one", async () => {
         for (const value of ["bogus", ""]) {
            const { status, body } = await get(
               `/workflows/analyst?skills=${value}`,
            );
            expect(status).toBe(400);
            expect(body.message).toContain("'skills' must be");
         }
      });

      it("rejects a repeated skillSet", async () => {
         const { status, body } = await get(
            "/workflows/analyst?skillSet=house&skillSet=analysis",
         );
         expect(status).toBe(400);
         expect(body.message).toContain("'skillSet' must be");
      });

      it("answers 404 for an undeclared workflow, a skill set the workflow lacks, and a name that is not a valid workflow name", async () => {
         expect((await get("/workflows/nope")).status).toBe(404);
         expect((await get("/workflows/analyst?skillSet=nope")).status).toBe(
            404,
         );
         expect(
            (await get("/workflows/weekly-summary?skillSet=house")).status,
         ).toBe(404);
         expect((await get("/workflows/Bad_Name")).status).toBe(404);
      });
   });

   describe("workflow skill files", () => {
      const read = (p: string, workflow = "analyst") =>
         get(`/workflows/${workflow}/skills/${p}`);

      it("returns one file by a path with slashes, literal or encoded, from any of the workflow's sets", async () => {
         for (const p of [
            "conventions/reference/glossary.md",
            "conventions%2Freference%2Fglossary.md",
         ]) {
            const { status, body } = await read(p);
            expect(status).toBe(200);
            expect(body.relative_filepath).toBe(
               "conventions/reference/glossary.md",
            );
            expect(body.file_contents).toContain("net = after refunds");
            expect(body.source.sourceContentSha).toMatch(/^[0-9a-f]{64}$/);
         }
         // From the set the analyst shares with the default workflow.
         const shared = await read("house-rules/scripts/check.py");
         expect(shared.status).toBe(200);
         expect(shared.body.file_contents).toContain("check margin");
      });

      it("reads a default-workflow skill's other files at /workflows/default, literal or encoded", async () => {
         for (const p of [
            "house-rules/reference/margin.md",
            "house-rules%2Freference%2Fmargin.md",
         ]) {
            const { status, body } = await read(p, "default");
            expect(status).toBe(200);
            expect(body.relative_filepath).toBe(
               "house-rules/reference/margin.md",
            );
            expect(body.file_contents).toBe(
               fs.readFileSync(
                  path.join(
                     fixtureDir,
                     "skill_sets/house/house-rules/reference/margin.md",
                  ),
                  "utf8",
               ),
            );
         }
      });

      it("reads a file inside the skill set a skillSet names, and nothing another set holds", async () => {
         const own = await read("house-rules/scripts/check.py?skillSet=house");
         expect(own.status).toBe(200);
         expect(own.body.file_contents).toContain("check margin");
         expect(
            (await read("house-rules/scripts/check.py?skillSet=analysis"))
               .status,
         ).toBe(404);
         expect((await read("conventions/SKILL.md?skillSet=nope")).status).toBe(
            404,
         );
      });

      it("rejects a repeated skillSet", async () => {
         const { status, body } = await read(
            "conventions/SKILL.md?skillSet=house&skillSet=analysis",
         );
         expect(status).toBe(400);
         expect(body.message).toContain("'skillSet' must be");
      });

      it("serves nothing a skill's .skillignore drops", async () => {
         for (const dropped of ["scratch/notes.txt", "margin.draft"]) {
            expect((await read(`house-rules/${dropped}`)).status).toBe(404);
         }
      });

      it("answers a binary file with a message naming its size, never its bytes", async () => {
         const { status, body } = await read("house-rules/assets/blob.bin");
         expect(status).toBe(200);
         expect(body.file_contents).toBeUndefined();
         expect(body.message).toContain("4 bytes");
      });

      it("decodes the path once, so an encoded percent does not name another file", async () => {
         expect(
            (await read("conventions%252Freference%252Fglossary.md")).status,
         ).toBe(404);
      });

      it("gives a file read of a refused workflow WORKFLOW_NOT_SERVED, and of an undeclared one WORKFLOW_NOT_DECLARED", async () => {
         const refused = await read("conventions/SKILL.md", "broken");
         expect(refused.status).toBe(404);
         expect(refused.body.reason).toBe("WORKFLOW_NOT_SERVED");
         expect(refused.body.message).toStartWith(
            "Workflow 'broken' is not served: ",
         );
         const undeclared = await read("conventions/SKILL.md", "nope");
         expect(undeclared.status).toBe(404);
         expect(undeclared.body.reason).toBe("WORKFLOW_NOT_DECLARED");
         // A missing file of a served workflow is neither.
         const missing = await read("conventions/nope.md");
         expect(missing.status).toBe(404);
         expect(missing.body.reason).toBeUndefined();
      });

      it("answers 404 for a file outside the resolved set, a skill the workflow does not list, and an unknown workflow", async () => {
         expect((await read("conventions/nope.md")).status).toBe(404);
         expect((await read("..%2F..%2F..%2Fpublisher.json")).status).toBe(404);
         expect((await read("conventions/SKILL.md", "default")).status).toBe(
            404,
         );
         expect((await read("conventions/SKILL.md", "nope")).status).toBe(404);
      });
   });

   it("reports the served identity on the package itself, and no agents", async () => {
      const { status, body } = await get("");
      expect(status).toBe(200);
      expect(body.sourceContentSha).toMatch(/^[0-9a-f]{64}$/);
      expect(body.servedRevision).toBeString();
      expect(body).not.toHaveProperty("agents");
      expect(body).not.toHaveProperty("workflows");
   });
});
