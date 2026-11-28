// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * E2E coverage for the skill, skill set and workflow routes against the real
 * Express app: name resolution (bundled skills, the default skill set's skills,
 * its `overrides`), wildcard paths with slashes and percent-encoding, the
 * `?skills=` validation, and the 404s with their reasons.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const ENV_NAME = "workflows-test-env";
const PACKAGE_NAME = "workflows-test";
const fixtureDir = path.resolve(__dirname, "../../fixtures/workflows-test");
/** A package whose manifest writes `workflows` as a string, so no skill set or workflow of it is served. */
const REFUSED_PACKAGE = "workflows-refused";
/** A package that declares one skill set at a folder that does not exist, so that set alone is refused. */
const REFUSED_SET_PACKAGE = "skill-set-refused";

describe("Package skills and workflows (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let refusedDir: string;
   let refusedSetDir: string;

   const url = (sub: string, packageName = PACKAGE_NAME) =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${packageName}${sub}`;
   const get = async (sub: string, packageName = PACKAGE_NAME) => {
      const res = await fetch(url(sub, packageName));
      return { status: res.status, body: await res.json() };
   };

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      refusedDir = fs.mkdtempSync(path.join(os.tmpdir(), REFUSED_PACKAGE));
      fs.writeFileSync(
         path.join(refusedDir, "publisher.json"),
         JSON.stringify({ name: REFUSED_PACKAGE, workflows: "nope" }),
      );
      refusedSetDir = fs.mkdtempSync(
         path.join(os.tmpdir(), REFUSED_SET_PACKAGE),
      );
      fs.writeFileSync(
         path.join(refusedSetDir, "publisher.json"),
         JSON.stringify({
            name: REFUSED_SET_PACKAGE,
            skillSets: { broken: { path: "nowhere" } },
         }),
      );

      const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [
               { name: PACKAGE_NAME, location: fixtureDir },
               { name: REFUSED_PACKAGE, location: refusedDir },
               { name: REFUSED_SET_PACKAGE, location: refusedSetDir },
            ],
            connections: [],
         }),
      });
      if (!createRes.ok) {
         throw new Error(
            `Failed to create test environment (${createRes.status}): ${await createRes.text()}`,
         );
      }

      const deadline = Date.now() + 30_000;
      for (const packageName of [
         PACKAGE_NAME,
         REFUSED_PACKAGE,
         REFUSED_SET_PACKAGE,
      ]) {
         for (;;) {
            try {
               if ((await fetch(url("", packageName))).ok) break;
            } catch {
               // not ready yet
            }
            if (Date.now() > deadline) {
               throw new Error("Package did not become available in time");
            }
            await new Promise((r) => setTimeout(r, 500));
         }
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
      for (const dir of [refusedDir, refusedSetDir]) {
         if (dir) fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   describe("skills", () => {
      const skillDir = path.join(fixtureDir, "skill_sets/house/house-rules");
      const text = (relative: string) =>
         fs.readFileSync(path.join(skillDir, relative), "utf8");

      it("lists bundled skills and the default skill set's, the package's replacing a bundled name", async () => {
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
         // Only the default skill set's skills are advertised, not the finance set's.
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

      it("answers 404 for a name nothing defines, a skill only another set holds, and an unknown package", async () => {
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

   describe("skill sets", () => {
      const defaultPath = "skill_sets/house";
      const financePath = "workflows/analyst/skill_sets/analysis";
      const digest = expect.stringMatching(/^[0-9a-f]{64}$/);

      it("lists every declared set by name, whether or not a workflow lists it", async () => {
         const { status, body } = await get("/skill-sets");
         expect(status).toBe(200);
         expect(body).toEqual([
            { name: "default", digest },
            { name: "finance", digest },
         ]);
      });

      it("returns a set whole, with its files, listed files, warnings and the source that pins them", async () => {
         const { status, body } = await get("/skill-sets/default");
         expect(status).toBe(200);
         expect(body).toMatchObject({
            name: "default",
            path: defaultPath,
            digest,
            warnings: [],
         });
         expect(
            body.skills.map(
               (f: { relative_filepath: string }) => f.relative_filepath,
            ),
         ).toEqual([
            "house-rules/SKILL.md",
            "house-rules/reference/margin.md",
            "malloy-analysis/SKILL.md",
         ]);
         expect(
            body.assets.map(
               (a: { relative_filepath: string }) => a.relative_filepath,
            ),
         ).toEqual([
            "house-rules/assets/blob.bin",
            "house-rules/scripts/check.py",
         ]);
         expect(body.source.sourceContentSha).toMatch(/^[0-9a-f]{64}$/);
         expect(body.source.servedRevision).toBeString();
         expect(body.source.definitionSha).toBeUndefined();
      });

      it("serves a SKILL.md exactly as written, on the set route and the skills route", async () => {
         const written = fs.readFileSync(
            path.join(fixtureDir, defaultPath, "house-rules/SKILL.md"),
            "utf8",
         );
         const { body } = await get("/skill-sets/default");
         const entry = body.skills.find(
            (f: { relative_filepath: string }) =>
               f.relative_filepath === "house-rules/SKILL.md",
         );
         expect(entry.file_contents).toBe(written);
         expect(entry).toMatchObject({
            name: "house-rules",
            description: expect.any(String),
         });
         expect((await get("/skills/house-rules")).body.content).toBe(written);
      });

      it("serves a set whole whatever `overrides` says: the package's malloy-analysis is in the default set and not in finance's", async () => {
         const names = async (set: string) =>
            (await get(`/skill-sets/${set}`)).body.skills.map(
               (f: { relative_filepath: string }) => f.relative_filepath,
            );
         expect(await names("default")).toContain("malloy-analysis/SKILL.md");
         expect(await names("finance")).not.toContain(
            "malloy-analysis/SKILL.md",
         );
      });

      it("summarizes skills without their text, under the same pin", async () => {
         const full = await get("/skill-sets/finance");
         const { status, body } = await get(
            "/skill-sets/finance?skills=summary",
         );
         expect(status).toBe(200);
         expect(JSON.stringify(body)).not.toContain("file_contents");
         expect(body.skills[0]).toMatchObject({
            relative_filepath: "conventions/SKILL.md",
            name: "conventions",
            description: "House rules for the analyst.",
         });
         expect(body.digest).toBe(full.body.digest);
         expect(body.source).toEqual(full.body.source);
         // `bytes` is the file's size on disk, not the length of its text.
         for (const file of body.skills as Array<{
            relative_filepath: string;
            bytes: number;
         }>) {
            expect(file.bytes).toBe(
               fs.statSync(
                  path.join(fixtureDir, financePath, file.relative_filepath),
               ).size,
            );
         }
      });

      it("rejects a skills value that is not full or summary, including an empty one", async () => {
         for (const value of ["bogus", ""]) {
            const { status, body } = await get(
               `/skill-sets/finance?skills=${value}`,
            );
            expect(status).toBe(400);
            expect(body.message).toContain("'skills' must be");
         }
      });

      it("gives a set the manifest never declared SKILL_SET_NOT_DECLARED, and a name that is not a valid set name a 404", async () => {
         const undeclared = await get("/skill-sets/nope");
         expect(undeclared.status).toBe(404);
         expect(undeclared.body.reason).toBe("SKILL_SET_NOT_DECLARED");
         expect((await get("/skill-sets/Bad_Name")).status).toBe(404);
      });

      it("gives a set the package refuses SKILL_SET_NOT_SERVED and its refusal", async () => {
         const { status, body } = await get(
            "/skill-sets/broken",
            REFUSED_SET_PACKAGE,
         );
         expect(status).toBe(404);
         expect(body.reason).toBe("SKILL_SET_NOT_SERVED");
         expect(body.message).toStartWith("Skill set 'broken' is not served: ");
         const { body: pkg } = await get("", REFUSED_SET_PACKAGE);
         const refusals = (
            pkg.warnings as Array<{ code?: string; subject?: string }>
         ).filter((w) => w.code === "skill-set-not-served");
         expect(refusals.map((w) => w.subject)).toEqual(["broken"]);
         expect((await get("/skill-sets", REFUSED_SET_PACKAGE)).body).toEqual(
            [],
         );
      });

      describe("one file of a set", () => {
         const read = (p: string, set = "finance") =>
            get(`/skill-sets/${set}/skills/${p}`);

         it("returns one file by a path with slashes, literal or encoded, pinned to its set's digest", async () => {
            const { body: setBody } = await get("/skill-sets/finance");
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
               expect(body.digest).toBe(setBody.digest);
               expect(body.source.sourceContentSha).toMatch(/^[0-9a-f]{64}$/);
            }
         });

         it("reads a listed text file, and answers a binary one with a message naming its size, never its bytes", async () => {
            const script = await read(
               "house-rules/scripts/check.py",
               "default",
            );
            expect(script.status).toBe(200);
            expect(script.body.file_contents).toContain("check margin");
            const blob = await read("house-rules/assets/blob.bin", "default");
            expect(blob.status).toBe(200);
            expect(blob.body.file_contents).toBeUndefined();
            expect(blob.body.message).toContain("4 bytes");
         });

         it("reads a skill's SKILL.md by its skill name alone", async () => {
            const { status, body } = await read("house-rules/", "default");
            expect(status).toBe(200);
            expect(body.relative_filepath).toBe("house-rules/SKILL.md");
         });

         it("serves nothing a skill's .skillignore drops", async () => {
            for (const dropped of ["scratch/notes.txt", "margin.draft"]) {
               expect(
                  (await read(`house-rules/${dropped}`, "default")).status,
               ).toBe(404);
            }
         });

         it("decodes the path once, so an encoded percent does not name another file", async () => {
            expect(
               (await read("conventions%252Freference%252Fglossary.md")).status,
            ).toBe(404);
         });

         it("answers 404 for a file outside the set, another set's skill, and an unknown set, naming a reason only for the set", async () => {
            expect((await read("conventions/nope.md")).status).toBe(404);
            expect((await read("..%2F..%2F..%2Fpublisher.json")).status).toBe(
               404,
            );
            expect((await read("house-rules/scripts/check.py")).status).toBe(
               404,
            );
            const missingFile = await read("conventions/nope.md");
            expect(missingFile.body.reason).toBeUndefined();
            const unknown = await read("conventions/SKILL.md", "nope");
            expect(unknown.status).toBe(404);
            expect(unknown.body.reason).toBe("SKILL_SET_NOT_DECLARED");
         });

         it("has no workflow route for a file, and a workflow takes no skillSet", async () => {
            expect(
               (await get("/workflows/analyst/skills/conventions/SKILL.md"))
                  .status,
            ).toBe(404);
            const { status, body } = await get(
               "/workflows/analyst?skillSet=default",
            );
            expect(status).toBe(200);
            expect(body.skill_sets).toHaveLength(2);
         });
      });
   });

   describe("workflows", () => {
      it("lists the workflows the manifest declares, in its order, with the names of the sets they list", async () => {
         const { status, body } = await get("/workflows");
         expect(status).toBe(200);
         expect(JSON.stringify(body)).not.toContain("has_instructions");
         expect(body).toEqual([
            {
               name: "analyst",
               description: "Answers questions using the house conventions",
               skill_set_names: ["finance", "default"],
            },
            {
               name: "weekly-summary",
               description: "Summarizes last week's numbers",
               skill_set_names: [],
            },
            {
               name: "reviewer",
               description: "Reviews work against the package's own skills",
               skill_set_names: ["default"],
            },
         ]);
      });

      it("returns the instructions and each listed set whole, exactly as its own route serves it", async () => {
         const { status, body } = await get("/workflows/analyst");
         expect(status).toBe(200);
         // Windows checks the fixture out with CRLF.
         expect(body.instructions.replace(/\r\n/g, "\n")).toBe(
            "Be careful with revenue.\n",
         );
         expect(body.skill_sets.map((s: { name: string }) => s.name)).toEqual([
            "finance",
            "default",
         ]);
         for (const set of body.skill_sets) {
            const { source: _source, ...own } = (
               await get(`/skill-sets/${set.name}`)
            ).body;
            expect(set).toEqual(own);
         }
         expect(body.skill_sets[0].skills[0].file_contents).toContain(
            "Use net revenue.",
         );
         expect(body.warnings).toEqual([]);
         expect(JSON.stringify(body)).not.toContain("has_instructions");
         expect(body.source.sourceContentSha).toMatch(/^[0-9a-f]{64}$/);
         expect(body.source.definitionSha).toMatch(/^[0-9a-f]{64}$/);
      });

      it("gives two workflows that list one set that set's digest, and each its own definitionSha", async () => {
         const analyst = (await get("/workflows/analyst")).body;
         const reviewer = (await get("/workflows/reviewer")).body;
         expect(analyst.skill_sets[1].digest).toBe(
            reviewer.skill_sets[0].digest,
         );
         expect(analyst.source.definitionSha).not.toBe(
            reviewer.source.definitionSha,
         );
      });

      it("does not serve a workflow whose skill set is missing, and says why in the package's warnings, while the others serve", async () => {
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
               "Workflow 'broken' is not served: it lists the skill set 'missing', which the top-level 'skillSets' does not declare",
            ),
         ).toBe(true);
         expect((await get("/workflows/analyst")).status).toBe(200);
      });

      it("gives a workflow the package refuses WORKFLOW_NOT_SERVED and its refusal, and one it never declared WORKFLOW_NOT_DECLARED", async () => {
         const refused = await get("/workflows/broken");
         expect(refused.status).toBe(404);
         expect(refused.body.reason).toBe("WORKFLOW_NOT_SERVED");
         expect(refused.body.message).toStartWith(
            "Workflow 'broken' is not served: it lists the skill set 'missing', which the top-level 'skillSets' does not declare",
         );
         const undeclared = await get("/workflows/nope");
         expect(undeclared.status).toBe(404);
         expect(undeclared.body.reason).toBe("WORKFLOW_NOT_DECLARED");
         expect(undeclared.body.message).toContain(
            "No workflow named 'nope' is declared in package",
         );
         expect((await get("/workflows/Bad_Name")).status).toBe(404);
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

      it("serves a workflow that lists no set", async () => {
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
         expect(body.instructions).toBe(full.body.instructions);
         expect(body.skill_sets[0].skills[0]).toMatchObject({
            relative_filepath: "conventions/SKILL.md",
            name: "conventions",
            description: "House rules for the analyst.",
         });
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
   });

   describe("a manifest whose whole workflows declaration is refused", () => {
      it("answers any workflow name 404 WORKFLOW_NOT_SERVED, and any set name 404 SKILL_SET_NOT_SERVED, with the manifest warning's text", async () => {
         for (const [sub, reason] of [
            ["/workflows/analyst", "WORKFLOW_NOT_SERVED"],
            ["/skill-sets/default", "SKILL_SET_NOT_SERVED"],
         ] as const) {
            const { status, body } = await get(sub, REFUSED_PACKAGE);
            expect(status).toBe(404);
            expect(body.reason).toBe(reason);
            expect(body.message).toContain(
               "Package manifest 'workflows' must be an object",
            );
         }
         expect((await get("/skill-sets", REFUSED_PACKAGE)).body).toEqual([]);
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
