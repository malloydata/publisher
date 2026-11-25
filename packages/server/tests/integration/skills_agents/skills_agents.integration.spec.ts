// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * E2E coverage for the skill and agent routes against the real Express app:
 * name resolution (bundled skills, package skills, shadowing), wildcard paths
 * with slashes and percent-encoding, the `?skills=` validation, and the 404s.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { BUNDLED_CATALOG } from "../../../src/service/skills/package_skills";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const ENV_NAME = "skills-agents-test-env";
const PACKAGE_NAME = "skills-agents-test";
const fixtureDir = path.resolve(__dirname, "../../fixtures/skills-agents-test");

describe("Package skills and agents (E2E)", () => {
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
      const skillDir = path.join(fixtureDir, "skills/house-rules");
      const text = (relative: string) =>
         fs.readFileSync(path.join(skillDir, relative), "utf8");

      it("lists bundled and package skills, the package's replacing a bundled name", async () => {
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

      it("serves any file of a skill by its path, verbatim, literal or encoded", async () => {
         for (const sub of [
            "reference/margin.md",
            "reference%2Fmargin.md",
            "./reference/margin.md",
         ]) {
            const { status, body } = await get(`/skills/house-rules/${sub}`);
            expect(status).toBe(200);
            expect(body).toMatchObject({
               name: "house-rules",
               path: "reference/margin.md",
               origin: "package",
               content: text("reference/margin.md"),
            });
            expect(body.frontmatter).toBeUndefined();
            expect(body.files).toBeUndefined();
         }
         const script = await get("/skills/house-rules/scripts/check.py");
         expect(script.status).toBe(200);
         expect(script.body.content).toBe(text("scripts/check.py"));
      });

      it("answers a binary file with its size and a message, never its bytes", async () => {
         const { status, body } = await get(
            "/skills/house-rules/assets/blob.bin",
         );
         expect(status).toBe(200);
         expect(body).toMatchObject({
            name: "house-rules",
            path: "assets/blob.bin",
            size: 4,
            origin: "package",
         });
         expect(body.content).toBeUndefined();
         expect(body.message).toContain("binary");
      });

      it("serves a bundled skill the package does not shadow, its Markdown by path and a file it does not carry as a message", async () => {
         const { status, body } = await get("/skills/malloy-getting-started");
         expect(status).toBe(200);
         expect(body.origin).toBe("bundled");
         const withScript = BUNDLED_CATALOG.find((s) =>
            Object.keys(s.files).some((f) => !(f in s.bodies)),
         )!;
         const script = Object.keys(withScript.files).find(
            (f) => !(f in withScript.bodies),
         )!;
         const file = await get(`/skills/${withScript.name}/${script}`);
         expect(file.status).toBe(200);
         expect(file.body.content).toBeUndefined();
         expect(file.body.size).toBe(withScript.files[script]!.size);
         expect(file.body.message).toContain("@malloy-publisher/skills");
      });

      it("answers 404 for a name nothing defines, a path the skill lacks, the old <skill>/<stem> name, and an unknown package", async () => {
         expect((await get("/skills/nope")).status).toBe(404);
         const missing = await get("/skills/house-rules/nope.md");
         expect(missing.status).toBe(404);
         expect(missing.body.message).toContain(
            "Its files: SKILL.md, assets/blob.bin, reference/margin.md, scripts/check.py.",
         );
         expect((await get("/skills/house-rules/margin")).status).toBe(404);
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/missing/skills`,
         );
         expect(res.status).toBe(404);
      });
   });

   describe("agents", () => {
      it("lists the agents the manifest declares, without their text", async () => {
         const { status, body } = await get("/agents");
         expect(status).toBe(200);
         expect(body).toEqual([
            {
               name: "analyst",
               description: "Answers questions using the house conventions",
               model: "inherit",
               schedules: [],
            },
         ]);
      });

      it("returns the resolved agent with every skill file by default", async () => {
         const { status, body } = await get("/agents/analyst");
         expect(status).toBe(200);
         // Windows checks the fixture out with CRLF.
         expect(body.instructions.replace(/\r\n/g, "\n")).toBe(
            "Be careful with revenue.\n",
         );
         expect(
            body.skills.map(
               (f: { relative_filepath: string }) => f.relative_filepath,
            ),
         ).toEqual([
            "conventions/SKILL.md",
            "conventions/reference/50%-rule.md",
            "conventions/reference/glossary.md",
         ]);
         expect(body.skills[0].file_contents).toContain("Use net revenue.");
         expect(body.source.sourceContentSha).toMatch(/^[0-9a-f]{64}$/);
      });

      it("summarizes skills without their text, under the same pin", async () => {
         const full = await get("/agents/analyst");
         const { status, body } = await get("/agents/analyst?skills=summary");
         expect(status).toBe(200);
         expect(JSON.stringify(body)).not.toContain("file_contents");
         expect(body.skills[0]).toMatchObject({
            relative_filepath: "conventions/SKILL.md",
            name: "conventions",
            description: "House rules for the analyst.",
         });
         expect(body.source).toEqual(full.body.source);
      });

      it("rejects a skills value that is not full or summary, including an empty one", async () => {
         for (const value of ["bogus", ""]) {
            const { status, body } = await get(
               `/agents/analyst?skills=${value}`,
            );
            expect(status).toBe(400);
            expect(body.message).toContain("'skills' must be");
         }
      });

      it("answers 404 for an undeclared agent and for a name that is not a valid agent name", async () => {
         expect((await get("/agents/nope")).status).toBe(404);
         expect((await get("/agents/Bad_Name")).status).toBe(404);
      });
   });

   describe("agent skill files", () => {
      const read = (p: string) => get(`/agents/analyst/skills/${p}`);

      it("returns one file by a path with slashes, literal or encoded", async () => {
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
      });

      it("decodes the path once, so %25 names a file with a literal percent", async () => {
         const { status, body } = await read(
            "conventions/reference/50%25-rule.md",
         );
         expect(status).toBe(200);
         expect(body.relative_filepath).toBe(
            "conventions/reference/50%-rule.md",
         );
      });

      it("answers 404 for a file outside the resolved set and for an unknown agent", async () => {
         expect((await read("conventions/nope.md")).status).toBe(404);
         expect((await read("..%2F..%2F..%2Fpublisher.json")).status).toBe(404);
         const { status } = await get(
            "/agents/nope/skills/conventions/SKILL.md",
         );
         expect(status).toBe(404);
      });
   });

   it("reports the served identity and the agents on the package itself", async () => {
      const { status, body } = await get("");
      expect(status).toBe(200);
      expect(body.sourceContentSha).toMatch(/^[0-9a-f]{64}$/);
      expect(body.servedRevision).toBeString();
      expect(body.agents.map((a: { name: string }) => a.name)).toEqual([
         "analyst",
      ]);
   });
});
