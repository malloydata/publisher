import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { type RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

// HTTP end-to-end for the /compile authorize gate: proves AccessDeniedError
// surfaces as a real 403 body through the express route + internalErrorToHttpError,
// and that the 403 names only the source (never the gate expression).

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROJECT = "authorize-compile-test-project";
const PKG = "authorize-compile";
const MODEL = "model.malloy";

describe("compile authorize gate (HTTP E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const fixtureDir = path.resolve(
         __dirname,
         "../../fixtures/authorize-compile",
      );
      const createRes = await fetch(`${baseUrl}/api/v0/projects`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PROJECT,
            packages: [{ name: PKG, location: fixtureDir }],
            connections: [],
         }),
      });
      if (!createRes.ok) {
         throw new Error(
            `Failed to create test project (${createRes.status}): ${await createRes.text()}`,
         );
      }
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
         const res = await fetch(
            `${baseUrl}/api/v0/projects/${PROJECT}/packages/${PKG}`,
         );
         if (res.ok) break;
         await new Promise((r) => setTimeout(r, 250));
      }
   });

   afterAll(async () => {
      await env?.stop();
   });

   const compile = (body: unknown) =>
      fetch(
         `${baseUrl}/api/v0/environments/${PROJECT}/packages/${PKG}/models/${MODEL}/compile`,
         {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
         },
      );

   it("returns 403 with a source-only message when the gate is not satisfied", async () => {
      const res = await compile({
         source: "run: gated -> { aggregate: c }",
         givens: {},
      });
      expect(res.status).toBe(403);
      const json = (await res.json()) as { code: number; message: string };
      expect(json.code).toBe(403);
      expect(json.message).toBe('Access denied for source "gated".');
      // Redaction: the runtime 403 must NOT leak the authorize expression.
      expect(json.message).not.toContain("ROLE");
      expect(json.message).not.toContain("analyst");
   });

   it("returns 200 when a satisfying given is supplied", async () => {
      const res = await compile({
         source: "run: gated -> { aggregate: c }",
         givens: { ROLE: "analyst" },
      });
      expect(res.status).toBe(200);
   });

   it("compiles an ungated source without any given", async () => {
      const res = await compile({
         source: "run: open_src -> { aggregate: c }",
      });
      expect(res.status).toBe(200);
   });
});
