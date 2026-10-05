// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What an agent sees when its query text fails to compile on a curated package
 * (an `index.malloy` surface, the default `queryableSources`), over REST and
 * over MCP `execute_query`.
 *
 * The rule: the compiler's own message comes back for text that reaches only
 * what the surface exports, because it says nothing about anything hidden. A
 * refusal that could confirm a hidden source exists stays the same 404 a
 * missing source gets. Over HTTP against the real app, so the mapping from the
 * model's errors to a status and to the MCP tool result is what is tested.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as path from "path";
import { fileURLToPath } from "url";
import {
   cleanupE2ETestEnvironment,
   McpE2ETestEnvironment,
   setupE2ETestEnvironment,
} from "../../harness/mcp_test_setup";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const fixture = (name: string) =>
   path.resolve(__dirname, "../../fixtures", name);

const ENV_NAME = "curated-compile-errors-env";
const OPEN = "curated-compile-errors";
const GATED = "curated-compile-errors-gated";

type RestAnswer = {
   status: number;
   body: { message?: string; problems?: { code?: string }[] };
};
type McpAnswer = { isError: boolean; text: string };

describe.serial("compile errors on a curated package", () => {
   let rest: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let mcp: McpE2ETestEnvironment | null = null;
   let baseUrl: string;

   const viaRest = async (pkg: string, query: string): Promise<RestAnswer> => {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${pkg}/models/index.malloy/query`,
         {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ query }),
         },
      );
      return { status: res.status, body: await res.json() };
   };

   const viaMcp = async (pkg: string, query: string): Promise<McpAnswer> => {
      const result = await mcp!.mcpClient.callTool({
         name: "execute_query",
         arguments: {
            environmentName: ENV_NAME,
            packageName: pkg,
            modelPath: "index.malloy",
            query,
         },
      });
      const content = (result.content ?? []) as { text?: string }[];
      return {
         isError: result.isError === true,
         text: content.map((c) => c.text ?? "").join("\n"),
      };
   };

   beforeAll(async () => {
      mcp = await setupE2ETestEnvironment();
      rest = await startRestE2E();
      baseUrl = rest.baseUrl;

      const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [
               { name: OPEN, location: fixture(OPEN) },
               { name: GATED, location: fixture(GATED) },
            ],
            connections: [],
         }),
      });
      if (!createRes.ok) {
         throw new Error(
            `Failed to create test environment (${createRes.status}): ` +
               `${await createRes.text()}`,
         );
      }
      for (const pkg of [OPEN, GATED]) {
         const deadline = Date.now() + 30_000;
         for (;;) {
            const res = await fetch(
               `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${pkg}`,
            );
            if (res.ok) break;
            if (Date.now() > deadline) {
               throw new Error(`Package ${pkg} did not become available`);
            }
            await new Promise((r) => setTimeout(r, 500));
         }
      }
   }, 120_000);

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      await rest?.stop();
      rest = null;
      await cleanupE2ETestEnvironment(mcp);
      mcp = null;
   });

   // Each of these is the agent's own mistake on an exported source. The real
   // message must come back, over both transports, with the default setting.
   const ownMistakes: [string, string, string, string][] = [
      [
         "an unknown field",
         "run: orders -> { where: nosuchfield = 1 aggregate: n is total }",
         "field-not-found",
         "'nosuchfield' is not defined",
      ],
      [
         "a grammar error inside the statement",
         "run: orders -> { group_by channel }",
         "syntax-error",
         "Expected ':' following 'group_by'",
      ],
      [
         "the wrong operation on a real field",
         "run: orders -> { aggregate: m is max(total) }",
         "aggregate-of-aggregate",
         "Aggregate expression cannot be aggregate",
      ],
      [
         "a type error",
         "run: orders -> { aggregate: s is sum(channel) }",
         "expression-type-error",
         "Can't use type string",
      ],
      [
         "a field reached through a hidden join",
         "run: orders -> { group_by: stores.nosuchfield }",
         "field-not-found",
         "'nosuchfield' is not defined",
      ],
      [
         "no colon after run, so no source can be read",
         "run orders -> { aggregate: total }",
         "syntax-error",
         "no viable alternative at input 'run'",
      ],
      [
         "SQL instead of Malloy",
         "SELECT channel FROM orders",
         "syntax-error",
         "no viable alternative at input 'SELECT'",
      ],
   ];

   for (const [label, query, code, words] of ownMistakes) {
      it(`returns the compiler's message for ${label}`, async () => {
         const r = await viaRest(OPEN, query);
         expect(r.status).toBe(400);
         expect(r.body.message).toContain(words);
         expect(r.body.problems?.map((p) => p.code)).toContain(code);

         const m = await viaMcp(OPEN, query);
         expect(m.isError).toBe(true);
         expect(m.text).toContain(words);
         expect(m.text).not.toContain("Resource not found");
      }, 30_000);
   }

   it("keeps a hidden source, and a missing one, a 404 with no compiler message", async () => {
      for (const query of [
         "run: stores -> { aggregate: n is count() }",
         "run: nosuchsrc -> { aggregate: n is count() }",
         "run: stores -> { group_by: nosuchfield }",
         "source: x is stores extend {}\nrun: x -> { group_by: nosuchfield }",
      ]) {
         const r = await viaRest(OPEN, query);
         expect(r.status).toBe(404);
         expect(r.body.problems).toBeUndefined();
         expect(r.body.message).not.toContain("nosuchfield");
         const m = await viaMcp(OPEN, query);
         expect(m.isError).toBe(true);
         expect(m.text).not.toContain("is not defined");
         expect(m.text).not.toContain("undefined object");
      }
   }, 60_000);

   it("answers a hidden name and a missing name the same way when something is gated", async () => {
      // `vault` is hidden and gated; `stores` is hidden; `nosuchsrc` does not
      // exist. Each pair of texts differs only in that name.
      const shapes = [
         (n: string) => `run: ${n} -> { aggregate: k is count() }`,
         (n: string) => `run: ${n} -> { group_by: nosuchfield }`,
         // Grammar errors that name the source but cannot be read as a run
         // statement: the answer must not depend on the name.
         (n: string) => `run ${n} -> { aggregate: k is count() }`,
         (n: string) =>
            `source: x is ${n} extend {}\nrun x -> { group_by: nosuchfield }`,
      ];
      for (const shape of shapes) {
         const answers: { rest: RestAnswer; mcp: McpAnswer }[] = [];
         for (const name of ["vault", "stores", "nosuchsrc"]) {
            const query = shape(name);
            answers.push({
               rest: await viaRest(GATED, query),
               mcp: await viaMcp(GATED, query),
            });
         }
         const [vault, stores, missing] = answers;
         for (const a of answers) {
            expect(a.rest.status).toBe(404);
            expect(a.rest.body.problems).toBeUndefined();
            expect(a.mcp.isError).toBe(true);
            // Over MCP all three read the same.
            expect(a.mcp).toEqual(missing.mcp);
         }
         // Over REST a hidden, ungated source and a missing one read the same.
         // (A hidden source that is itself gated can answer in the words of
         // the named-source refusal where the others use the generic ones;
         // that predates this and is not what this test pins.)
         expect(stores.rest).toEqual(missing.rest);
         expect(vault.rest.status).toBe(missing.rest.status);
      }
   }, 120_000);

   it("still returns the compiler's message for the agent's own mistake when something is gated", async () => {
      const r = await viaRest(
         GATED,
         "run: orders -> { where: nosuchfield = 1 aggregate: n is total }",
      );
      expect(r.status).toBe(400);
      expect(r.body.message).toContain("'nosuchfield' is not defined");
   }, 30_000);
});
