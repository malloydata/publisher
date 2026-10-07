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
import * as fs from "fs";
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
      [
         "`~` against a date literal, which Malloy throws on",
         "run: orders -> { where: d ~ @2025 aggregate: n is total }",
         "translator-error",
         "mysterious error in range computation",
      ],
      [
         "`~` against a number, which Malloy throws a type mismatch on",
         "run: orders -> { where: d ~ 2025 aggregate: n is total }",
         "translator-error",
         "Incompatible types for match('~') operator",
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
         "run: stores -> { where: store_id ~ @2025 aggregate: n is count() }",
         "run: nosuchsrc -> { where: store_id ~ @2025 aggregate: n is count() }",
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
         (n: string) =>
            `run: ${n} -> { where: store_id ~ @2025 aggregate: k is count() }`,
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
         // Over REST too: status and body, so a gated name answered in other
         // words than a missing one would fail here.
         expect(stores.rest).toEqual(missing.rest);
         expect(vault.rest).toEqual(missing.rest);
      }
   }, 120_000);

   it("returns the compiler's message for `~` against a date literal on /compile at every scope", async () => {
      // Malloy throws a plain Error here, not a MalloyError. /compile read it
      // as a server fault (500) at every scope; it is the caller's text.
      const query = "run: orders -> { where: d ~ @2025 aggregate: n is total }";
      const file = `${fs.readFileSync(path.join(fixture(OPEN), "index.malloy"), "utf8")}\n${query}\n`;
      for (const [scope, source] of [
         ["append", query],
         ["file", file],
         ["package", file],
      ]) {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${OPEN}/models/index.malloy/compile`,
            {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({ source, scope }),
            },
         );
         const body = (await res.json()) as {
            status?: string;
            problems?: { code?: string; severity?: string; message?: string }[];
         };
         expect({ scope, status: res.status, outcome: body.status }).toEqual({
            scope,
            status: 200,
            outcome: "error",
         });
         const errors = (body.problems ?? []).filter(
            (p) => p.severity === "error",
         );
         expect(errors.map((p) => p.code)).toEqual(["translator-error"]);
         expect(errors[0].message).toBe(
            "Malloy could not compile this query: mysterious error in range " +
               "computation. This comes from comparing a date or timestamp to " +
               "a date literal such as @2025 with `~`, which Malloy cannot " +
               "compile. Use `=` to match the whole year, month or day " +
               "(`order_date = @2025`), or an explicit range " +
               "(`order_date ? @2025-01-01 to @2026-01-01`).",
         );
      }
   }, 60_000);

   it("still returns the compiler's message for the agent's own mistake when something is gated", async () => {
      const r = await viaRest(
         GATED,
         "run: orders -> { where: nosuchfield = 1 aggregate: n is total }",
      );
      expect(r.status).toBe(400);
      expect(r.body.message).toContain("'nosuchfield' is not defined");
   }, 30_000);
});
