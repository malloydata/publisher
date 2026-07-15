/// <reference types="bun-types" />

/**
 * HTTP E2E coverage for `given:` runtime parameters on POST .../query:
 * givens retarget query results, unknown/mistyped givens surface as 400 (not
 * 500), givens compose with filterParams, a `null` given value is an
 * explicit SQL-NULL override, and a numeric-string value is accepted for a
 * `number`-typed given (Malloy's own coercion). Malloy is the single validator:
 * an unknown name or bad value throws a `runtime-given-*` error that
 * model.ts maps to a 400 (see getQueryResults).
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { type RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ENV_NAME = "query-givens-env";
const PKG = "query-givens";
const MODEL = "model.malloy";

type Row = Record<string, unknown>;

describe("givens forwarding on /query (HTTP E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const fixtureDir = path.resolve(__dirname, "../../fixtures/query-givens");
      const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [{ name: PKG, location: fixtureDir }],
            connections: [],
         }),
      });
      if (!createRes.ok) {
         throw new Error(
            `Failed to create test environment (${createRes.status}): ${await createRes.text()}`,
         );
      }
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}`,
         );
         if (res.ok) break;
         await new Promise((r) => setTimeout(r, 250));
      }
   });

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => {});
      await env?.stop();
   });

   const query = (body: Record<string, unknown>) =>
      fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}/models/${MODEL}/query`,
         {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               sourceName: "orders",
               queryName: "by_given_region",
               compactJson: true,
               ...body,
            }),
         },
      );

   async function rows(res: Response): Promise<Row[]> {
      expect(res.status).toBe(200);
      const body = (await res.json()) as { result: string };
      return JSON.parse(body.result) as Row[];
   }

   it("retargets rows via a given override (EU vs default US)", async () => {
      const res = await query({ givens: { target_region: "EU" } });
      const r = await rows(res);
      expect(r.length).toBe(1);
      expect(Number(r[0].order_count)).toBe(3);
   });

   it("returns 400 for an unknown given name, naming it in the message", async () => {
      // Malloy rejects the unknown name (`runtime-given-unknown`, with a
      // did-you-mean hint) at prepare time; model.ts maps that to a 400 and
      // forwards Malloy's message, which names the given.
      const res = await query({ givens: { NOtaGiven: 1 } });
      expect(res.status).toBe(400);
      const body = (await res.json()) as { message: string };
      expect(body.message).toContain("NOtaGiven");
   });

   it("returns 400 for a value-type mismatch on a number-typed given", async () => {
      // "min_amount" is declared `:: number`; a non-numeric string can't be
      // coerced, so Malloy rejects it at prepare time (`runtime-given-*`),
      // surfaced as a 400 via the run try/catch in model.ts, not a 500.
      const res = await query({ givens: { min_amount: "not-a-number" } });
      expect(res.status).toBe(400);
   });

   it("composes givens with filterParams", async () => {
      // target_region=EU narrows to ids 4,5,6; filterParams status=active
      // narrows further to ids 4,6 -> order_count=2.
      const res = await query({
         givens: { target_region: "EU" },
         filterParams: { status: "active" },
      });
      const r = await rows(res);
      expect(r.length).toBe(1);
      expect(Number(r[0].order_count)).toBe(2);
   });

   it("omitting givens entirely uses the declared defaults (US baseline)", async () => {
      const res = await query({});
      const r = await rows(res);
      expect(r.length).toBe(1);
      expect(Number(r[0].order_count)).toBe(3);
   });

   it("a null given value is an explicit SQL-NULL override", async () => {
      // `region = NULL` is never true in SQL, so this should return zero
      // matching rows rather than falling back to the 'US' default.
      const res = await query({ givens: { target_region: null } });
      const r = await rows(res);
      expect(r.length).toBe(1);
      expect(Number(r[0].order_count)).toBe(0);
   });

   it("accepts a numeric string for a number-typed given", async () => {
      // Guards against Malloy's numeric-string coercion for `number` givens
      // regressing; "5" should be accepted just like the literal 5.
      const res = await query({ givens: { min_amount: "5" } });
      const r = await rows(res);
      expect(r.length).toBe(1);
      expect(Number(r[0].order_count)).toBe(3);
   });
});
