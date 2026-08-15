// The pre-aggregation publish/load gate, end to end over a real package.
//
// preaggregation_validation.spec.ts covers the RULES against compiled IR. This
// file covers the WIRING: that a bad `#@ preaggregate` actually fails the load as
// a 400 rather than being logged somewhere, that the message names the file, and
// that a valid declaration does not disturb a package that loads fine today.
//
// Why the load and not just publish (see Package.preaggregatePolicyWarnings): a
// package can arrive without ever passing through POST/PATCH — uploaded to a
// control plane's storage and loaded by a worker — and pre-aggregation is
// invisible when it works, so a warning would leave the author reading a clean
// publish while their measure quietly never rolled up.
//
// Follows the real-package pattern of incremental_policy.spec.ts: Environment +
// addPackage over a temp dir.
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { BadRequestError, internalErrorToHttpError } from "../errors";
import { Environment } from "./environment";
import type { Package } from "./package";

let rootDir: string;
let envPath: string;

beforeEach(async () => {
   rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-preagg-"));
   envPath = path.join(rootDir, "env");
   await fs.mkdir(envPath, { recursive: true });
});

afterEach(async () => {
   await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
});

const HEADER = `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""
  SELECT 1 AS order_id, 10 AS amount, 'A' AS category,
         TIMESTAMP '2024-01-01 00:00:00' AS order_time
""") extend {
  dimension: order_day is order_time.day
`;

/** A model whose `orders` source carries `body` inside its extend block. */
function model(body: string): string {
   return `${HEADER}${body}\n}\n`;
}

async function loadPackage(
   text: string,
   file = "model.malloy",
): Promise<Package> {
   const dir = path.join(envPath, "pkg");
   await fs.mkdir(dir, { recursive: true });
   await fs.writeFile(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name: "pkg", description: "fixture" }),
   );
   await fs.writeFile(path.join(dir, file), text);
   const env = await Environment.create("testEnv", envPath, []);
   await env.addPackage("pkg");
   return env.getPackage("pkg", false);
}

describe("a valid declaration does not disturb the load", () => {
   it(
      "loads, and the gate is silent",
      async () => {
         const pkg = await loadPackage(
            model(`  #@ preaggregate grain="category"
  measure: total is amount.sum()`),
         );
         expect(pkg.preaggregatePolicyWarnings()).toEqual([]);
         expect(pkg.formatInvalidPreaggregatePolicy()).toBe("");
      },
      { timeout: 30000 },
   );

   it(
      "is inert for a package that declares nothing",
      async () => {
         const pkg = await loadPackage(
            model(`  measure: total is amount.sum()`),
         );
         expect(pkg.preaggregatePolicyWarnings()).toEqual([]);
      },
      { timeout: 30000 },
   );
});

describe("an unusable declaration FAILS the load as a 400", () => {
   it(
      "a non-additive measure, with the file named and the fix in the message",
      async () => {
         let error: unknown;
         try {
            await loadPackage(
               model(`  #@ preaggregate grain="category"
  measure: avg_amount is amount.avg()`),
            );
         } catch (e) {
            error = e;
         }
         expect(error).toBeInstanceOf(BadRequestError);
         // 400 and not the 424 a control plane defaults to: this is an authoring
         // error, not a dependency failure.
         const http = internalErrorToHttpError(error as Error);
         expect(http.status).toBe(400);
         expect(http.json.message).toContain("model.malloy");
         expect(http.json.message).toContain("avg_amount");
      },
      { timeout: 30000 },
   );

   it(
      "an annotation on a dimension, which compiles perfectly well",
      async () => {
         // The case the gate exists for: legal Malloy that would silently do
         // nothing, so nothing but this check stands between the author and a
         // rollup they believe in.
         let error: unknown;
         try {
            await loadPackage(
               model(`  #@ preaggregate grain="category"
  dimension: cat is category`),
            );
         } catch (e) {
            error = e;
         }
         expect(error).toBeInstanceOf(BadRequestError);
         expect((error as Error).message).toContain("`cat`");
      },
      { timeout: 30000 },
   );

   it(
      "an inline time truncation, refused with the dimension to declare",
      async () => {
         let error: unknown;
         try {
            await loadPackage(
               model(`  #@ preaggregate grain="order_time.day"
  measure: total is amount.sum()`),
            );
         } catch (e) {
            error = e;
         }
         expect(error).toBeInstanceOf(BadRequestError);
         expect((error as Error).message).toContain(
            "dimension: order_time_day is order_time.day",
         );
      },
      { timeout: 30000 },
   );

   it(
      "reports EVERY bad declaration, so one republish fixes them all",
      async () => {
         let error: unknown;
         try {
            await loadPackage(
               model(`  #@ preaggregate grain="category"
  measure: avg_amount is amount.avg()
  #@ preaggregate grain="nope"
  measure: total is amount.sum()`),
            );
         } catch (e) {
            error = e;
         }
         expect(error).toBeInstanceOf(BadRequestError);
         const lines = (error as Error).message.split("\n");
         expect(lines).toHaveLength(2);
         expect(lines.every((l) => l.startsWith("model.malloy: "))).toBe(true);
      },
      { timeout: 30000 },
   );
});
