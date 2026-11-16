// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The connection-type test seam's two doors: the `NODE_ENV=test` guard on
 * `recordConnectionsAsForTests`, and the ratchet that keeps the conformance
 * harness's unguarded door (`recordConnectionsAsForConformanceHarness`) out
 * of every production module.
 */

import { afterEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import {
   connectionTypeOf,
   recordConnectionsAsForConformanceHarness,
   recordConnectionsAsForTests,
   recordConnectionType,
} from "./connection_type";

const SERVER_DIR = path.resolve(__dirname, "../..");

describe("recordConnectionsAsForTests", () => {
   const nodeEnv = process.env.NODE_ENV;
   afterEach(() => {
      process.env.NODE_ENV = nodeEnv;
   });

   for (const value of [undefined, "production", "development"]) {
      it(`throws for a caller outside a test process (NODE_ENV=${value})`, () => {
         if (value === undefined) delete process.env.NODE_ENV;
         else process.env.NODE_ENV = value;
         expect(() =>
            recordConnectionsAsForTests({ duckdb: "postgres" }),
         ).toThrow(/test seam/);
         // Nothing was recorded.
         const conn = recordConnectionType({ name: "duckdb" }, "duckdb");
         expect(connectionTypeOf(conn)).toBe("duckdb");
      });
   }

   it("records under NODE_ENV=test until restored", () => {
      process.env.NODE_ENV = "test";
      const restore = recordConnectionsAsForTests({ duckdb: "postgres" });
      expect(
         connectionTypeOf(recordConnectionType({ name: "duckdb" }, "duckdb")),
      ).toBe("postgres");
      restore();
      expect(
         connectionTypeOf(recordConnectionType({ name: "duckdb" }, "duckdb")),
      ).toBe("duckdb");
   });
});

describe("recordConnectionsAsForConformanceHarness", () => {
   it("records without NODE_ENV, for the standalone conformance CLI", () => {
      const nodeEnv = process.env.NODE_ENV;
      delete process.env.NODE_ENV;
      try {
         const restore = recordConnectionsAsForConformanceHarness({
            duckdb: "postgres",
         });
         expect(
            connectionTypeOf(
               recordConnectionType({ name: "duckdb" }, "duckdb"),
            ),
         ).toBe("postgres");
         restore();
      } finally {
         process.env.NODE_ENV = nodeEnv;
      }
   });

   /**
    * The ratchet. The harness's door skips the `NODE_ENV` guard, so only the
    * conformance harness may reach it, and the guarded door only specs and
    * test helpers. A module under src/ that names either anywhere else fails
    * here: a recorded type decides whether a `storage=` source is built, so
    * no production path may change it.
    */
   it("is imported only by test_helpers/conformance, and the guarded seam only by specs and test helpers", () => {
      const srcDir = path.join(SERVER_DIR, "src");
      const files = fs
         .readdirSync(srcDir, { recursive: true, encoding: "utf8" })
         .filter((f) => /\.tsx?$/.test(f) && !f.includes("node_modules"))
         .map((f) => f.split(path.sep).join("/"));
      const isSpec = (f: string) => /\.spec\.tsx?$/.test(f);
      const offenders: string[] = [];
      for (const f of files) {
         if (f === "service/connection_type.ts") continue;
         const text = fs.readFileSync(path.join(srcDir, f), "utf8");
         if (
            text.includes("recordConnectionsAsForConformanceHarness") &&
            !f.startsWith("test_helpers/conformance/") &&
            f !== "service/connection_type.spec.ts"
         ) {
            offenders.push(`${f}: recordConnectionsAsForConformanceHarness`);
         }
         if (
            /\brecordConnectionsAsForTests\b/.test(text) &&
            !isSpec(f) &&
            !f.startsWith("test_helpers/")
         ) {
            offenders.push(`${f}: recordConnectionsAsForTests`);
         }
      }
      expect(offenders).toEqual([]);
   });
});
