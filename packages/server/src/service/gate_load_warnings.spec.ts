// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The gate findings a load makes without refusing the model reach the
 * package's warnings, coded, rather than only the server log.
 */

import { afterEach, describe, expect, it } from "bun:test";
import {
   loadTempPackage,
   type LoadedTempPackage,
} from "../test_helpers/temp_package";

const ROWS = `duckdb.sql("select 'us' as region, 1 as amount")`;

describe("gate findings at load reach Package.warnings", () => {
   let loaded: LoadedTempPackage | undefined;
   afterEach(async () => {
      await loaded?.cleanup();
      loaded = undefined;
   });

   const gateWarnings = () =>
      (loaded!.pkg.getPackageMetadata().warnings ?? []).filter((w) =>
         w.code?.startsWith("row-level-gate-"),
      );

   it("warns row-level-gate-reads-no-given for a gate that reads no given, once per entry point", async () => {
      loaded = await loadTempPackage({
         "orders.malloy": `##! experimental { givens }
#(authorize) true
source: orders is ${ROWS}
`,
         "other.malloy": `import "orders.malloy"
source: more is orders
`,
      });
      // Each entry point the gate reaches, once, though two models check
      // `orders`.
      expect(
         gateWarnings().map((w) => [w.code, w.subject, w.model, w.severity]),
      ).toEqual([
         ["row-level-gate-reads-no-given", "orders", "orders.malloy", "warn"],
         ["row-level-gate-reads-no-given", "more", "other.malloy", "warn"],
      ]);
   });

   it("does not warn for a deliberate deny-all lock, #(authorize) false", async () => {
      loaded = await loadTempPackage({
         "orders.malloy": `##! experimental { givens }
#(authorize) false
source: orders is ${ROWS}
`,
      });
      expect(gateWarnings()).toEqual([]);
   });

   it("warns row-level-gate-unexpressible for an inherited gate an entry point cannot carry", async () => {
      loaded = await loadTempPackage({
         "orders.malloy": `##! experimental { givens }
given: REGIONS :: string[]
#(access_filter) region in $REGIONS
source: orders is ${ROWS}
source: amounts is orders extend { except: region }
`,
      });
      expect(gateWarnings().map((w) => [w.code, w.subject])).toEqual([
         ["row-level-gate-unexpressible", "amounts"],
      ]);
   });

   it("warns nothing for a gate keyed on the caller", async () => {
      loaded = await loadTempPackage({
         "orders.malloy": `##! experimental { givens }
given: REGIONS :: string[]
#(access_filter) region in $REGIONS
source: orders is ${ROWS}
`,
      });
      expect(gateWarnings()).toEqual([]);
   });
});
