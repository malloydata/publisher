// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` rollup is stacked on its base's stored table whether or not
 * the base carries a row-level gate: gates apply at read, and the base's
 * table holds every caller's rows, as its source does, so a rollup grouped
 * from it holds what one grouped from the warehouse would
 * (`storedBaseBuilds`).
 */

import { afterEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { installSettings } from "../../../test_helpers/settings_env";
import { createModelRuntime } from "../../model_runtime";
import { tryCompileRollupCompanion } from "./rollup_compile";

const model = (gate: string) => `##! experimental { persistence givens }
given: CAT :: string
source: raw is duckdb.sql("""SELECT 1 AS id, 'a' AS cat, 1.5::DOUBLE AS amt""")

${gate}#@ persist storage="lake"
source: orders is raw -> { select: * } extend {
  #(rollup) grain=["cat"] storage="lake"
  measure: grand is amt.sum()
}
`;

describe("a storage= rollup over a gated base is stacked as over an ungated one", () => {
   const restores: (() => void)[] = [];
   afterEach(() => {
      while (restores.length) restores.pop()!();
   });
   for (const gated of [false, true]) {
      it(`${gated ? "a gated base" : "an ungated base"}: stacked on its table`, async () => {
         restores.push(
            installSettings({
               env: {
                  PERSIST_STORAGE_MODE: "on",
                  PUBLISHER_MATERIALIZE: "true",
                  PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
               },
            }).restore,
         );
         const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rv-"));
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            model(gated ? "#(access_filter) cat = $CAT\n" : ""),
         );
         const duckdb = new DuckDBConnection("duckdb", ":memory:");
         const conns = new Map([["duckdb", duckdb]]);
         const author = await createModelRuntime(
            dir,
            "m.malloy",
            conns as never,
         );
         const mm = author.runtime.loadModel(author.modelURL, {
            importBaseURL: author.importBaseURL,
         });
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const def = ((await mm.getModel()) as any)._modelDef;
         const out = await tryCompileRollupCompanion({
            packagePath: dir,
            modelPath: "m.malloy",
            modelDef: def,
            getRuntime: async (overlay) =>
               createModelRuntime(dir, "m.malloy", conns as never, { overlay }),
         });
         // Planned either way: the switches let a gated storage= rollup be read.
         expect(out.refused ?? []).toEqual([]);
         expect(out.generated?.plans.length).toBe(1);
         expect(out.generated?.plans[0].baseGated).toBe(gated);
         const stacked = out.generated!.storedBaseBuilds.size;
         expect(stacked).toBe(1);
         await duckdb.close();
      });
   }
});
