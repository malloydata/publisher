// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { CAPABILITIES, capabilitiesOf } from "../domain/dialect";
import {
   decodeTablePathSegments,
   probeServerVersion,
} from "./incremental_apply";

/**
 * The incremental refresh's per-dialect facts that moved into the dialect
 * table and its SQL half (the server-version read that gates MERGE, and
 * which dialects' table paths are decoded for the target probe), run for
 * every registered dialect and for names nothing registers, and compared
 * with what the code produced before the move, recorded in
 * incremental_dialect.golden.json.
 *
 * Re-record only for a reviewed behaviour change:
 * `RECORD_INCREMENTAL_DIALECT=1 bun test <this file>`.
 */

const GOLDEN = path.join(__dirname, "incremental_dialect.golden.json");

const DIALECTS = [
   ...Object.keys(CAPABILITIES),
   "bigquery",
   "motherduck",
   "ducklake",
   "constructor",
   "toString",
   "nope",
];

const PATHS = ["t", "s.t", '"S"."T"', "Lake.Main.T", '"a.b".c', "`p`.d.t"];

/** The statements the incremental build's MERGE-floor read sends for `dialect`. */
async function versionReads(dialect: string): Promise<unknown> {
   const sent: string[] = [];
   let version: number | undefined;
   if (capabilitiesOf(dialect).mergeGatedByServerVersion) {
      version = await probeServerVersion(async (sql) => {
         sent.push(sql);
         return { rows: [{ version_num: "160004" }] };
      }, dialect);
   }
   return { sent, version: version ?? null };
}

describe("the incremental refresh's dialect facts are unchanged by the move", () => {
   it("for every dialect", async () => {
      const actual: Record<string, unknown> = {};
      for (const dialect of DIALECTS) {
         actual[dialect] = {
            version: await versionReads(dialect),
            decoded: Object.fromEntries(
               PATHS.map((p) => {
                  let value: unknown;
                  try {
                     value = decodeTablePathSegments(dialect, p) ?? null;
                  } catch (error) {
                     value = {
                        error:
                           error instanceof Error
                              ? error.message
                              : String(error),
                     };
                  }
                  return [p, value];
               }),
            ),
         };
      }
      if (process.env.RECORD_INCREMENTAL_DIALECT === "1") {
         fs.writeFileSync(GOLDEN, JSON.stringify(actual, null, 2) + "\n");
      }
      expect(actual).toEqual(JSON.parse(fs.readFileSync(GOLDEN, "utf8")));
   });
});
