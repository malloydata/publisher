// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `unrecordedAudit()` hands a spec an audit with no record behind it. A route
 * that used it would run a query nobody is owed an access record for, so only
 * specs and their helpers may call it; every route gets its audit from
 * `withAccessRecord`.
 */
import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";

const SRC = path.resolve(__dirname, "..");

function sourceFiles(dir: string): string[] {
   return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) return sourceFiles(full);
      return full.endsWith(".ts") && !full.endsWith(".spec.ts") ? [full] : [];
   });
}

describe("runUnrecorded", () => {
   it("is called only by the value-index build, which no caller asked for", () => {
      const callers = sourceFiles(SRC)
         .map((file) => path.relative(SRC, file))
         .filter(
            (file) =>
               file !== path.join("service", "access_record.ts") &&
               fs
                  .readFileSync(path.join(SRC, file), "utf8")
                  .includes("runUnrecorded("),
         );
      expect(callers).toEqual([path.join("retrieval", "search", "values.ts")]);
   });
});

describe("unrecordedAudit", () => {
   it("is called by no production file", () => {
      const callers = sourceFiles(SRC)
         .map((file) => path.relative(SRC, file))
         .filter(
            (file) =>
               file !== path.join("service", "access_record.ts") &&
               !file.startsWith("test_helpers"),
         )
         .filter((file) =>
            fs
               .readFileSync(path.join(SRC, file), "utf8")
               .includes("unrecordedAudit("),
         );
      expect(callers).toEqual([]);
   });
});
