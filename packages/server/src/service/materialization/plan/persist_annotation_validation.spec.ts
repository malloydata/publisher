// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { persistNameProblems } from "./persist_annotation_validation";

/** Every load-gate line `persistNameProblems` reports, one per line. */
function problemText(modelSource: string): string {
   return persistNameProblems(modelSource, "m.malloy")
      .map((p) => p.line)
      .join("\n");
}

describe("persistNameProblems", () => {
   it("reports a persist-invalid line for a bare (unquoted) persist name", () => {
      expect(
         problemText(
            `#@ persist name=engaged_events\nsource: engaged_events is x -> { select: * }`,
         ),
      ).toEndWith("[persist-invalid]");
   });

   it("tolerates whitespace around = but still flags an unquoted value", () => {
      expect(problemText(`#@ persist name = engaged_events`)).toMatch(
         /must be quoted/,
      );
   });

   it("accepts a double-quoted name", () => {
      expect(problemText(`#@ persist name="engaged_events"`)).toBe("");
   });

   it("accepts a single-quoted name", () => {
      expect(problemText(`#@ persist name='engaged_events'`)).toBe("");
   });

   it("accepts a dotted, quoted dialect path", () => {
      expect(problemText(`#@ persist name="my_dataset.engaged_events"`)).toBe(
         "",
      );
   });

   it("ignores non-persist annotations and persist lines with no name field", () => {
      expect(
         problemText(
            `# number=2\n#@ persist storage="lake"\nsource: x is y -> { select: * }`,
         ),
      ).toBe("");
   });

   it("does not mistake a neighbouring key for the name field", () => {
      expect(problemText(`#@ persist tablename=foo name="bar"`)).toBe("");
   });

   it("does not mistake a DOTTED key for the name field", () => {
      // `.` is a word boundary, so a `\b`-anchored pattern reads
      // `queryMetadata.name=` as the persist name and fails the whole model
      // load with a 424 about quoting a name the author never declared.
      expect(
         problemText(`#@ persist name="bar" queryMetadata.name=finance`),
      ).toBe("");
   });

   it("reports every offending annotation", () => {
      expect(
         problemText(
            `#@ persist name=a\nsource: a is x -> { select: * }\n#@ persist name=b\nsource: b is x -> { select: * }`,
         ),
      ).toMatch(/name=a.*name=b/s);
   });
});
