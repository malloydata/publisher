// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The served copy's classification backstop over real compiles: a served
 * compile whose classes differ from the compile of record (a field that lost
 * its class, or one past the source's `#(carries)`) declines the sources it
 * affects, by `sourceID`, with a `classification_not_kept` problem each; the
 * same text declines nothing.
 */

import { describe, expect, it } from "bun:test";
import { materializationFixture } from "../../../test_helpers/materialization_fixture";
import { SourceMap } from "../text/source_map";
import {
   classificationCeiling,
   type ServedCopyProblem,
} from "./served_copy_serve";

const mz = materializationFixture();
const compile = (text: string) => mz.compile(text, { root: "file:///p/" });

/** `name` classed `nameClass` (unclassified when empty); `s` carries `ceiling`. */
const model = (nameClass: string, ceiling: string) => {
   const name = nameClass
      ? `#(classification) "${nameClass}"\n  public: name`
      : "public: name";
   return `##! experimental.access_modifiers
source: t is duckdb.sql("select 1 as id, 'x' as name") include {
  #(classification) "none"
  public: id
  ${name}
}
#(carries) [${ceiling}]
source: s is t extend {
  #(classification) "none"
  measure: n is count()
}
`;
};

async function check(recordText: string, servedText: string) {
   const recordDef = await compile(recordText);
   const servedDef = await compile(servedText);
   const servedMap = SourceMap.of(servedDef);
   const problems: ServedCopyProblem[] = [];
   const unsafe = classificationCeiling(
      "m.malloy",
      recordDef,
      servedDef,
      servedMap,
      problems,
   );
   return { unsafe, problems, idOf: (n: string) => servedMap.sourceIDOf(n) };
}

describe("classificationCeiling", () => {
   it("declines nothing when the served compile classifies as the record does", async () => {
      const text = model("phi", `"none", "phi"`);
      const { unsafe, problems } = await check(text, text);
      expect([...unsafe]).toEqual([]);
      expect(problems).toEqual([]);
   });

   it("declines a source whose served compile leaves a field unclassified", async () => {
      const { unsafe, problems, idOf } = await check(
         model("phi", `"none", "phi"`),
         model("", `"none", "phi"`),
      );
      expect(problems.map((p) => p.reason)).toContain(
         "classification_not_kept",
      );
      const declined = problems.map((p) => p.sourceName ?? "");
      expect(declined).toContain("s");
      expect(problems.every((p) => (p.coverage ?? []).length > 0)).toBe(true);
      expect([...unsafe].sort()).toEqual(
         declined.map((n) => idOf(n) as string).sort(),
      );
   });

   it("declines a source whose served compile reads a class past its #(carries)", async () => {
      const { unsafe, problems, idOf } = await check(
         model("none", `"none"`),
         model("phi", `"none"`),
      );
      const s = problems.find((p) => p.sourceName === "s");
      expect(s?.reason).toBe("classification_not_kept");
      expect((s?.leaks ?? []).join(" ")).toContain("name");
      expect(unsafe.has(idOf("s") as string)).toBe(true);
   });
});
