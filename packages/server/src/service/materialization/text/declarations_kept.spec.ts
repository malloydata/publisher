// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { keepDeclarations, leaveOutDefinitionAt } from "./declarations_kept";

const FILE = `##! experimental.persistence
import "other.malloy"

given: REGION :: string is 'x'

source: raw is wh.table('t')

#@ persist storage=lake
source: daily is raw -> {
  group_by: d
}

#(doc) "the downstream"
#@ persist storage=lake
source: weekly is daily -> { aggregate: n is count() }

query: q is raw -> { select: * }
run: raw -> { select: * }
`;

/** The position of the first occurrence of `needle`. */
function at(text: string, needle: string): { line: number; character: number } {
   const i = text.indexOf(needle);
   const before = text.slice(0, i).split("\n");
   return { line: before.length - 1, character: before.at(-1)!.length };
}

const trimmed = (text: string) =>
   text
      .split("\n")
      .map((l) => l.trimEnd())
      .join("\n");

describe("keepDeclarations", () => {
   it("blanks every source it does not keep, and every query and run, with their annotations", () => {
      const kept = keepDeclarations(FILE, [
         at(FILE, "daily is"),
         at(FILE, "weekly is"),
      ])!;
      expect(trimmed(kept)).toBe(
         trimmed(`##! experimental.persistence
import "other.malloy"

given: REGION :: string is 'x'



#@ persist storage=lake
source: daily is raw -> {
  group_by: d
}

#(doc) "the downstream"
#@ persist storage=lake
source: weekly is daily -> { aggregate: n is count() }



`),
      );
   });

   it("keeps every position, so a range the compiler reported still holds", () => {
      const kept = keepDeclarations(FILE, [at(FILE, "weekly is")])!;
      expect(kept.length).toBe(FILE.length);
      expect(kept.split("\n").length).toBe(FILE.split("\n").length);
      expect(kept.indexOf("source: weekly")).toBe(
         FILE.indexOf("source: weekly"),
      );
      // The annotation above a blanked source goes with it: it would
      // otherwise land on the next statement.
      expect(kept).not.toContain("#@ persist storage=lake\nsource: daily");
      expect(kept).toContain('#(doc) "the downstream"');
   });

   it("is undefined for text that does not lex", () => {
      expect(keepDeclarations("source: a is b extend {", [])).toBeUndefined();
   });
});

describe("leaveOutDefinitionAt", () => {
   const UPSTREAM = `source: daily is lake.virtual('h')::t extend {
  where: total > 60
  join_one: r is regions on region_id = r.id
  dimension: region_name is r.name
}
source: rollup is daily -> { aggregate: g is total.sum() }
`;
   const within = [{ start: 0, end: UPSTREAM.indexOf("source: rollup") }];

   it("blanks the join or dimension a problem points at, inside the range it may", () => {
      const left = leaveOutDefinitionAt(
         UPSTREAM,
         at(UPSTREAM, "regions on"),
         within,
      )!;
      expect(left).not.toContain("join_one");
      expect(left).toContain("dimension: region_name");
      expect(left.length).toBe(UPSTREAM.length);
      const again = leaveOutDefinitionAt(left, at(left, "r.name"), within)!;
      expect(again).not.toContain("region_name");
      expect(again).toContain("where: total > 60");
   });

   it("never leaves out a filter, or anything outside the range", () => {
      expect(
         leaveOutDefinitionAt(UPSTREAM, at(UPSTREAM, "total > 60"), within),
      ).toBeUndefined();
      expect(
         leaveOutDefinitionAt(UPSTREAM, at(UPSTREAM, "g is total"), within),
      ).toBeUndefined();
   });
});
