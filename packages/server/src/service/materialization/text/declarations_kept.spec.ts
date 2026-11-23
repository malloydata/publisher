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

   describe("a block annotation goes with the statement it tags", () => {
      // Each blanking case keeps the statement directly above the one tested, so a
      // line walker that swept the next statement's annotations can't pass it.
      const REPRO = `##! experimental.persistence

#@ persist
source: org_records is duckdb.table('orgs.parquet') extend {
  dimension: is_active is status = 'active'
}

#@ persist
source: signup_org_creators is org_records -> {
  group_by: creator_email
  aggregate: orgs is count()
}

#|(doc)
Orgs that left the funnel this quarter.
Read by the exec dashboard.
|#
source: churned_orgs is duckdb.table('churn.parquet')
`;

      it("blanks a block doc on a source it does not keep", () => {
         const kept = keepDeclarations(REPRO, [
            at(REPRO, "org_records is"),
            at(REPRO, "signup_org_creators is"),
         ])!;
         expect(kept).toContain("aggregate: orgs is count()\n}");
         expect(kept).not.toContain("#|");
         expect(kept).not.toContain("|#");
         expect(kept).not.toContain("left the funnel");
         expect(kept).not.toContain("exec dashboard");
         expect(kept.length).toBe(REPRO.length);
      });

      it("blanks a block doc on a query and a run", () => {
         const text = `source: a is wh.table('a')

#|(doc)
q's doc
|#
query: q is a -> { select: * }

source: b is wh.table('b')

  #|
  run's doc
  |#
run: b -> { select: * }
`;
         const kept = keepDeclarations(text, [
            at(text, "a is"),
            at(text, "b is"),
         ])!;
         expect(kept).toContain("source: a is");
         expect(kept).toContain("source: b is");
         expect(kept).not.toContain("|#");
         expect(kept).not.toContain("doc");
      });

      it("keeps the block doc on a kept source below a blanked one", () => {
         const text = `source: a is wh.table('a')
#|(doc)
b's doc
|#
source: b is wh.table('b')
`;
         const kept = keepDeclarations(text, [at(text, "b is")])!;
         expect(kept).not.toContain("source: a");
         expect(kept).toContain("#|(doc)\nb's doc\n|#\nsource: b");
      });
   });

   it("blanks an annotation a comment separates from its statement", () => {
      const text = `source: a is wh.table('a')

#@ persist
// a note
source: b is wh.table('b')
`;
      const kept = keepDeclarations(text, [at(text, "a is")])!;
      expect(kept).toContain("source: a is");
      expect(kept).not.toContain("#@ persist");
      expect(kept).not.toContain("source: b");
   });

   it("leaves a kept statement its access label, and the annotation on its line", () => {
      const text = `source: a is wh.table('a') # t
private source: b is wh.table('b')
`;
      const kept = keepDeclarations(text, [at(text, "b is")])!;
      expect(kept).not.toContain("source: a");
      expect(kept).toContain("# t\nprivate source: b");
   });

   it("keeps a model annotation between statements", () => {
      const text = `source: a is wh.table('a')
##! experimental.persistence
source: b is wh.table('b')
`;
      const kept = keepDeclarations(text, [at(text, "b is")])!;
      expect(kept).not.toContain("source: a");
      expect(kept).toContain("##! experimental.persistence\nsource: b");
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

   it("leaves out a block doc with the definition it tags", () => {
      const text = `source: s is wh.table('t') extend {
  dimension: kept is 1
  #|(doc)
  Left out.
  |#
  dimension: gone is 2
}
`;
      const left = leaveOutDefinitionAt(text, at(text, "gone is"), [
         { start: 0, end: text.length },
      ])!;
      expect(left).toContain("dimension: kept is 1");
      expect(left).not.toContain("gone");
      expect(left).not.toContain("|#");
      expect(left).not.toContain("Left out.");
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
