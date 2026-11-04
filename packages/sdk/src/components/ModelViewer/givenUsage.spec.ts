// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   givensDeclaredIn,
   givensForSource,
   givensInheritedBy,
   givensReadBy,
   scanGivensForSource,
   scanGivensOfNamedQuery,
} from "./givenUsage";

const g = (name: string) => ({ name, type: "number", modelFile: "m.malloy" });
const GIVENS = [g("MIN_AMOUNT"), g("CATEGORY"), g("UNUSED"), g("MIN")];

describe("givensReadBy", () => {
   it("finds each declared given a definition reads by $NAME, whole names only", () => {
      expect(
         givensReadBy("is_large is amount >= $MIN_AMOUNT", GIVENS).map(
            (x) => x.name,
         ),
      ).toEqual(["MIN_AMOUNT"]);
      expect(
         givensReadBy(
            "where: category ~ $CATEGORY and amount > $MIN",
            GIVENS,
         ).map((x) => x.name),
      ).toEqual(["CATEGORY", "MIN"]);
      expect(givensReadBy("order_count is count()", GIVENS)).toEqual([]);
      expect(givensReadBy(undefined, GIVENS)).toEqual([]);
   });

   it("ignores a $NAME in a comment", () => {
      expect(givensReadBy("count() // not $CATEGORY", GIVENS)).toEqual([]);
   });

   it("ignores a $NAME in a block comment or a string, and reads past a -- inside a string", () => {
      expect(givensReadBy("count() /* was $CATEGORY */", GIVENS)).toEqual([]);
      expect(givensReadBy("note is '$CATEGORY'", GIVENS)).toEqual([]);
      expect(
         givensReadBy(
            "where: code = 'A--B' and amount > $MIN_AMOUNT",
            GIVENS,
         ).map((x) => x.name),
      ).toEqual(["MIN_AMOUNT"]);
   });
});

const TEXT = `##! experimental.givens
given:
  MIN_AMOUNT :: number is 100
  CATEGORY :: filter<string> is f''
  UNUSED :: number is 1

source: orders is pg.table('public.orders') extend {
  dimension: is_large is amount >= $MIN_AMOUNT
}

#(authorize) 'finance' in $GROUPS
source: targets is pg.sql("SELECT 1") extend {
  measure: n is count()
}

source: narrowed is orders extend {
  where: category ~ $CATEGORY
}

source: vs_targets is targets extend {
  join_one: o is narrowed with category
}

query: q is orders -> { where: amount > $MIN; aggregate: n is count() }
`;
const names = (list: ReadonlyArray<{ name: string }>) =>
   list.map((x) => x.name).sort();

describe("givensForSource", () => {
   it("keeps reading a source's body past an annotation at column 0 inside it", () => {
      // `a` reads CATEGORY only after a `#(doc)` at column 0 inside its
      // `extend { … }`; `b` reads it too, so it is not a model-wide given.
      const text = [
         "source: a is duckdb.table('a') extend {",
         '#(doc) "The big ones"',
         "  dimension: big is category ~ $CATEGORY",
         "}",
         "source: b is duckdb.table('b') extend {",
         "  where: category ~ $CATEGORY",
         "}",
      ].join("\n");
      expect(
         givensForSource(text, "a", [g("CATEGORY")]).map((x) => x.name),
      ).toEqual(["CATEGORY"]);
   });

   it("keeps the givens the source reads, through what it extends, plus those no source reads", () => {
      // UNUSED and MIN are read by no source: model-wide, shown everywhere.
      expect(names(givensForSource(TEXT, "orders", GIVENS))).toEqual([
         "MIN",
         "MIN_AMOUNT",
         "UNUSED",
      ]);
      expect(names(givensForSource(TEXT, "narrowed", GIVENS))).toEqual([
         "CATEGORY",
         "MIN",
         "MIN_AMOUNT",
         "UNUSED",
      ]);
      expect(names(givensForSource(TEXT, "targets", GIVENS))).toEqual([
         "MIN",
         "UNUSED",
      ]);
   });

   it("follows a join to the source it joins", () => {
      expect(names(givensForSource(TEXT, "vs_targets", GIVENS))).toEqual([
         "CATEGORY",
         "MIN",
         "MIN_AMOUNT",
         "UNUSED",
      ]);
   });

   it("keeps every given when the text is absent or the source isn't found", () => {
      expect(givensForSource(undefined, "orders", GIVENS)).toEqual(GIVENS);
      expect(givensForSource(TEXT, "missing", GIVENS)).toEqual(GIVENS);
   });

   it("keeps every given when a source builds on or joins what the file doesn't declare", () => {
      // `regional` comes from the import and may read REGION; `local` reads it
      // here, so without the fallback the scan would settle that only `local`
      // does, and a run on the others would go out without it.
      const text = `import "regions.malloy"
source: local is duckdb.table('l') extend {
  where: region = $REGION
}
source: other is duckdb.table('o') extend {
  where: code = $CODE
}
source: on_local is local extend {
  measure: n is count()
}
query: by_region is local -> { group_by: region }
source: via_import is regional extend {
  measure: n is count()
}
source: via_include is regional include {
  public: *
}
source: via_join is duckdb.table('t') extend {
  join_one: r is regional on r.id = id
}
source: via_join_by_name is duckdb.table('t') extend {
  join_one: regional with region_id
}
source: via_query is by_region extend {
  measure: n is count()
}
source: via_compose is compose(local, regional)
`;
      const given = [g("REGION"), g("CODE")];
      for (const source of [
         "via_import",
         "via_include",
         "via_join",
         "via_join_by_name",
         "via_query",
         "via_compose",
      ]) {
         expect([source, names(givensForSource(text, source, given))]).toEqual([
            source,
            ["CODE", "REGION"],
         ]);
      }
      // What the file does declare still narrows: a table, and its own source.
      expect(names(givensForSource(text, "local", given))).toEqual(["REGION"]);
      expect(names(givensForSource(text, "on_local", given))).toEqual([
         "REGION",
      ]);
   });

   it("names the givens it can't tie to the source either way", () => {
      const text = `import { regional } from "regions.malloy"
source: local is duckdb.table('l') extend {
  where: region = $REGION
}
source: overview is regional extend {
  where: region = $REGION
}
source: via_import is regional extend {
  measure: n is count()
}
`;
      const given = [g("REGION"), g("CODE")];
      const unknown = (source: string) => [
         ...scanGivensForSource(text, source, given).unknown,
      ];
      // Its own text reads REGION; what `regional` reads, it can't see.
      expect(unknown("overview")).toEqual(["CODE"]);
      expect(unknown("via_import")).toEqual(["REGION", "CODE"]);
      // A source declared in another file: none of them.
      expect(unknown("regional")).toEqual(["REGION", "CODE"]);
      // Traced to the end: it can tell.
      expect(unknown("local")).toEqual([]);
   });

   it("reaches a given an unexported source reads, through what extends or joins it", () => {
      const text = `source: base_orders is pg.table('public.orders') extend {
  where: amount >= $MIN_AMOUNT
}

source: orders is base_orders extend {
  measure: n is count()
}

source: by_target is pg.sql("SELECT 1") extend {
  join_one: b is base_orders with id
}
`;
      expect(names(givensForSource(text, "orders", GIVENS))).toContain(
         "MIN_AMOUNT",
      );
      expect(names(givensForSource(text, "by_target", GIVENS))).toContain(
         "MIN_AMOUNT",
      );
      // base_orders isn't exported, so the section says where MIN_AMOUNT comes from.
      const exported = new Set(["orders", "by_target"]);
      expect(givensInheritedBy(text, "orders", GIVENS, exported)).toEqual(
         new Map([["MIN_AMOUNT", ["base_orders"]]]),
      );
      // An exported upstream is a page the reader can open: nothing to name.
      expect(
         givensInheritedBy(
            text,
            "orders",
            GIVENS,
            new Set([...exported, "base_orders"]),
         ),
      ).toEqual(new Map());
   });
});

describe("scanGivensOfNamedQuery", () => {
   const given = [g("REGION"), g("CODE"), g("MIN")];
   const names = (list: ReadonlyArray<{ name: string }>) =>
      list.map((x) => x.name).sort();
   // `items` is a model source whose own scan couldn't tie CODE to it.
   const bySource = new Map([["items", [g("REGION"), g("CODE")]]]);
   const unknownBySource = new Map([["items", new Set(["CODE"])]]);

   it("takes a source it names, with what that source's scan couldn't tie", () => {
      const scan = scanGivensOfNamedQuery(
         "query: q is items -> { group_by: name }",
         given,
         bySource,
         unknownBySource,
      );
      expect(names(scan.givens)).toEqual(["CODE", "REGION"]);
      expect([...scan.unknown]).toEqual(["CODE"]);
   });

   it("can't tie any given it doesn't read itself when it runs over what the model doesn't hold", () => {
      // `regional` comes from an import the model doesn't export.
      const scan = scanGivensOfNamedQuery(
         "query: q is regional -> { where: amount > $MIN; group_by: name }",
         given,
         bySource,
         unknownBySource,
      );
      expect(names(scan.givens)).toEqual(["CODE", "MIN", "REGION"]);
      expect([...scan.unknown].sort()).toEqual(["CODE", "REGION"]);
   });

   it("knows a query over a table reads only what its text does", () => {
      const scan = scanGivensOfNamedQuery(
         "query: q is duckdb.table('t') -> { where: amount > $MIN }",
         given,
         bySource,
         unknownBySource,
      );
      expect(names(scan.givens)).toEqual(["MIN"]);
      expect(scan.unknown.size).toBe(0);
   });

   it("is every given, none known, without its text", () => {
      const scan = scanGivensOfNamedQuery(undefined, given, bySource);
      expect(names(scan.givens)).toEqual(["CODE", "MIN", "REGION"]);
      expect(scan.unknown.size).toBe(3);
   });
});

describe("givensDeclaredIn", () => {
   it("names the givens a file declares itself, not those it imports", () => {
      const text = `import { CODE } from "codes.malloy"
// MIN :: number in a comment is not a declaration
given:
  REGION :: filter<string> is f''
`;
      expect([
         ...givensDeclaredIn(text, [g("REGION"), g("CODE"), g("MIN")]),
      ]).toEqual(["REGION"]);
      expect(givensDeclaredIn(undefined, [g("REGION")]).size).toBe(0);
   });
});
