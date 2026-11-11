// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { viewDeclaration } from "./codeDisplay";
import * as malloy from "@malloydata/malloy";
import { fieldDeclarations as declarationsOf } from "./modelTextDeclarations";
import { parseModelText } from "./modelTextParse";

const fieldDeclarations = (text: string | undefined, sourceName: string) =>
   declarationsOf(
      text === undefined ? undefined : parseModelText(malloy, text),
      sourceName,
   );

// The shapes of a real draft's model (bird_feederwatch's feederwatch.malloy): a
// source whose header wraps onto a second line, doc and chart tags above each
// view, joined fields in a group_by, a nested field's own `# percent`, a measure
// wrapped onto the next line, and a view built on another view.
const FEEDERWATCH = `##! experimental.givens
// Project FeederWatch semantic model.

#(doc) eBird taxonomy: one row per taxon.
source: taxonomy is bqtest.table('bird_feederwatch.ebird_taxonomy_2023')
  extend {
  primary_key: SPECIES_CODE
  rename: taxon_category is CATEGORY
  #(doc) English common name of the taxon.
  dimension: common_name is COMMON_NAME
}

source: observations is
  bqtest.table('bird_feederwatch.pfw_observations') extend {
  join_one: species is taxonomy on SPECIES_CODE = species.SPECIES_CODE
  join_one: site is sites

  dimension: season_label is pick 'early' when month_num < 1
    else 'late'
  # number="#,##0"
  measure: checklist_count is count(SUB_ID)
  #(doc) Average number of full species reported per checklist.
  # number="#,##0.0"
  measure: species_per_checklist is
    count() { where: species.is_true_species } / checklist_count

  #(doc) Headline figures for whatever filter is applied.
  view: key_figures is {
    aggregate: checklist_count
  }

  #(doc) Species per checklist by number of feeders at the site.
  # bar_chart { y=species_per_checklist }
  view: richness_by_feeders is {
    group_by: site.feeder_count_band, site.feeder_band_order
    aggregate: species_per_checklist, checklist_count, site_count
    order_by: feeder_band_order
  }

  #(doc) Every species' share of checklists per season.
  view: species_share is {
    where: season_end_year >= 1989
    group_by: season, common_name
    aggregate:
      # percent
      season_share is checklist_count / all(checklist_count, season)
      checklist_count
  }

  #(doc) Share of checklists per season for a few familiar feeder birds.
  # line_chart { y=season_share series=common_name }
  view: species_trend is species_share -> {
    where: common_name = 'Dark-eyed Junco' | 'House Finch'
      | 'Northern Cardinal'
    select: season, common_name, season_share
  }
}
`;

describe("fieldDeclarations", () => {
   const decls = fieldDeclarations(FEEDERWATCH, "observations");

   it("cuts a view with doc and chart tags above it, keeping the tags and its whole block", () => {
      expect(decls.get("richness_by_feeders")).toEqual({
         kind: "view",
         code: [
            "#(doc) Species per checklist by number of feeders at the site.",
            "# bar_chart { y=species_per_checklist }",
            "richness_by_feeders is {",
            "    group_by: site.feeder_count_band, site.feeder_band_order",
            "    aggregate: species_per_checklist, checklist_count, site_count",
            "    order_by: feeder_band_order",
            "  }",
         ].join("\n"),
      });
   });

   it("reads as the view's declaration on its Code, tags kept and doc dropped", () => {
      expect(
         viewDeclaration(
            "richness_by_feeders",
            decls.get("richness_by_feeders")!.code,
         ),
      ).toBe(
         [
            "# bar_chart { y=species_per_checklist }",
            "view: richness_by_feeders is {",
            "  group_by: site.feeder_count_band, site.feeder_band_order",
            "  aggregate: species_per_checklist, checklist_count, site_count",
            "  order_by: feeder_band_order",
            "}",
         ].join("\n"),
      );
   });

   it("finds every view in the source, of every shape", () => {
      const views = [...decls]
         .filter(([, d]) => d.kind === "view")
         .map(([name]) => name);
      expect(views).toEqual([
         "key_figures",
         "richness_by_feeders",
         "species_share",
         "species_trend",
      ]);
      // A nested field's annotation is part of its view, not the start of a new statement.
      expect(decls.get("species_share")?.code).toContain(
         "# percent\n      season_share is",
      );
      expect(
         decls.get("species_share")?.code.endsWith("checklist_count\n  }"),
      ).toBe(true);
      // A view on a view: its pipeline and the where that wraps onto more lines.
      expect(decls.get("species_trend")?.code).toBe(
         [
            "#(doc) Share of checklists per season for a few familiar feeder birds.",
            "# line_chart { y=season_share series=common_name }",
            "species_trend is species_share -> {",
            "    where: common_name = 'Dark-eyed Junco' | 'House Finch'",
            "      | 'Northern Cardinal'",
            "    select: season, common_name, season_share",
            "  }",
         ].join("\n"),
      );
   });

   it("cuts measures and dimensions too, through an expression that wraps", () => {
      expect(decls.get("species_per_checklist")).toEqual({
         kind: "measure",
         code: [
            "#(doc) Average number of full species reported per checklist.",
            '# number="#,##0.0"',
            "species_per_checklist is",
            "    count() { where: species.is_true_species } / checklist_count",
         ].join("\n"),
      });
      expect(decls.get("checklist_count")?.code).toBe(
         '# number="#,##0"\nchecklist_count is count(SUB_ID)',
      );
      expect(decls.get("season_label")).toEqual({
         kind: "dimension",
         code: "season_label is pick 'early' when month_num < 1\n    else 'late'",
      });
   });

   it("reads only the source asked for: another source's fields and its renames are not this one's", () => {
      expect(decls.has("common_name")).toBe(false);
      expect(decls.has("species")).toBe(false);
      const taxonomy = fieldDeclarations(FEEDERWATCH, "taxonomy");
      expect([...taxonomy.keys()]).toEqual(["common_name"]);
   });

   it("reads keyword blocks, comma-separated entries, and the source a source extends", () => {
      const text = `source: base is duckdb.table('t') extend {
  measure:
    # currency
    revenue is sum(amount), order_count is count()
  view:
    by_day is { group_by: order_day aggregate: revenue }
    # bar_chart
    by_week is by_day + { limit: 5 }
}

source: narrowed is base extend {
  where: amount > 0
  view: by_week is { group_by: order_week }
}
`;
      const base = fieldDeclarations(text, "base");
      expect(base.get("revenue")?.code).toBe(
         "# currency\nrevenue is sum(amount)",
      );
      expect(base.get("order_count")?.code).toBe("order_count is count()");
      expect(base.get("by_day")?.code).toBe(
         "by_day is { group_by: order_day aggregate: revenue }",
      );
      expect(base.get("by_week")?.code).toBe(
         "# bar_chart\nby_week is by_day + { limit: 5 }",
      );

      const narrowed = fieldDeclarations(text, "narrowed");
      // Its own declaration wins over the one it inherits; the rest come from `base`.
      expect(narrowed.get("by_week")?.code).toBe(
         "by_week is { group_by: order_week }",
      );
      expect(narrowed.get("by_day")?.code).toBe(
         "by_day is { group_by: order_day aggregate: revenue }",
      );
   });

   it("cuts from the name when the line has trailing whitespace, for every entry on it", () => {
      const text =
         "source: s is duckdb.table('t') extend {\n  dimension: foo is bar  \n  measure: a is count(), b is sum(x)   \n}\n";
      const found = fieldDeclarations(text, "s");
      expect(found.get("foo")?.code).toBe("foo is bar");
      expect(found.get("a")?.code).toBe("a is count()");
      expect(found.get("b")?.code).toBe("b is sum(x)");
   });

   it("passes a base's fields on through `extend` only, not through a query's `->`", () => {
      const text = `source: a is duckdb.table('t') extend {
  dimension: carrier is upper(code)
}
source: b is a -> { group_by: carrier }
source: c is a extend { }
`;
      expect(fieldDeclarations(text, "b").get("carrier")).toBeUndefined();
      expect(fieldDeclarations(text, "c").get("carrier")?.code).toBe(
         "carrier is upper(code)",
      );
   });

   it("leaves out a name declared twice, and has nothing without the text or the source", () => {
      const text = `source: s is duckdb.table('t') extend {
  view: v is { group_by: a }
  view: v is { group_by: b }
}
`;
      expect(fieldDeclarations(text, "s").has("v")).toBe(false);
      expect(fieldDeclarations(undefined, "s").size).toBe(0);
      expect(fieldDeclarations(text, "missing").size).toBe(0);
   });

   it("is not thrown by brackets in strings, comments and tags", () => {
      const text = `source: s is duckdb.table('t') extend {
  # bar_chart { y=x
  view: v is {
    where: label != "}" // a } in a comment
    group_by: label
  }
  view: w is { group_by: label }
}
`;
      const decls2 = fieldDeclarations(text, "s");
      expect(decls2.get("v")?.code).toBe(
         '# bar_chart { y=x\nv is {\n    where: label != "}" // a } in a comment\n    group_by: label\n  }',
      );
      expect(decls2.get("w")?.code).toBe("w is { group_by: label }");
   });

   it("reads a one-line source, which also inherits every field its base declares", () => {
      const text = `source: base is duckdb.table('t') extend {
  # currency
  dimension: carrier is upper(code)
  measure: a is count()
}
#(doc) One line.
source: one is base extend { measure: c is count() }
`;
      const one = fieldDeclarations(text, "one");
      // Not the source's own doc line: the entry doesn't start its line.
      expect(one.get("c")).toEqual({ kind: "measure", code: "c is count()" });
      expect(one.get("carrier")).toEqual({
         kind: "dimension",
         code: "# currency\ncarrier is upper(code)",
      });
      expect(one.get("a")?.code).toBe("a is count()");
   });

   it("reads a field on the `extend {` line and the ones on the lines after it", () => {
      const text = `source: s is duckdb.table('t') extend { dimension: d is upper(code)
  measure: m is count()
  view: v is { group_by: d }
}
`;
      const found = fieldDeclarations(text, "s");
      expect(found.get("d")).toEqual({
         kind: "dimension",
         code: "d is upper(code)",
      });
      expect(found.get("m")).toEqual({ kind: "measure", code: "m is count()" });
      expect(found.get("v")).toEqual({
         kind: "view",
         code: "v is { group_by: d }",
      });
   });

   it("takes the kind from a keyword with an access modifier, and leaves out a comment among the tags", () => {
      const text = `source: s is duckdb.table('t') extend {
  dimension:
    d is upper(code)
  public measure:
    #(doc) Orders.
    // a comment between the tags
    # number="#,##0"
    m is count()
    n is sum(x)
}
`;
      const found = fieldDeclarations(text, "s");
      expect(found.get("d")?.kind).toBe("dimension");
      expect(found.get("m")).toEqual({
         kind: "measure",
         code: '#(doc) Orders.\n# number="#,##0"\nm is count()',
      });
      expect(found.get("n")).toEqual({ kind: "measure", code: "n is sum(x)" });
   });

   it("reads a file with CRLF line endings", () => {
      const text =
         "source: s is duckdb.table('t') extend {\r\n  # currency\r\n  measure: m is\r\n    sum(x)\r\n}\r\n";
      expect(fieldDeclarations(text, "s").get("m")?.code).toBe(
         "# currency\nm is\n    sum(x)",
      );
   });

   it("still reads the fields of a file mid-edit", () => {
      const text = `source: s is duckdb.table('t') extend {
  dimension: ok is upper(code)
  measure: bad is count(
}
`;
      expect(fieldDeclarations(text, "s").get("ok")?.code).toBe(
         "ok is upper(code)",
      );
   });
});
