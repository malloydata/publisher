// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   stripDefinitionWrapper,
   stripDocAnnotations,
   viewDeclaration,
} from "./codeDisplay";

describe("stripDocAnnotations", () => {
   it("drops a `#(doc)` line, leaving the definition", () => {
      const code = [
         "#(doc) Year the recall was reported (truncated to Jan 1)",
         "    recall_year is `Report Received Date`.year",
      ].join("\n");
      expect(stripDocAnnotations(code)).toBe(
         "recall_year is `Report Received Date`.year",
      );
   });

   it('drops consecutive `#"` doc lines', () => {
      const code = [
         '#" First paragraph.',
         '#" Second paragraph.',
         "total is sum(x)",
      ].join("\n");
      expect(stripDocAnnotations(code)).toBe("total is sum(x)");
   });

   it("keeps non-doc annotations — they say how the field behaves", () => {
      const code = ["#(index)", "state is upper(state_code)"].join("\n");
      expect(stripDocAnnotations(code)).toBe(code);
   });

   it("leaves an undocumented definition untouched", () => {
      expect(stripDocAnnotations("total is sum(x)")).toBe("total is sum(x)");
   });

   it("drops docs that follow another annotation — both orders are legal Malloy", () => {
      // A `#(index)` first must not end the scan, or the docstring stays in the
      // code block as well as in the hover.
      const code = [
         "#(index)",
         '#" What the field means.',
         "state is upper(state_code)",
      ];
      expect(stripDocAnnotations(code.join("\n"))).toBe(
         ["#(index)", "state is upper(state_code)"].join("\n"),
      );
   });

   it("keeps the surviving annotations in their original order", () => {
      const code = ["#(doc) Prose.", "#(index)", "# bar_chart", "state is x"];
      expect(stripDocAnnotations(code.join("\n"))).toBe(
         ["#(index)", "# bar_chart", "state is x"].join("\n"),
      );
   });
});

describe("stripDefinitionWrapper", () => {
   it("keeps `name is …` on an expression, so it still reads as a definition", () => {
      // Stripping it left a bare identifier under the field's own name, which
      // reads as a stray reference rather than "this field is defined as that".
      expect(
         stripDefinitionWrapper("category_grain", "category_grain is category"),
      ).toBe("category_grain is category");
      expect(
         stripDefinitionWrapper("airport_count", "airport_count is count()"),
      ).toBe("airport_count is count()");
   });

   it("keeps the leading kind keyword on an expression too", () => {
      expect(
         stripDefinitionWrapper(
            "airport_count",
            "measure: airport_count is count()",
         ),
      ).toBe("measure: airport_count is count()");
   });

   it("unwraps a view block's outer braces and dedents the body", () => {
      // A block IS worth unwrapping: the braces and their indent are pure
      // wrapper, and `group_by:` / `aggregate:` describe themselves.
      const code = [
         "by_state is {",
         "    where: state != null",
         "    group_by: state",
         "    aggregate: airport_count",
         "  }",
      ].join("\n");
      expect(stripDefinitionWrapper("by_state", code)).toBe(
         [
            "where: state != null",
            "group_by: state",
            "aggregate: airport_count",
         ].join("\n"),
      );
   });

   it("unwraps a block even with the kind keyword present", () => {
      expect(
         stripDefinitionWrapper(
            "by_state",
            "view: by_state is {\n  group_by: state\n}",
         ),
      ).toBe("group_by: state");
   });

   it("leaves non-matching slices verbatim (never mangle what it can't parse)", () => {
      expect(
         stripDefinitionWrapper("origin_code", "rename: origin_code is origin"),
      ).toBe("rename: origin_code is origin");
      expect(
         stripDefinitionWrapper("engines", "duckdb.table('data/x.parquet')"),
      ).toBe("duckdb.table('data/x.parquet')");
   });

   it("escapes regex metacharacters in field names", () => {
      // `a+b` must not be treated as a regex; the wrapper is recognized (and
      // kept, since the body is an expression) rather than mangled.
      expect(stripDefinitionWrapper("a+b", "a+b is 1")).toBe("a+b is 1");
      expect(stripDefinitionWrapper("a+b", "a+b is {\n  group_by: x\n}")).toBe(
         "group_by: x",
      );
   });
});

describe("viewDeclaration", () => {
   it("shows a view's whole declaration, putting back the keyword a stored slice starts without", () => {
      const code =
         "#(doc) Recall count trend over time by year\n# line_chart\nby_year is {\n  group_by: recall_year\n  aggregate: recall_count\n}";
      expect(viewDeclaration("by_year", code)).toBe(
         "# line_chart\nview: by_year is {\n  group_by: recall_year\n  aggregate: recall_count\n}",
      );
   });

   it("lines the body up under the keyword, however deep the view sat in its file", () => {
      // As stored: cut from inside `source: recalls is … extend {`, so the first line
      // is at column zero and the rest keep their four-space nesting.
      const code = [
         "by_year_manufacturer_line_chart is {",
         "    nest: top_manufacturers is {",
         "      group_by: Manufacturer",
         "      limit: 5",
         "    }",
         "  }",
         "  -> {",
         "    # x",
         "    group_by: x.recall_year",
         "  }",
      ].join("\n");
      expect(viewDeclaration("by_year_manufacturer_line_chart", code)).toBe(
         [
            "view: by_year_manufacturer_line_chart is {",
            "  nest: top_manufacturers is {",
            "    group_by: Manufacturer",
            "    limit: 5",
            "  }",
            "}",
            "-> {",
            "  # x",
            "  group_by: x.recall_year",
            "}",
         ].join("\n"),
      );
   });

   it("keeps a slice that already carries the keyword, and anything unexpected, as written", () => {
      const withKeyword = "view: by_year is {\n  group_by: recall_year\n}";
      expect(viewDeclaration("by_year", withKeyword)).toBe(withKeyword);
      expect(viewDeclaration("by_year", "other_view + { limit: 3 }")).toBe(
         "other_view + { limit: 3 }",
      );
   });
});
