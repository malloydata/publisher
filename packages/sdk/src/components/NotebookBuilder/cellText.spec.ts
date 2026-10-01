// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { ApiError } from "../ApiErrorDisplay";
import { cellFailure } from "./cellResult";
import { captionOf, cellQueries, queryCode } from "./cellText";
import {
   notebookSourceRefused,
   readNotebookSource,
   type NotebookSource,
} from "./readNotebookSource";

async function sourceOf(text: string): Promise<NotebookSource> {
   const result = await readNotebookSource(text);
   if (notebookSourceRefused(result)) throw new Error(result.refused);
   return result.source;
}

const RUN = "run: duckdb.sql('select 1 as x') -> { select: x }";

describe("cellQueries", () => {
   const withProse = (prose: string, block: string) =>
      [
         "## artifact { kind=notebook }",
         "// why",
         '#" Revenue by month',
         `#(markdown) ${prose}`,
         "#|(markdown)",
         block,
         // At another column than the opener's, so the block does not close here.
         "  |# indented, so not a closer",
         "still prose #(access_filter) naming gated_source",
         "|#",
         "#|(text)",
         "Text prose.",
         "|#",
         "# bar_chart",
         RUN,
         "",
      ].join("\n");

   it("sends a query cell without its prose notes and caption, keeping tags and comments", async () => {
      const source = await sourceOf(
         withProse("Attached, naming #(authorize) and gated.", "Block prose."),
      );
      expect(cellQueries(source).get("0")).toBe(
         `// why\n# bar_chart\n${RUN}\n`,
      );
   });

   it("sends the same text whatever the prose says, so the cached result still applies", async () => {
      const one = await sourceOf(withProse("One.", "First."));
      const two = await sourceOf(
         withProse("Two, longer.", "Second\n\nparagraph."),
      );
      expect(cellQueries(one).get("0")).toBe(cellQueries(two).get("0")!);
   });

   it("takes an indented note's leading whitespace with it, so a block below still closes", async () => {
      // [name, the prose lines, the lines that run]; Malloy closes a block only at its opener's column.
      const cases: [string, string[], string[]][] = [
         ["indented prose", ["  #(markdown) a"], ["#|(doc)", "doc body", "|#"]],
         ["indented caption", ['  #" cap'], ["#|(doc)", "doc body", "|#"]],
         [
            "before a tag block",
            ["  #(markdown) a"],
            ["#|", "# bar_chart", "|#"],
         ],
         [
            "indented prose block",
            ["  #|(markdown)", "x", "  |#"],
            ["#|(doc)", "doc body", "|#"],
         ],
      ];
      for (const [name, prose, kept] of cases) {
         const source = await sourceOf(
            ["## artifact { kind=notebook }", ...prose, ...kept, RUN, ""].join(
               "\n",
            ),
         );
         expect([name, cellQueries(source).get("0")]).toEqual([
            name,
            [...kept, RUN, ""].join("\n"),
         ]);
      }
   });

   it("keeps a definition cell's text, and a query cell with no prose, byte for byte", async () => {
      const text = `## artifact { kind=notebook }\nsource: a is duckdb.sql("select 1 as x")\n\n# bar_chart\n${RUN}\n`;
      const source = await sourceOf(text);
      const queries = cellQueries(source);
      for (const cell of source.cells)
         expect(queries.get(cell.id)).toBe(
            text.slice(cell.span.start, cell.span.end),
         );
   });
});

describe("queryCode", () => {
   it("drops prose and caption lines, keeping tags, comments and the run", () => {
      const slice = [
         "// why",
         '#" Revenue by month',
         "#(markdown) Attached.",
         "#|(markdown)",
         "Block prose.",
         "|#",
         "# bar_chart",
         "run: a -> by_month",
         "",
      ].join("\n");
      expect(queryCode(slice)).toBe("// why\n# bar_chart\nrun: a -> by_month");
   });
});

describe("captionOf", () => {
   it("finds a caption after an attached prose block, and none past the statement", () => {
      expect(
         captionOf(
            '#|(markdown)\nBlock prose.\n|#\n// note\n#" Revenue\n# bar_chart\nrun: a -> b\n#" not this\n',
         ),
      ).toBe("Revenue");
      expect(captionOf("run: a -> b\n")).toBeUndefined();
   });
});

describe("cellFailure", () => {
   it("reads 404 and 403 as unavailable, and only Malloy's restricted code as restricted", () => {
      expect(cellFailure({ name: "", message: "", status: 404 })).toBe(
         "unavailable",
      );
      expect(cellFailure({ name: "", message: "", status: 403 })).toBe(
         "unavailable",
      );
      const restricted = {
         name: "",
         message: "",
         status: 400,
         data: {
            code: 400,
            message: "raw SQL is not permitted",
            problems: [
               { code: "field-not-found" },
               { code: "restricted-construct-forbidden" },
            ],
         },
      } as ApiError;
      expect(cellFailure(restricted)).toBe("restricted");
      // The message alone says nothing: an ordinary compile error mentioning the word is still an error.
      expect(
         cellFailure({
            ...restricted,
            data: {
               code: 400,
               message: "restricted",
               problems: [{ code: "field-not-found" }],
            },
         } as ApiError),
      ).toBe("error");
      expect(cellFailure({ ...restricted, status: 500 })).toBe("error");
      expect(cellFailure(undefined)).toBe("error");
   });
});
