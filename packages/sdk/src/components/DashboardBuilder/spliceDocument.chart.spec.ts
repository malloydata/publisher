// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { chartLineText } from "./chartLine";
import { openDocument, refused, splice, spliced } from "./testing/fixtures";
import { spliceFailed } from "./spliceDocument";

const FILE = (tags: string) => `##! experimental.givens
## artifact { title="T" tiles=["a -> x", "a -> y"] } dashboard { columns=12 }
import { one } from "../m.malloy"

source: a is one extend {
${tags}  view: x is vx

  # colspan=6
  view: y is vy
}`;

const SPARK = "  # bar_chart { size=spark }\n";
const LABELLED = '  # line_chart label="Revenue"\n';

describe("a tile's chart line", () => {
   it("reads the tile's chart from the file, with no catalog", async () => {
      expect((await openDocument(FILE(""))).tiles[0].chart).toBeUndefined();
      expect((await openDocument(FILE("  # bar_chart\n"))).tiles[0].chart).toBe(
         "bar_chart",
      );
      expect(
         (await openDocument(FILE(`  ${chartLineText("line_chart")}\n`)))
            .tiles[0].chart,
      ).toBe("line_chart");
      expect(
         (await openDocument(FILE(`  ${chartLineText("none")}\n`))).tiles[0]
            .chart,
      ).toBe("none");
      // A line with properties is not the picker's to model.
      expect((await openDocument(FILE(SPARK))).tiles[0].chart).toBe("custom");
      expect((await openDocument(FILE(LABELLED))).tiles[0].chart).toBe(
         "custom",
      );
      expect(
         (await openDocument(FILE("  # bar_chart\n  # line_chart\n"))).tiles[0]
            .chart,
      ).toBe("custom");
   });

   it.each([SPARK, LABELLED])(
      "keeps %p byte for byte through a label and a width edit",
      async (line) => {
         const out = await spliced(FILE(line), (d) => {
            d.tiles[0].label = "Revenue";
            d.tiles[0].colspan = 4;
         });
         expect(out).toContain(line);
         expect(out).toContain('# label="Revenue"');
      },
   );

   it("refuses a chart change on a tile with a line it does not model, quoting the line", async () => {
      const reason = await refused(FILE(SPARK), (d) => {
         d.tiles[0].chart = "line_chart";
      });
      expect(reason).toContain("# bar_chart { size=spark }");
      const second = await refused(FILE(LABELLED), (d) => {
         d.tiles[0].chart = "default";
      });
      expect(second).toContain('# line_chart label="Revenue"');
   });

   it("writes a pick above the declaration, and reads it back", async () => {
      const out = await spliced(FILE(""), (d) => {
         d.tiles[0].chart = "line_chart";
      });
      expect(out).toContain(
         `  ${chartLineText("line_chart")}\n  view: x is vx`,
      );
      expect((await openDocument(out)).tiles[0].chart).toBe("line_chart");
   });

   it("replaces a recognized line, bare or written, and removes it for Default", async () => {
      const bare = await spliced(FILE("  # bar_chart\n"), (d) => {
         d.tiles[0].chart = "big_value";
      });
      expect(bare).toContain(`  ${chartLineText("big_value")}\n`);
      expect(bare).not.toContain("  # bar_chart\n");
      const gone = await spliced(bare, (d) => {
         d.tiles[0].chart = "default";
      });
      expect(gone).toContain("  view: x is vx");
      expect(gone).not.toContain("big_value");
      const none = await spliced(FILE("  # bar_chart\n"), (d) => {
         d.tiles[0].chart = "none";
      });
      expect(none).toContain(`  ${chartLineText("none")}\n`);
      expect((await openDocument(none)).tiles[0].chart).toBe("none");
   });

   it("leaves a recognized bare line alone while its chart is unchanged", async () => {
      const out = await spliced(FILE("  # bar_chart\n"), (d) => {
         d.tiles[0].label = "L";
      });
      expect(out).toContain("  # bar_chart\n");
      expect(out).not.toContain("-line_chart");
   });

   it("puts a chart on a tile it adds", async () => {
      const out = await spliced(FILE(""), (d) => {
         d.tiles.push({
            name: "z_tile",
            source: "a",
            declaration: { kind: "reference", from: "vz" },
            colspan: 6,
            chart: "scatter_chart",
         });
      });
      const z = (await openDocument(out)).tiles[2];
      expect(z.chart).toBe("scatter_chart");
      expect(out).toContain(chartLineText("scatter_chart"));
   });

   it("refuses a chart it cannot write", async () => {
      for (const chart of ["sparkline", "pie", "custom", ""]) {
         const reason = await refused(FILE(""), (d) => {
            (d.tiles[0] as { chart?: string }).chart = chart;
         });
         expect(reason).toContain("is not a chart");
      }
      const added = await refused(FILE(""), (d) => {
         d.tiles.push({
            name: "z_tile",
            source: "a",
            declaration: { kind: "reference", from: "vz" },
            chart: "custom",
         });
      });
      expect(added).toContain("is not a chart");
   });

   it("leaves an unchanged custom chart alone", async () => {
      const result = await splice(FILE(SPARK), (d) => {
         d.tiles[1].label = "Other";
      });
      expect(spliceFailed(result)).toBe(false);
   });
});

describe("strings written into annotations", () => {
   it.each([
      ["label", "Revenue\nby month"],
      ["subtitle", "a\rb"],
      ["label", "x ## authorize y"],
      ["subtitle", "see #(row_authorize) here"],
   ] as const)("refuses a tile %s of %p", async (key, value) => {
      const reason = await refused(FILE(""), (d) => {
         d.tiles[0][key] = value;
      });
      expect(reason).toContain(`tile ${key}`);
   });

   it("refuses a title and a given label or description the same way", async () => {
      expect(
         await refused(FILE(""), (d) => {
            d.title = "a\nb";
         }),
      ).toContain("A title is one line");
      const given = {
         name: "CATEGORY",
         type: "filter<string>",
         default: "f''",
      };
      expect(
         await refused(FILE(""), (d) => {
            d.localGivens = [{ ...given, label: "x\ny" }];
         }),
      ).toContain("A given label is one line");
      expect(
         await refused(FILE(""), (d) => {
            d.localGivens = [{ ...given, description: "#(authorize) me" }];
         }),
      ).toContain(
         "A given description cannot contain what reads as an access-control tag",
      );
   });

   it("does not refuse a string already in the file when something else changes", async () => {
      const source = FILE('  # label="Ask about # authorize"\n');
      const out = await spliced(source, (d) => {
         d.tiles[0].colspan = 3;
      });
      expect(out).toContain('# label="Ask about # authorize"');
   });

   it("refuses a tile, source or view name that is not an identifier", async () => {
      const reason = await refused(FILE(""), (d) => {
         d.tiles.push({
            name: "bad name",
            source: "a",
            declaration: { kind: "reference", from: "vz" },
         });
      });
      expect(reason).toBe(
         'The name "bad name" cannot be written as a Malloy name.',
      );
      const from = await refused(FILE(""), (d) => {
         d.tiles.push({
            name: "z_tile",
            source: "a",
            declaration: { kind: "reference", from: "v z\n" },
         });
      });
      expect(from).toBe(
         'The name "v z\\n" cannot be written as a Malloy name.',
      );
   });
});

describe("chart lines: review fixes", () => {
   const SALES = '  # label="Sales viz"\n';

   it("does not read a word inside a quoted value as a chart tag", async () => {
      expect((await openDocument(FILE(SALES))).tiles[0].chart).toBeUndefined();
      const out = await spliced(FILE(""), (d) => {
         d.tiles[0].label = "Sales viz";
      });
      expect(out).toContain('# label="Sales viz"');
      const picked = await spliced(FILE(SALES), (d) => {
         d.tiles[0].chart = "bar_chart";
      });
      expect(picked).toContain(`  ${chartLineText("bar_chart")}\n`);
      expect(picked).toContain('# label="Sales viz"');
   });

   it("keeps a tile's line when the document leaves `chart` out; only default removes it", async () => {
      const source = FILE("  # big_value\n");
      const kept = await spliced(source, (d) => {
         delete d.tiles[0].chart;
         d.tiles[0].colspan = 3;
      });
      expect(kept).toContain("  # big_value\n");
      const noop = await splice(source, (d) => {
         delete d.tiles[0].chart;
      });
      expect(spliceFailed(noop)).toBe(false);
      const removed = await spliced(source, (d) => {
         d.tiles[0].chart = "default";
      });
      expect(removed).not.toContain("big_value");
   });

   it.each(["  # sparkline\n", "  # -bar_chart\n"])(
      "refuses a chart change on a recognized line the picker cannot show (%p)",
      async (line) => {
         expect((await openDocument(FILE(line))).tiles[0].chart).toBe("custom");
         const reason = await refused(FILE(line), (d) => {
            d.tiles[0].chart = "line_chart";
         });
         expect(reason).toContain(line.trim());
      },
   );

   it("reads the lines behind a custom chart", async () => {
      const doc = await openDocument(FILE("  # bar_chart\n  # line_chart\n"));
      expect(doc.tiles[0].chartLines).toEqual(["# bar_chart", "# line_chart"]);
   });

   it("refuses an authorize-like line in a changed description", async () => {
      const reason = await refused(FILE(""), (d) => {
         d.description = "fine\nsee ## authorize here";
      });
      expect(reason).toContain("A description line cannot contain what reads");
   });
});
