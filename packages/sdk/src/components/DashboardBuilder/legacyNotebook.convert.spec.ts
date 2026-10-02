// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { isQueryTile, isTextTile } from "./document";
import {
   conversionRefused,
   convertLegacyNotebook,
   notebookSourceRefused,
   readNotebookSource,
} from "./legacyNotebook";
import { readDashboardDocument, readFailed } from "./readDocument";
import {
   spliceDashboardDocument,
   spliceFailed,
   syntaxErrors,
} from "./spliceDocument";

const REPO = path.resolve(import.meta.dir, "../../../../..");
const CATEGORY_REVIEW = path.join(
   REPO,
   "examples/storefront/notebooks/category-review.malloy",
);
const FIXTURES = path.join(
   REPO,
   "packages/server/tests/fixtures/notebooks-malloyyo/notebooks",
);

const read = (file: string) =>
   fs.readFileSync(file, "utf8").replace(/\r\n/g, "\n");

/** The converted text, or a thrown reason. */
async function convert(text: string): Promise<string> {
   const result = await convertLegacyNotebook(text);
   if (conversionRefused(result)) throw new Error(result.refused);
   return result.text;
}

/** Why the converter refuses `text`. */
async function refusal(text: string) {
   const result = await convertLegacyNotebook(text);
   if (!conversionRefused(result)) throw new Error("expected a refusal");
   return result;
}

async function document(text: string) {
   const result = await readDashboardDocument(text);
   if (readFailed(result)) throw new Error(result.reason);
   return result.document;
}

/** Compiles `text` as `file` inside the package at `pkg` on DuckDB, and hands back a way to run a tile's view. */
async function compiled(pkg: string, file: string, text: string) {
   const { Runtime } = await import("@malloydata/malloy");
   const { DuckDBConnection } = await import("@malloydata/db-duckdb");
   const url = new URL(pathToFileURL(path.join(pkg, file)).href);
   const connection = new DuckDBConnection("duckdb", ":memory:", pkg);
   const runtime = new Runtime({
      urlReader: {
         readURL: async (at: URL) =>
            at.href === url.href
               ? text
               : fs.readFileSync(fileURLToPath(at.href), "utf8"),
      },
      connections: { lookupConnection: async () => connection },
   } as never);
   const model = runtime.loadModel(url);
   await model.getModel();
   return {
      rows: async (source: string, view: string) =>
         (
            await model.loadQuery(`run: ${source} -> ${view}`).run()
         ).data.toObject(),
      close: () => connection.close(),
   };
}

const HEAD = `##! experimental.givens
## artifact { kind=notebook title="T" }
import "../models/orders.malloy"
`;

describe("convertLegacyNotebook: the storefront category review", () => {
   it("becomes a layout notebook that reads back tile for tile", async () => {
      const original = read(CATEGORY_REVIEW);
      const converted = await convert(original);
      expect(await syntaxErrors(converted)).toEqual([]);

      const doc = await document(converted);
      expect(doc.kind).toBe("notebook");
      expect(doc.title).toBe("Category review");
      expect(doc.description).toContain("One category at a time");
      expect(
         doc.tiles.map((t) => `${isTextTile(t) ? "text" : "query"}`),
      ).toEqual(["text", "text", "query", "text", "query", "text", "query"]);

      const texts = doc.tiles.filter(isTextTile).map((t) => t.markdown);
      expect(texts[0]).toStartWith("# Category review\n\nPick a **Category**");
      expect(texts[1]).toBe(
         "The monthly trend says which way revenue is moving.",
      );
      expect(texts[3]).toBe(
         "Last, the best-selling products, defined once as a named query and then run.",
      );

      const queries = doc.tiles.filter(isQueryTile);
      expect(queries.map((t) => t.source)).toEqual([
         "order_items_tiles",
         "order_items_tiles",
         "order_items_tiles",
      ]);
      expect(queries.map((t) => t.label)).toEqual([
         "Revenue by month",
         "Top brands",
         "The ten best-selling products in the selected category",
      ]);
      expect(queries.map((t) => t.subtitle)).toEqual([
         "Revenue by month for the selected category",
         "The eight brands with the most revenue in the selected category",
         undefined,
      ]);
      expect(queries.map((t) => t.chart)).toEqual([
         "line_chart",
         "bar_chart",
         undefined,
      ]);
      expect(doc.localGivens?.map((g) => g.name)).toEqual(["CATEGORY"]);
   });

   it("carries the header and the givens over as written, and leaves no run behind", async () => {
      const original = read(CATEGORY_REVIEW);
      const converted = await convert(original);
      const header = original.slice(0, original.indexOf("import {"));
      expect(converted).toContain(
         header
            .replace("## artifact {", "##| artifact {")
            .replace(" }\n", "\n  tiles=["),
      );
      expect(converted).toContain(
         '# description="Narrow to one product category. Leave empty for all"\n# label="Category" control=select suggest { source=products dimension=category }\ngiven: CATEGORY :: filter<string> is f\'\'\n',
      );
      expect(converted).not.toMatch(/^run:/m);
      // The named query was run once and nowhere else, so it became its view.
      expect(converted).not.toContain("query: top_products_in_category");
      expect(converted).toContain(
         "view: the_ten_best_selling_products_in_the is top_products + { where: category ~ $CATEGORY }",
      );
      expect(converted).not.toMatch(/\n\n\n/);
   });

   it("is a file the writer opens and writes back byte for byte", async () => {
      const converted = await convert(read(CATEGORY_REVIEW));
      const result = await spliceDashboardDocument(
         converted,
         await document(converted),
      );
      if (spliceFailed(result)) throw new Error(result.reason);
      expect(result.source).toBe(converted);
   });

   it("compiles under Malloy and its views run", async () => {
      const pkg = path.join(REPO, "examples/storefront");
      const converted = await convert(read(CATEGORY_REVIEW));
      const model = await compiled(
         pkg,
         "notebooks/converted.malloy",
         converted,
      );
      try {
         for (const view of [
            "revenue_by_month",
            "top_brands_2",
            "the_ten_best_selling_products_in_the",
         ])
            expect(
               (await model.rows("order_items_tiles", view)).length,
            ).toBeGreaterThan(0);
      } finally {
         await model.close();
      }
   });
});

describe("convertLegacyNotebook: a named query that ends in a line comment", () => {
   const PKG = path.dirname(FIXTURES);
   for (const [name, comment] of [
      ["//", "// c"],
      ["--", "-- c"],
   ] as const)
      it(`keeps a refinement out of a ${name} comment`, async () => {
         const converted = await convert(
            `${HEAD}\nquery: q is orders -> { group_by: region } ${comment}\n\nrun: q + { limit: 1 }\n`,
         );
         expect(converted).toMatch(
            new RegExp(`${comment}\\n\\s*\\+ \\{ limit: 1 \\}`),
         );
         const model = await compiled(PKG, "notebooks/c.malloy", converted);
         try {
            const [tile] = (await document(converted)).tiles.filter(
               isQueryTile,
            );
            expect(await model.rows(tile.source, tile.name)).toHaveLength(1);
         } finally {
            await model.close();
         }
      });

   it("keeps a following stage out of the comment", async () => {
      const converted = await convert(
         `${HEAD}\nquery: q is orders -> { group_by: region } // c\n\nrun: q -> { select: region }\n`,
      );
      expect(converted).toMatch(/\/\/ c\n\s*-> \{ select: region \}/);
      expect(await syntaxErrors(converted)).toEqual([]);
   });
});

describe("convertLegacyNotebook: every cell-format notebook in the corpus", () => {
   const files = fs
      .readdirSync(FIXTURES)
      .filter(
         (f) =>
            f.endsWith(".malloy") &&
            f !== "refused.malloy" &&
            f !== "layout.malloy",
      );
   for (const file of files) {
      it(`converts ${file} to one tile per cell, with its prose`, async () => {
         const original = read(path.join(FIXTURES, file));
         const source = await readNotebookSource(original);
         if (notebookSourceRefused(source)) throw new Error(source.refused);
         const expected = source.source.cells.reduce(
            (n, cell) =>
               n +
               (cell.kind === "markdown" ? 1 : 0) +
               (cell.kind === "query" ? 1 : 0) +
               (cell.kind !== "markdown" && cell.markdown !== undefined
                  ? 1
                  : 0),
            0,
         );
         const converted = await convert(original);
         expect(await syntaxErrors(converted)).toEqual([]);
         const doc = await document(converted);
         expect(doc.tiles).toHaveLength(expected);
         // Syntax is not enough: the file compiles against its package and every view tile runs.
         const model = await compiled(
            path.dirname(FIXTURES),
            `notebooks/${file}`,
            converted,
         );
         try {
            for (const tile of doc.tiles.filter(isQueryTile))
               await model.rows(tile.source, tile.name);
         } finally {
            await model.close();
         }
         // Every prose cell's text survives as a text tile.
         const prose = source.source.cells.flatMap((c) =>
            c.markdown === undefined ? [] : [c.markdown],
         );
         expect(doc.tiles.filter(isTextTile).map((t) => t.markdown)).toEqual(
            prose,
         );
         // And a converted notebook is a layout one: nothing left to convert.
         expect((await refusal(converted)).refused).toContain(
            "already lists its tiles",
         );
      });
   }

   it("passes a notebook the cell reader refuses through with its reason", async () => {
      const result = await refusal(read(path.join(FIXTURES, "refused.malloy")));
      expect(result.line).toBeGreaterThan(0);
   });
});

describe("convertLegacyNotebook: shapes", () => {
   it("keeps a named query that more than one run, or anything else, names", async () => {
      const converted = await convert(
         `${HEAD}\nquery: q is orders -> kpis\n\nrun: q\n\nrun: q + { limit: 1 }\n`,
      );
      expect(converted).toContain("query: q is orders -> kpis");
      expect(converted).toContain("view: tile_1 is kpis\n");
      expect(converted).toContain("view: tile_2 is kpis + { limit: 1 }");
   });

   it("keeps a run-once named query that a definition also names", async () => {
      const converted = await convert(
         `${HEAD}\nquery: q is orders -> kpis\n\nquery: r is q + { limit: 1 }\n\nrun: q\n`,
      );
      expect(converted).toContain("query: q is orders -> kpis");
      expect(converted).toContain("view: tile_1 is kpis");
   });

   it("runs a query of a query as its source's view with the later stages", async () => {
      const converted = await convert(
         `${HEAD}\nquery: q is orders -> kpis\n\nrun: q -> { select: order_count }\n`,
      );
      expect(converted).toContain(
         "view: tile_1 is kpis -> { select: order_count }",
      );
      const doc = await document(converted);
      expect(doc.tiles[0]).toMatchObject({
         name: "tile_1",
         source: "orders_tiles",
         declaration: { kind: "opaque" },
      });
   });

   it("refuses a refinement of a query with more than one stage", async () => {
      const result = await refusal(
         `${HEAD}\nquery: q is orders -> kpis -> { select: order_count }\n\nrun: q + { limit: 1 }\n`,
      );
      expect(result.refused).toContain("more than one stage");
      expect(result.line).toBe(7);
   });

   it("groups views by source, in the order the sources first run", async () => {
      const converted = await convert(
         `${HEAD}\nsource: other is orders extend { where: region = 'US' }\n\nrun: orders -> a\n\nrun: other -> b\n\nrun: orders -> c\n`,
      );
      const doc = await document(converted);
      expect(
         doc.tiles.map((t) => (isQueryTile(t) ? `${t.source}.${t.name}` : "")),
      ).toEqual([
         "orders_tiles.tile_1",
         "other_tiles.tile_2",
         "orders_tiles.tile_3",
      ]);
      expect(doc.sources.map((s) => s.name)).toEqual([
         "other",
         "orders_tiles",
         "other_tiles",
      ]);
      // The extension comes after the source it extends.
      expect(converted.indexOf("source: other is")).toBeLessThan(
         converted.indexOf("source: other_tiles is"),
      );
   });

   it("puts a caption on the tile as its label, or as its subtitle beside a label", async () => {
      const converted = await convert(
         `${HEAD}\n#" Plain caption\nrun: orders -> a\n\n#" Second caption\n# label="Given label"\nrun: orders -> b\n\n#" Third\n# label="L"\n# subtitle="S"\nrun: orders -> c\n`,
      );
      const queries = (await document(converted)).tiles.filter(isQueryTile);
      expect(queries.map((t) => [t.label, t.subtitle])).toEqual([
         ["Plain caption", undefined],
         ["Given label", "Second caption"],
         ["L", "S"],
      ]);
      // Nothing is lost when both are taken: the caption stays as a doc note.
      expect(converted).toContain('  # subtitle="S"\n  #" Third\n  view: l');
   });

   it("keeps a caption it cannot write into a tag as a doc note", async () => {
      const converted = await convert(
         `${HEAD}\n#" # authorize everyone\nrun: orders -> a\n`,
      );
      expect(converted).toContain('  #" # authorize everyone\n');
      expect(await syntaxErrors(converted)).toEqual([]);
   });

   it("escapes quotes and backslashes in a caption", async () => {
      const converted = await convert(
         `${HEAD}\n#" Say "hi" \\ there\nrun: orders -> a\n`,
      );
      const [tile] = (await document(converted)).tiles.filter(isQueryTile);
      expect(tile.label).toBe('Say "hi" \\ there');
   });

   it("keeps comments and unmodelled tags above a run, in order, with the view", async () => {
      const converted = await convert(
         `${HEAD}\n// why this one\n# big_value\n/* and a block\n   comment */\n# label="K"\nrun: orders -> kpis\n`,
      );
      expect(converted).toContain(
         '  // why this one\n  # big_value\n  /* and a block\n     comment */\n  # label="K"\n  view: k is kpis',
      );
      const [tile] = (await document(converted)).tiles.filter(isQueryTile);
      expect(tile.label).toBe("K");
   });

   it("keeps a (text) note above a run as it was rather than dropping it", async () => {
      const converted = await convert(
         `${HEAD}\n#(text) a side note\nrun: orders -> kpis\n`,
      );
      expect(converted).toContain("  #(text) a side note\n  view: tile_1");
   });

   it("indents a multi-line view body under its declaration", async () => {
      const converted = await convert(
         `${HEAD}\nrun: orders -> {\n  group_by: region\n  aggregate: order_count\n}\n`,
      );
      expect(converted).toContain(
         "  view: tile_1 is {\n    group_by: region\n    aggregate: order_count\n  }",
      );
      const [tile] = (await document(converted)).tiles.filter(isQueryTile);
      expect(tile.declaration.kind).toBe("inline");
   });

   it("moves a comment above a prose cell with it", async () => {
      const converted = await convert(
         `${HEAD}\n// the lead-in\n##(markdown) Hello\n\nrun: orders -> kpis\n`,
      );
      expect(converted).toContain(
         "// the lead-in\n##|(markdown) text_1\nHello\n|##\n",
      );
   });

   it("collapses the blank lines a removed run leaves", async () => {
      const converted = await convert(
         `${HEAD}\nrun: orders -> a\n\n\n\nrun: orders -> b\n\n##(markdown) After\n`,
      );
      expect(converted).not.toMatch(/\n\n\n/);
   });

   it("keeps a stray model note and trailing comment where they were", async () => {
      const converted = await convert(
         `${HEAD}\nrun: orders -> a\n\n##(filters) ["orders.region"]\n\n// the end\n`,
      );
      expect(converted).toContain('##(filters) ["orders.region"]');
      expect(
         converted.trimEnd().endsWith("}") || converted.includes("// the end"),
      ).toBe(true);
      expect(converted).toContain("// the end");
   });

   it("gives a prose line that would close its block one space of indent", async () => {
      const converted = await convert(
         `${HEAD}\n##(markdown) |## not a closer\n`,
      );
      expect(await syntaxErrors(converted)).toEqual([]);
      const [tile] = (await document(converted)).tiles.filter(isTextTile);
      expect(tile.markdown).toBe(" |## not a closer");
   });

   it("takes names that cannot collide with what the file already declares", async () => {
      const converted = await convert(
         `${HEAD}\nsource: orders_tiles is orders extend { dimension: tile_1 is 1 }\n\nrun: orders -> kpis\n`,
      );
      const doc = await document(converted);
      expect(doc.tiles[0]).toMatchObject({
         name: "tile_1_2",
         source: "orders_tiles_2",
      });
   });

   it("names the extension for a backtick-quoted source with a valid identifier", async () => {
      const converted = await convert(
         `${HEAD}\nrun: \`order-items\` -> kpis\n`,
      );
      expect(converted).toContain(
         "source: order_items_tiles is `order-items` extend {",
      );
      expect(converted).toContain('"order_items_tiles -> tile_1"');
      expect(await syntaxErrors(converted)).toEqual([]);
      expect((await document(converted)).tiles[0]).toMatchObject({
         source: "order_items_tiles",
      });
   });

   it("converts a statement with a very long run of spaces in linear time", async () => {
      const started = performance.now();
      const converted = await convert(
         `${HEAD}\nrun: orders ->${" ".repeat(80_000)}kpis\n`,
      );
      expect(performance.now() - started).toBeLessThan(2000);
      expect(await syntaxErrors(converted)).toEqual([]);
   });

   it("converts a CRLF notebook", async () => {
      const original =
         `${HEAD}\n##(markdown) Hello\n\n# label="K"\nrun: orders -> kpis\n`.replace(
            /\n/g,
            "\r\n",
         );
      const converted = await convert(original);
      expect(converted).not.toMatch(/(?<!\r)\n/);
      const doc = await document(converted.replace(/\r\n/g, "\n"));
      expect(doc.tiles.map((t) => t.name)).toEqual(["text_1", "k"]);
   });

   it("keeps the blank lines inside a block comment between cells", async () => {
      const converted = await convert(
         `${HEAD}\nrun: orders -> kpis\n\n/* first\n\n\n   indented */\n\nrun: orders -> by_month\n`,
      );
      expect(converted).toContain("/* first\n\n\n   indented */");
   });

   it("writes the tag as a block with a tile per line, whether it was a line or a block", async () => {
      const cells = "\n##(markdown) Hello\n\nrun: orders -> kpis\n";
      const fromLine = await convert(`${HEAD}${cells}`);
      expect(fromLine).toContain(
         '##| artifact { kind=notebook title="T"\n  tiles=[\n    text_1 { kind=text },\n    "orders_tiles -> tile_1"\n  ]\n}\n|##\n',
      );
      const block = `##! experimental.givens\n##| artifact { kind=notebook\n  title="T"\n}\n|##\nimport "../models/orders.malloy"\n${cells}`;
      const fromBlock = await convert(block);
      expect(fromBlock).toContain(
         '##| artifact { kind=notebook\n  title="T"\n  tiles=[\n    text_1 { kind=text },\n    "orders_tiles -> tile_1"\n  ]\n}\n|##\n',
      );
      expect(await syntaxErrors(fromBlock)).toEqual([]);
      expect((await document(fromBlock)).tiles.map((t) => t.name)).toEqual([
         "text_1",
         "tile_1",
      ]);
   });

   it("converts a notebook with no run or prose cells to an empty tile list", async () => {
      const converted = await convert(HEAD);
      const doc = await document(converted);
      expect(doc.tiles).toEqual([]);
      expect(doc.kind).toBe("notebook");
   });

   it("carries the tags above a consumed query onto the view it becomes, and names the view from its label", async () => {
      const converted = await convert(
         `${HEAD}\n# label="Mine"\n# big_value\nquery: q is orders -> kpis\n\nrun: q\n`,
      );
      expect(converted).not.toContain("query: q");
      expect(converted).toContain(
         'source: orders_tiles is orders extend {\n  # label="Mine"\n  # big_value\n  view: mine is kpis\n}',
      );
      const [tile] = (await document(converted)).tiles.filter(isQueryTile);
      expect(tile).toMatchObject({ name: "mine", label: "Mine" });
   });

   it("names a text tile around an identifier the file already declares", async () => {
      const converted = await convert(
         `${HEAD}\nsource: text_1 is orders extend {}\n\n##(markdown) Hello\n\nrun: orders -> kpis\n\n##(markdown) Two\n`,
      );
      const doc = await document(converted);
      expect(doc.tiles.map((t) => t.name)).toEqual([
         "text_1_2",
         "tile_1",
         "text_2",
      ]);
      expect(converted).toContain("source: text_1 is orders extend {}");
      expect(converted).toContain("##|(markdown) text_1_2\nHello\n|##");
      expect(doc.tiles.filter(isTextTile).map((t) => t.markdown)).toEqual([
         "Hello",
         "Two",
      ]);
   });

   it("extends a backquoted source and keeps the quotes on it", async () => {
      const converted = await convert(`${HEAD}\nrun: \`orders\` -> kpis\n`);
      expect(converted).toContain(
         "source: orders_tiles is `orders` extend {\n  view: tile_1 is kpis\n}",
      );
      expect(await syntaxErrors(converted)).toEqual([]);
      const [tile] = (await document(converted)).tiles.filter(isQueryTile);
      expect(tile).toMatchObject({ source: "orders_tiles", name: "tile_1" });
   });

   it("refuses a run with nothing after the arrow, at the line the reader names", async () => {
      // The cell reader's parse refuses it before the converter's own empty-body check can run.
      const result = await refusal(`${HEAD}\nrun: orders ->\n`);
      expect(result.line).toBeGreaterThan(0);
      expect(result.refused).toContain("could not parse");
   });

   it("follows a chain of ten named queries and refuses an eleventh instead of making a source of a query", async () => {
      const chain = (length: number) => {
         let text = `${HEAD}\nquery: q1 is orders -> kpis\n\n`;
         for (let i = 2; i <= length; i++)
            text += `query: q${i} is q${i - 1} -> { limit: ${i} }\n\n`;
         return `${text}run: q${length}\n`;
      };
      const ten = await convert(chain(10));
      expect(ten).toContain(
         "view: tile_1 is kpis -> { limit: 2 } -> { limit: 3 }",
      );
      expect(ten).not.toContain("query:");
      const result = await refusal(chain(11));
      expect(result.refused).toContain("more than 10 other queries");
      expect(result.line).toBe(27);
   });

   it("leaves a given: block of several givens in place above the tiles' sources", async () => {
      const given =
         "given:\n  REGION :: filter<string> is f''\n  SINCE :: date is @2023-01-01\n";
      const converted = await convert(
         `${HEAD}\n${given}\nrun: orders -> kpis\n`,
      );
      expect(converted).toContain(
         `import "../models/orders.malloy"\n\n${given}\nsource: orders_tiles is orders extend {`,
      );
      expect(await syntaxErrors(converted)).toEqual([]);
      const doc = await document(converted);
      expect(doc.localGivens?.map((g) => g.name)).toEqual(["REGION", "SINCE"]);
   });

   describe("refusals", () => {
      it("refuses a run that extends its source inline, naming the line", async () => {
         const result = await refusal(
            `${HEAD}\nrun: orders extend { dimension: x is 1 } -> { select: x }\n`,
         );
         expect(result.refused).toContain("extends its source inline");
         expect(result.line).toBe(5);
      });

      it("refuses a run whose source is not a name", async () => {
         const result = await refusal(
            `${HEAD}\nrun: duckdb.table('x') -> { select: y }\n`,
         );
         expect(result.refused).toContain("not a name");
         expect(result.line).toBe(5);
      });

      it("refuses a run that is not source -> view", async () => {
         const result = await refusal(`${HEAD}\nrun: something\n`);
         expect(result.refused).toContain("source -> view");
      });

      it("refuses a file that is not a notebook the cell reader can place", async () => {
         const result = await refusal(`${HEAD}\n;;[ "stray" ]\n`);
         expect(result.line).toBeGreaterThan(0);
      });
   });
});
