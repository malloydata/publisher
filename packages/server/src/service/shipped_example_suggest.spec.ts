// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The storefront reports' Category control is the example agents and authors
 * copy, so it has to work in the package they copy it into. Most real packages
 * curate their surface with an `index.malloy`, and there a `suggest` may read
 * only a source the surface exports. A suggest over an imported but unexported
 * source compiles, then its options query answers 404 and the picker says
 * "Could not load the options for this control".
 *
 * The example used to suggest from `products`. It now suggests from
 * `order_items`, the source its tiles already read. This loads storefront with
 * an `index.malloy` that exports only `order_items`, the shape a copying author
 * most often has, and runs each report's own Category suggest through the real
 * query path.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { NotQueryableError } from "../errors";
import { Package } from "./package";

const STOREFRONT = path.resolve(
   import.meta.dir,
   "../../../../examples/storefront",
);
const REPORTS = [
   "overview.malloy",
   "overview-story.malloy",
   "category-review.malloy",
];

let dir: string;
let pkg: Package;
let duckdb: DuckDBConnection;

beforeAll(async () => {
   dir = fs.mkdtempSync(path.join(os.tmpdir(), "storefront-curated-"));
   for (const f of [...REPORTS, "storefront.malloy", "publisher.json"]) {
      fs.copyFileSync(path.join(STOREFRONT, f), path.join(dir, f));
   }
   fs.cpSync(path.join(STOREFRONT, "data"), path.join(dir, "data"), {
      recursive: true,
   });
   // A surface that exports what the tiles read, and the other two pickers'
   // sources, but not `products`.
   fs.writeFileSync(
      path.join(dir, "index.malloy"),
      'import "storefront.malloy"\n\nexport { order_items, regions, brand_suggest }\n',
   );
   const { MalloyConfig, FixedConnectionMap } = await import(
      "@malloydata/malloy"
   );
   duckdb = new DuckDBConnection("duckdb", ":memory:", dir);
   const connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
   const malloyConfig = new MalloyConfig({ connections: {} });
   malloyConfig.wrapConnections(() => connections);
   pkg = await Package.create(
      "examples",
      "storefront-curated",
      dir,
      malloyConfig,
   );
});

afterAll(async () => {
   await duckdb?.close();
   if (dir) fs.rmSync(dir, { recursive: true, force: true });
});

/** The Category control's suggest, as the report file writes it. */
function categorySuggest(report: string): {
   source: string;
   dimension: string;
} {
   const text = fs.readFileSync(path.join(STOREFRONT, report), "utf8");
   const m =
      /# label="Category"[^\n]*suggest \{ source=(\w+) dimension=(\w+) \}/.exec(
         text,
      );
   if (!m) throw new Error(`${report} has no Category suggest`);
   return { source: m[1], dimension: m[2] };
}

describe("storefront's Category control works on a curated surface", () => {
   for (const report of REPORTS) {
      it(`${report}: its suggest answers options`, async () => {
         const { source, dimension } = categorySuggest(report);
         const { compactResult } = await pkg
            .getModel(report)!
            .getQueryResults(
               undefined,
               undefined,
               `run: ${source} -> { group_by: ${dimension}; limit: 50 }`,
            );
         expect((compactResult as unknown[]).length).toBeGreaterThan(0);
      });
   }

   it("raises no suggest warning for the Category control", () => {
      const warnings = (pkg.getPackageMetadata().warnings ?? []).filter((w) =>
         /Filter CATEGORY .* suggests from/.test(w.message ?? ""),
      );
      expect(warnings).toEqual([]);
   });

   it("would refuse the old form, a suggest over the unexported products", async () => {
      await expect(
         pkg
            .getModel("overview.malloy")!
            .getQueryResults(
               undefined,
               undefined,
               "run: products -> { group_by: category; limit: 50 }",
            ),
      ).rejects.toBeInstanceOf(NotQueryableError);
   });
});
