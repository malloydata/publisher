// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { conversionRefused, convertLegacyNotebook } from "./legacyNotebook";
import { quoteFilterLiterals } from "./tagParse";
import { readDashboardDocument, readFailed } from "./readDocument";
import { spliceDashboardDocument, spliceFailed } from "./spliceDocument";

const BODY = `import "../models/orders.malloy"

source: orders_tiles is orders extend {
  view: by_cat is by_category
}
`;
const LINE = `##! experimental.givens
## artifact { title="T" tiles=["orders_tiles -> by_cat"] autorun=false givens { REGION=f'US' } }
${BODY}`;
const BLOCK = `##! experimental.givens
##| artifact { title="T" tiles=["orders_tiles -> by_cat"]
  autorun=false
  givens { REGION=f'US' }
}
|##
${BODY}`;
const LEGACY = (tag: string) => `##! experimental.givens
${tag}
import "../models/orders.malloy"

##(markdown) Hello

run: orders -> kpis
`;

describe("filter-literal givens in the artifact tag", () => {
   for (const [name, text] of [
      ["line", LINE],
      ["block", BLOCK],
   ]) {
      it(`reads the ${name} form`, async () => {
         const read = await readDashboardDocument(text);
         if (readFailed(read)) throw new Error(read.reason);
         expect(read.document.startingGivens).toEqual({ REGION: "f'US'" });
         expect(read.document.autorun).toBe(false);
         expect(read.document.tiles.map((t) => t.name)).toEqual(["by_cat"]);
      });

      it(`round-trips the ${name} form byte for byte`, async () => {
         const read = await readDashboardDocument(text);
         if (readFailed(read)) throw new Error(read.reason);
         const out = await spliceDashboardDocument(text, {
            ...read.document,
            title: "T",
         });
         if (spliceFailed(out)) throw new Error(out.reason);
         expect(out.source).toBe(text);
      });
   }

   for (const [name, tag] of [
      [
         "line",
         "## artifact { kind=notebook autorun=false givens { REGION=f'US' } }",
      ],
      [
         "block",
         "##| artifact { kind=notebook\n  autorun=false\n  givens { REGION=f'US' }\n}\n|##",
      ],
   ]) {
      it(`converts a legacy notebook with the ${name} tag and keeps the literal as written`, async () => {
         const result = await convertLegacyNotebook(LEGACY(tag));
         if (conversionRefused(result)) throw new Error(result.refused);
         expect(result.text).toContain("givens { REGION=f'US' }");
         const read = await readDashboardDocument(result.text);
         if (readFailed(read)) throw new Error(read.reason);
         expect(read.document.startingGivens).toEqual({ REGION: "f'US'" });
      });
   }

   it("reads a legacy notebook with a filter literal as a legacy notebook", async () => {
      const read = await readDashboardDocument(
         LEGACY(
            "## artifact { kind=notebook autorun=false givens { REGION=f'US' } }",
         ),
      );
      if (!readFailed(read)) throw new Error("expected a refusal");
      expect(read.legacyNotebook).toBe(true);
   });

   it("surfaces the tag's own syntax error when it does not parse", async () => {
      const read = await readDashboardDocument(
         LINE.replace("tiles=[", "tiles=[[[[").replace("autorun=false", "="),
      );
      if (!readFailed(read)) throw new Error("expected a refusal");
      expect(read.reason).toContain("does not parse");
   });
});

describe("quoteFilterLiterals", () => {
   it("quotes bare literals after = [ and , but not inside strings", () => {
      expect(quoteFilterLiterals(`# a=f'US' b=[f'x', f"y"] c="=f'z'"`)).toBe(
         `# a="f'US'" b=["f'x'", "f'y'"] c="=f'z'"`,
      );
   });
});
