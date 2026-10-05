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

describe("a filter literal holding a quote", () => {
   const QUOTED = LINE.replace("f'US'", `f"it's"`);

   it("reads as the wrapped body, and a givens edit re-emits it quoted, reading back the same", async () => {
      const read = await readDashboardDocument(QUOTED);
      if (readFailed(read)) throw new Error(read.reason);
      expect(read.document.startingGivens).toEqual({ REGION: "f'it's'" });
      const out = await spliceDashboardDocument(QUOTED, {
         ...read.document,
         startingGivens: { ...read.document.startingGivens, OTHER: "x" },
      });
      if (spliceFailed(out)) throw new Error(out.reason);
      // The bare f"…" spelling does not survive an edit of the givens block.
      expect(out.source).toContain(`givens { REGION="f'it's'" OTHER="x" }`);
      const again = await readDashboardDocument(out.source);
      if (readFailed(again)) throw new Error(again.reason);
      expect(again.document.startingGivens).toEqual({
         REGION: "f'it's'",
         OTHER: "x",
      });
   });

   it("leaves the spelling alone when the givens are not what changed", async () => {
      const read = await readDashboardDocument(QUOTED);
      if (readFailed(read)) throw new Error(read.reason);
      const out = await spliceDashboardDocument(QUOTED, {
         ...read.document,
         title: "T",
      });
      if (spliceFailed(out)) throw new Error(out.reason);
      expect(out.source).toBe(QUOTED);
   });
});

describe("a tag value Tag.text() throws on", () => {
   it("is refused with a reason rather than thrown, so the editor can say why", async () => {
      const read = await readDashboardDocument(
         LINE.replace("f'US'", "@2024-13-01"),
      );
      if (!readFailed(read)) throw new Error("expected a refusal");
      expect(read.reason).toContain("cannot be read");
   });
});

describe("quoteFilterLiterals", () => {
   it("quotes bare literals after = [ and , but not inside strings", () => {
      expect(quoteFilterLiterals(`# a=f'US' b=[f'x', f"y"] c="=f'z'"`)).toBe(
         `# a="f'US'" b=["f'x'", "f'y'"] c="=f'z'"`,
      );
   });
});
