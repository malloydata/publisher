// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { drillClausesText, type DrillClause } from "./drillClauses";
import { drillClickRelay } from "./drillClickRelay";
import type { DrillClickPayload } from "./resolveDrill";

const REGION: DrillClause = {
   filter: {
      kind: "literal_equality",
      expression: { kind: "field_reference", name: "region", path: ["v"] },
      value: { kind: "string_literal", string_value: "West" },
   },
};
const NO_PRODUCT: DrillClause = {
   filter: {
      kind: "literal_equality",
      expression: { kind: "field_reference", name: "product", path: ["v"] },
      value: { kind: "null_literal" },
   },
};

function click(timezone?: () => string | undefined): DrillClickPayload {
   return {
      field: { name: "region", getEffectiveQueryTimezone: timezone },
      value: "West",
   } as unknown as DrillClickPayload;
}

/** The relay, and every payload it forwarded once the held click is released. */
function relayed() {
   const forwarded: DrillClickPayload[] = [];
   const relay = drillClickRelay((payload) => forwarded.push(payload));
   const settle = () => new Promise<void>((resolve) => queueMicrotask(resolve));
   return { relay, forwarded, settle };
}

describe("drillClickRelay", () => {
   it("puts the row the renderer reports in the same dispatch on the click", async () => {
      const { relay, forwarded, settle } = relayed();
      relay.onClick(click());
      relay.onDrill({ stableDrillClauses: [REGION] });
      expect(forwarded).toHaveLength(0);
      await settle();
      expect(forwarded).toHaveLength(1);
      expect(forwarded[0].drillClauses).toEqual([REGION]);
   });

   it("forwards a click the renderer reports no row for as it came", async () => {
      const { relay, forwarded, settle } = relayed();
      relay.onClick(click());
      await settle();
      expect(forwarded).toHaveLength(1);
      expect(forwarded[0].drillClauses).toBeUndefined();
      expect(forwarded[0].value).toBe("West");
   });

   it("leaves the clauses off when the row has no stable form", async () => {
      const { relay, forwarded, settle } = relayed();
      relay.onClick(click());
      relay.onDrill({ stableDrillClauses: undefined });
      await settle();
      expect(forwarded[0].drillClauses).toBeUndefined();
   });

   it("never puts one click's row on another click", async () => {
      const { relay, forwarded, settle } = relayed();
      // A row with no click held, then a click that gets no row.
      relay.onDrill({ stableDrillClauses: [REGION] });
      relay.onClick(click());
      await settle();
      // A row that arrives after its click went out.
      relay.onDrill({ stableDrillClauses: [REGION] });
      relay.onClick(click());
      await settle();
      expect(forwarded.map((p) => p.drillClauses)).toEqual([
         undefined,
         undefined,
      ]);
   });

   it("reads the clicked field's query timezone, and survives a field that throws", async () => {
      const { relay, forwarded, settle } = relayed();
      relay.onClick(click(() => "Asia/Tokyo"));
      relay.onClick(
         click(() => {
            throw new Error("no query");
         }),
      );
      await settle();
      expect(forwarded.map((p) => p.timezone)).toEqual([
         "Asia/Tokyo",
         undefined,
      ]);
   });
});

describe("drillClausesText", () => {
   it("writes every clause, or none when any one cannot be written", () => {
      expect(drillClausesText([REGION])).toBe("v.region = 'West'");
      // A subset would drill to a different set of rows than the one clicked.
      expect(drillClausesText([REGION, NO_PRODUCT])).toBeUndefined();
      expect(drillClausesText([])).toBeUndefined();
   });
});
