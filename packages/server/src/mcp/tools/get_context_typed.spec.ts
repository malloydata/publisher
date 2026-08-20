import { describe, expect, it } from "bun:test";
import {
   isTypedRequest,
   kindsForTarget,
   resolveScope,
   stableEntityId,
   validateTypedCall,
} from "./get_context_typed";

describe("get_context typed helpers", () => {
   it("treats search_targets as the typed contract", () => {
      expect(isTypedRequest({})).toBe(false);
      expect(isTypedRequest({ search_targets: [] })).toBe(true);
   });

   it("maps view targets to views and named queries", () => {
      expect(kindsForTarget("view")).toEqual(["view", "query"]);
      expect(stableEntityId("query", "orders", "top")).toBe("view:orders:top");
   });

   it("resolves scopes over legacy names", () => {
      expect(
         resolveScope({
            environmentName: "legacy-env",
            packageName: "legacy-pkg",
            sourceName: "legacy-source",
            scopes: [
               {
                  environment: "examples",
                  package: "storefront",
                  source: "order_items",
               },
            ],
         }),
      ).toEqual({
         environmentName: "examples",
         packageName: "storefront",
         modelPath: undefined,
         sourceName: "order_items",
         entityName: undefined,
      });
   });

   it("rejects mixed source and entity targets", () => {
      expect(
         validateTypedCall({
            targets: [
               { target_type: "source", search_text: "orders" },
               { target_type: "measure", search_text: "revenue" },
            ],
         }),
      ).toMatch(/Do not mix/);
   });

   it("rejects offset alongside search_text", () => {
      expect(
         validateTypedCall({
            targets: [{ target_type: "source", search_text: "orders" }],
            offset: 20,
         }),
      ).toMatch(/offset is only valid/);
   });
});
