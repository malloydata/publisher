// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   exploreOf,
   modelDefIfAny,
   modelDefOf,
   plannedModelDefOf,
   primaryKeyOf,
   queryTimezoneOf,
   recordedTimezoneOf,
   sourceDefOf,
   timezoneOf,
} from "./compiled_model";

describe("compiled_model", () => {
   it("reads each compiler-internal slot", () => {
      const modelDef = { contents: {} };
      expect(modelDefOf({ _modelDef: modelDef })).toBe(modelDef);
      expect(modelDefIfAny(undefined)).toBeUndefined();
      expect(modelDefIfAny({})).toBeUndefined();
      expect(sourceDefOf({ _sourceDef: 1 })).toBe(1);
      expect(exploreOf({ _explore: 2 })).toBe(2);
      expect(plannedModelDefOf({ _model: { _modelDef: modelDef } })).toBe(
         modelDef,
      );
      expect(plannedModelDefOf({})).toBeUndefined();
   });

   it("reads a declared primary key only as a string", () => {
      expect(primaryKeyOf({ primaryKey: "id" })).toBe("id");
      expect(primaryKeyOf({ primaryKey: 3 })).toBeUndefined();
      expect(primaryKeyOf(undefined)).toBeUndefined();
   });

   it("reads a time zone as recorded, and as named", () => {
      expect(recordedTimezoneOf({ queryTimezone: "" })).toBe("");
      expect(timezoneOf({ queryTimezone: "" })).toBeUndefined();
      expect(timezoneOf({ queryTimezone: "UTC" })).toBe("UTC");
      expect(timezoneOf(undefined)).toBeUndefined();
   });

   it("takes a query-shaped source's zone from the last stage that sets one", () => {
      expect(
         queryTimezoneOf({
            queryTimezone: "UTC",
            query: {
               pipeline: [
                  { queryTimezone: "Asia/Tokyo" },
                  {},
                  { queryTimezone: "America/New_York" },
               ],
            },
         }),
      ).toBe("America/New_York");
      expect(queryTimezoneOf({ queryTimezone: "UTC" })).toBe("UTC");
      expect(queryTimezoneOf({ type: "table" })).toBeUndefined();
   });
});
