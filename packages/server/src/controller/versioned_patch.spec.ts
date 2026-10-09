// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { changedFields, echoes } from "./versioned_patch";

// What `latest` reads as now, as a GET of the package returns it.
const current = {
   name: "sales",
   versionId: "1.1.0",
   scope: "package",
   explores: ["sales.malloy"],
   queryMetadata: { tags: { team: "growth" } },
   materialization: { schedule: "0 * * * *", freshness: null },
   warnings: ["something"],
};

const changed = (body: Record<string, unknown>) =>
   changedFields(body, current, "sales", "gs://bucket/sales/1.1.0.zip");

describe("changedFields", () => {
   it("accepts the package sent back whole", () => {
      expect(
         changed({ ...current, location: "gs://bucket/sales/1.1.0.zip" }),
      ).toEqual([]);
   });

   it("ignores read-only fields, whatever they say", () => {
      expect(
         changed({
            versionId: "9.9.9",
            warnings: [],
            status: { serving: false },
         }),
      ).toEqual([]);
   });

   it("reads null, absent and empty-string fields as unset", () => {
      expect(
         changed({
            name: null,
            location: "",
            explores: null,
            queryMetadata: null,
         }),
      ).toEqual([]);
   });

   it("accepts the spec's default for a field latest leaves unset", () => {
      expect(changedFields({ scope: "package" }, {}, "sales", null)).toEqual(
         [],
      );
      expect(changedFields({ scope: "version" }, {}, "sales", null)).toEqual([
         "scope",
      ]);
   });

   it("reads a top-level empty list as unset, as a generated Java client sends one", () => {
      expect(changed({ explores: [], warnings: [] })).toEqual([]);
   });

   it("refuses an empty object, or an emptied field inside one, that would clear what latest has", () => {
      expect(changed({ queryMetadata: {} })).toEqual(["queryMetadata"]);
      expect(changed({ materialization: { schedule: null } })).toEqual([
         "materialization",
      ]);
   });

   it("accepts an empty list for what latest has none of", () => {
      expect(
         changedFields(
            { explores: [] },
            { explores: undefined },
            "sales",
            null,
         ),
      ).toEqual([]);
   });

   it("compares an object on the fields the client names", () => {
      // A client from an older spec, which never heard of `freshness`.
      expect(changed({ materialization: { schedule: "0 * * * *" } })).toEqual(
         [],
      );
      expect(changed({ materialization: { schedule: "5 * * * *" } })).toEqual([
         "materialization",
      ]);
   });

   it("refuses another name, another location, and a real content change", () => {
      expect(
         changed({
            name: "other",
            location: "gs://bucket/elsewhere.zip",
            explores: ["other.malloy"],
            scope: "version",
         }),
      ).toEqual(["name", "location", "explores", "scope"]);
   });

   it("leaves the fields the PATCH applies to the PATCH", () => {
      expect(
         changed({ description: "new", manifestLocation: "gs://m/x.json" }),
      ).toEqual([]);
   });
});

describe("echoes", () => {
   it("matches lists element by element, in order", () => {
      expect(echoes(["a", "b"], ["a", "b"])).toBe(true);
      expect(echoes(["b", "a"], ["a", "b"])).toBe(false);
      expect(echoes(["a"], ["a", "b"])).toBe(false);
   });
});
