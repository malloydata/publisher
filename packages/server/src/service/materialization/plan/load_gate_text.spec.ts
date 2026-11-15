// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { components } from "../../../api";
import type { PackageAnnotationFinding } from "../../annotation_reader";
import { loadGateText } from "./load_gate_text";

type BuildPlan = components["schemas"]["BuildPlan"];

function planOf(
   sources: Record<
      string,
      { name: string; tableName?: string; address: string }
   >,
): BuildPlan {
   return {
      graphs: [],
      refusedSources: {},
      sources: Object.fromEntries(
         Object.entries(sources).map(([id, s]) => [
            id,
            {
               name: s.name,
               sourceID: id,
               connectionName: "duckdb",
               dialect: "duckdb",
               sourceEntityId: s.address,
               annotationFields: s.tableName ? { name: s.tableName } : {},
               modelPath: "m.malloy",
            },
         ]),
      ),
   } as unknown as BuildPlan;
}

const COLLISION: PackageAnnotationFinding = {
   route: "materialize",
   tier: "malformed",
   code: "materialization-table-collision",
   message: '#(materialize) on source "x" is invalid: its name= `t` is a table',
   subject: "x",
   model: "m.malloy",
};

describe("loadGateText", () => {
   it("names every invalid finding and author collision with its code, and nothing valid", () => {
      const text = loadGateText(
         {
            plan: planOf({
               "a@file:///m.malloy": {
                  name: "a",
                  tableName: "main.t",
                  address: "ba",
               },
               "b@file:///m.malloy": {
                  name: "b",
                  tableName: "t",
                  address: "bb",
               },
               "c@file:///m.malloy": { name: "c", address: "bc" },
            }),
            annotationFindings: [
               COLLISION,
               {
                  route: "@",
                  tier: "unservable",
                  code: "persist-not-built",
                  message: "valid, refused alone",
               },
            ],
            incrementalDeclarations: {},
         },
         { enforceCollisions: true, versionScoped: false },
      );
      const lines = text.split("\n");
      expect(lines[0]).toEndWith(". [materialization-table-collision]");
      expect(
         lines.filter((l) => l.includes('"a" (m.malloy), "b" (m.malloy)')),
      ).toHaveLength(1);
      expect(text).not.toContain("valid, refused alone");
      expect(text).not.toContain('"c"');
   });

   it("under the opt-out, refuses collisions alone rather than failing", () => {
      expect(
         loadGateText(
            {
               plan: planOf({
                  "a@file:///m.malloy": {
                     name: "a",
                     tableName: "t",
                     address: "ba",
                  },
                  "b@file:///m.malloy": {
                     name: "b",
                     tableName: "t",
                     address: "bb",
                  },
               }),
               annotationFindings: [COLLISION],
               incrementalDeclarations: {},
            },
            { enforceCollisions: false, versionScoped: false },
         ),
      ).toBe("");
   });
});
