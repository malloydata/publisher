// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { readFileSync } from "fs";
import { resolve } from "path";
import { parse } from "yaml";
import type { components } from "./api";
import { deriveBuildPlan } from "./service/materialization/plan/build_plan";

/**
 * `BuildPlan.omittedSources` is what lets a host retire a table safely, so a
 * client generated from api-doc.yaml must see it declared, and its `reason`
 * must stay an open string: a generated client rejects an enum value it does
 * not know, and the set of codes grows.
 */
const REPO_ROOT = resolve(import.meta.dir, "../../..");

type Schema = {
   type?: string;
   enum?: unknown[];
   required?: string[];
   properties?: Record<string, Schema & { items?: { $ref?: string } }>;
};

describe("the BuildPlan.omittedSources contract", () => {
   const schemas = parse(
      readFileSync(resolve(REPO_ROOT, "api-doc.yaml"), "utf8"),
   ).components.schemas as Record<string, Schema>;

   it("declares omittedSources on BuildPlan as a list of OmittedSource", () => {
      const field = schemas.BuildPlan.properties?.omittedSources;
      expect(field?.type).toBe("array");
      expect(field?.items?.$ref).toBe("#/components/schemas/OmittedSource");
   });

   it("keeps reason an open string, and requires sourceID and reason", () => {
      const omitted = schemas.OmittedSource;
      expect(omitted.required?.sort()).toEqual(["reason", "sourceID"]);
      expect(omitted.properties?.reason?.type).toBe("string");
      expect(omitted.properties?.reason?.enum).toBeUndefined();
      expect(Object.keys(omitted.properties ?? {}).sort()).toEqual([
         "reason",
         "sourceID",
      ]);
   });

   it("round-trips a derived plan through JSON as the generated type, with only declared fields", () => {
      const plan = deriveBuildPlan([], {}, {}, undefined, undefined, null, {
         omittedSources: [
            { sourceID: "a@file:///p/m.malloy", reason: "projection_failed" },
         ],
      });
      const wire = JSON.parse(
         JSON.stringify(plan),
      ) as components["schemas"]["BuildPlan"];
      expect(wire.omittedSources).toEqual([
         { sourceID: "a@file:///p/m.malloy", reason: "projection_failed" },
      ]);
      const declared = Object.keys(schemas.OmittedSource.properties ?? {});
      for (const entry of wire.omittedSources ?? []) {
         for (const key of Object.keys(entry)) {
            expect(declared).toContain(key);
         }
      }
      const planFields = Object.keys(schemas.BuildPlan.properties ?? {});
      for (const key of Object.keys(wire)) expect(planFields).toContain(key);
   });
});
