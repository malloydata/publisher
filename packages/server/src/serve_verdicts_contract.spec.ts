// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { readFileSync } from "fs";
import { resolve } from "path";
import { parse } from "yaml";

/**
 * `serveVerdicts` is served on `GET …/packages/{pkg}/status` and on a dry run,
 * so a client generated from api-doc.yaml must see it declared as a property
 * of both, not only described in prose.
 */
const REPO_ROOT = resolve(import.meta.dir, "../../..");

describe("the serveVerdicts contract", () => {
   const schemas = parse(
      readFileSync(resolve(REPO_ROOT, "api-doc.yaml"), "utf8"),
   ).components.schemas as Record<
      string,
      {
         properties?: Record<
            string,
            {
               type?: string;
               description?: string;
               items?: { $ref?: string; type?: string };
            }
         >;
      }
   >;

   it("declares serveVerdicts on the package's serving state and on the package", () => {
      for (const schema of ["PackageServingState", "Package"]) {
         expect(schemas[schema].properties?.serveVerdicts?.items?.$ref).toBe(
            "#/components/schemas/ServeVerdict",
         );
      }
   });

   // Whether a built table is served: one field on every verdict, each model
   // with every reason, the same for `#(materialize)` and `#@ persist`.
   it("declares where and why the served copy does not read a table", () => {
      const verdict = schemas.ServeVerdict.properties ?? {};
      expect(verdict.servedCopyReason).toBeUndefined();
      expect(verdict.notServedIn?.type).toBe("array");
      expect(verdict.notServedIn?.items?.$ref).toBe(
         "#/components/schemas/NotServedIn",
      );
      // Consumers are told that `serves` alone is not every model.
      expect(verdict.notServedIn?.description).toContain("must read it");
      const entry = schemas.NotServedIn as {
         required?: string[];
         properties?: Record<
            string,
            { type?: string; description?: string; items?: { type?: string } }
         >;
      };
      expect(entry.required).toEqual(["model", "reasons"]);
      expect(entry.properties?.model?.type).toBe("string");
      expect(entry.properties?.reasons?.type).toBe("array");
      expect(entry.properties?.reasons?.items?.type).toBe("string");
      for (const code of [
         "cross_connection_join",
         "cross_connection_unbuilt",
         "not_rerooted",
         "most actionable first",
      ]) {
         expect(entry.properties?.reasons?.description).toContain(code);
      }
      // `served_copy` covers `#@ persist` sources, not only `#(materialize)`.
      expect(verdict.reason?.description).toMatch(
         /`served_copy` \(a source the served copy\s+reads, `#\(materialize\)` or `#@ persist`/,
      );
   });

   // Every change to the tables a query reads compiles the served copy
   // before the swap (a bind, a freshness edge), so the pending marker is
   // retained in the schema for older clients and never produced.
   it("retains the served copy's pending marker, never produced", () => {
      const pending = schemas.ServeVerdict.properties?.servedCopyPending as
         | { type?: string; deprecated?: boolean; description?: string }
         | undefined;
      expect(pending?.type).toBe("boolean");
      expect(pending?.deprecated).toBe(true);
      expect(pending?.description).toContain("no longer produced");
   });
});
