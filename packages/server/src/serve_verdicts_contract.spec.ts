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

   // Whether a built table is served: the served copy's own reason, and the
   // models it is not served in, on every verdict (not a parallel object).
   it("declares where and why the served copy does not read a table", () => {
      const verdict = schemas.ServeVerdict.properties ?? {};
      expect(verdict.servedCopyReason?.type).toBe("string");
      expect(verdict.notServedIn?.type).toBe("array");
      expect(verdict.notServedIn?.items?.type).toBe("string");
      // The reasons are the package warning's, named in the contract.
      for (const code of [
         "cross_connection_join",
         "cross_connection_unbuilt",
         "not_rerooted",
      ]) {
         expect(verdict.servedCopyReason?.description).toContain(code);
      }
      // `served_copy` covers `#@ persist` sources, not only `#(materialize)`.
      expect(verdict.reason?.description).toMatch(
         /`served_copy` \(a source the served copy\s+reads, `#\(materialize\)` or `#@ persist`/,
      );
   });
});
