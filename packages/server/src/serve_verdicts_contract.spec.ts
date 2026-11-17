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
      { properties?: Record<string, { items?: { $ref?: string } }> }
   >;

   it("declares serveVerdicts on the package's serving state and on the package", () => {
      for (const schema of ["PackageServingState", "Package"]) {
         expect(schemas[schema].properties?.serveVerdicts?.items?.$ref).toBe(
            "#/components/schemas/ServeVerdict",
         );
      }
   });
});
