// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { readFileSync } from "fs";
import { resolve } from "path";

/**
 * The `versionId` contract as api-doc.yaml declares it, for clients generated
 * from the spec:
 *
 *  - every operation that takes the shared `versionId` parameter declares the
 *    400 a malformed one answers (VERSION_ID_INVALID), so a strict client
 *    does not meet an undeclared status;
 *  - `versionId` is a plain string wherever it is declared, as before
 *    versions: an `anyOf` renders as an empty interface in typescript-axios,
 *    so a call passing request options positionally would bind them to
 *    `versionId` with no type error.
 *
 * Parsed with the runtime's own YAML reader (no dependency).
 */

const REPO_ROOT = resolve(import.meta.dir, "../../..");

type Operation = {
   parameters?: { $ref?: string; name?: string }[];
   responses?: Record<string, unknown>;
   requestBody?: unknown;
};

const doc = Bun.YAML.parse(
   readFileSync(resolve(REPO_ROOT, "api-doc.yaml"), "utf8"),
) as {
   paths: Record<string, Record<string, Operation>>;
   components: {
      parameters: Record<string, { schema?: unknown }>;
      schemas: Record<string, { properties?: Record<string, unknown> }>;
   };
};

const METHODS = ["get", "put", "post", "patch", "delete"];

/** Every operation, as "METHOD /path", with its definition. */
function operations(): [string, Operation][] {
   const out: [string, Operation][] = [];
   for (const [route, item] of Object.entries(doc.paths)) {
      for (const method of METHODS) {
         const op = item[method];
         if (op) out.push([`${method.toUpperCase()} ${route}`, op]);
      }
   }
   return out;
}

const VERSION_ID_REF = "#/components/parameters/versionId";

describe("versionId in api-doc.yaml", () => {
   it("declares a 400 on every operation that takes the shared versionId parameter", () => {
      const taking = operations().filter(([, op]) =>
         (op.parameters ?? []).some((p) => p.$ref === VERSION_ID_REF),
      );
      // The package-scoped read routes, compile, skills, agents, data apps,
      // events and the materialization routes.
      expect(taking.length).toBeGreaterThanOrEqual(30);
      const undeclared = taking
         .filter(([, op]) => !op.responses?.["400"])
         .map(([name]) => name);
      expect(undeclared).toEqual([]);
   });

   it("declares a 400 on every operation that takes versionId in its body", () => {
      const taking = operations().filter(([, op]) =>
         /QueryRequest|CreateMaterializationRequest/.test(
            JSON.stringify(op.requestBody ?? {}),
         ),
      );
      expect(taking.length).toBeGreaterThan(0);
      expect(
         taking.filter(([, op]) => !op.responses?.["400"]).map(([n]) => n),
      ).toEqual([]);
   });

   it("types versionId as a plain string, never an anyOf", () => {
      expect(doc.components.parameters.versionId.schema).toEqual({
         type: "string",
      });
      for (const schema of ["QueryRequest", "CreateMaterializationRequest"]) {
         const versionId = doc.components.schemas[schema].properties
            ?.versionId as Record<string, unknown>;
         expect([schema, versionId.type]).toEqual([schema, "string"]);
         expect([schema, versionId.$ref, versionId.anyOf]).toEqual([
            schema,
            undefined,
            undefined,
         ]);
      }
      expect(doc.components.schemas).not.toHaveProperty("VersionIdOrEmpty");
   });

   it("declares no version on the Materialization record: it goes out in metadata", () => {
      expect(
         Object.keys(doc.components.schemas.Materialization.properties ?? {}),
      ).not.toContain("version");
   });
});
