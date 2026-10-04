// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `retrieval` block of a package's publisher.json: how THIS package is
 * searched and indexed. The server operator's side (credentials, limits) is
 * publisher.config.json. A package can choose how its index is built, never
 * past what the operator allows.
 *
 * Only keys whose code exists are accepted. Any other key is an error naming
 * the valid ones, so a typo (or a key from a later release) does not silently
 * do nothing.
 */

import { PackageManifestError } from "../errors";

export const PACKAGE_REPRESENTATIONS = ["single", "facets"] as const;
export type PackageRepresentation = (typeof PACKAGE_REPRESENTATIONS)[number];

const RETRIEVAL_KEYS = ["representation"] as const;

export interface PackageRetrievalSettings {
   /** `single`: one row per entity. `facets`: a name row plus doc chunks. */
   representation: PackageRepresentation;
}

export const DEFAULT_PACKAGE_RETRIEVAL: PackageRetrievalSettings = {
   representation: "single",
};

function fail(message: string): never {
   throw new PackageManifestError(message);
}

function describe(value: unknown): string {
   return JSON.stringify(value) ?? "nothing";
}

/**
 * Validate the block's shape and return what it says. Throws a
 * PackageManifestError (424: the package is not served until the author fixes
 * the file) naming the key and a fix.
 */
export function parsePackageRetrieval(raw: unknown): PackageRetrievalSettings {
   if (raw === undefined || raw === null) {
      return { ...DEFAULT_PACKAGE_RETRIEVAL };
   }
   if (typeof raw !== "object" || Array.isArray(raw)) {
      fail(
         `Invalid publisher.json retrieval: expected an object, got ${describe(raw)}. ` +
            `Fix: "retrieval": { "representation": "single" }`,
      );
   }
   const obj = raw as Record<string, unknown>;
   const unknown = Object.keys(obj).filter(
      (k) => !(RETRIEVAL_KEYS as readonly string[]).includes(k),
   );
   if (unknown.length > 0) {
      fail(
         `Invalid publisher.json retrieval: unknown key ${unknown.map((k) => `'${k}'`).join(", ")}. ` +
            `Valid keys: ${RETRIEVAL_KEYS.join(", ")}.`,
      );
   }

   const representation = obj.representation ?? "single";
   if (
      typeof representation !== "string" ||
      !(PACKAGE_REPRESENTATIONS as readonly string[]).includes(representation)
   ) {
      fail(
         `Invalid publisher.json retrieval.representation: expected one of ${PACKAGE_REPRESENTATIONS.join(", ")}, got ${describe(obj.representation)}. ` +
            `Fix: "representation": "single" (one row per entity) or "facets" (a name row plus doc chunks).`,
      );
   }
   return { representation: representation as PackageRepresentation };
}

/**
 * Parse the block. Called when the package loads, so an edit takes effect on
 * reload and a bad value stops the load with a message naming the key.
 */
export async function readPackageRetrieval(
   _packageRoot: string,
   raw: unknown,
): Promise<PackageRetrievalSettings> {
   return parsePackageRetrieval(raw);
}
