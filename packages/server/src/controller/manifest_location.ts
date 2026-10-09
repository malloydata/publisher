// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { BadRequestError } from "../errors";

/**
 * Whether `value` is the gs:// or s3:// URI of a build manifest, as the spec
 * declares `manifestLocation`: a manifest is fetched from object storage,
 * never read from a path on this server.
 */
export function isManifestUri(value: string): boolean {
   return /^(gs|s3):\/\/[^/]+\/./.test(value);
}

/**
 * A `manifestLocation` a request binds a published version to: a manifest
 * URI, or null (also for an empty string) to serve live; undefined when the
 * request does not name one. Anything else is refused with 400.
 */
export function versionManifestLocation(
   value: unknown,
): string | null | undefined {
   if (value === undefined) return undefined;
   if (value === null || value === "") return null;
   if (typeof value === "string" && isManifestUri(value)) return value;
   throw new BadRequestError(
      "`manifestLocation` must be the gs:// or s3:// URI of a build manifest, or null to serve live.",
   );
}
