// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as path from "path";
import { fileURLToPath } from "url";
import { BadRequestError } from "../errors";

/** What a `manifestLocation` that is refused is told it may be. */
export const MANIFEST_LOCATION_FORMS =
   "a gs:// or s3:// URI, a file:// URL or an absolute path to a build manifest";

/**
 * Whether `value` names a build manifest this server can read: a gs:// or
 * s3:// URI, a file:// URL, or an absolute path (a manifest on a mounted
 * volume), which are what the manifest loader reads. A relative path is not:
 * it would resolve against the server's working directory, which the caller
 * cannot know, so a manifest named that way could bind on one server and fail
 * on the next. A file URL must be written `file://`, the only form the loader
 * recognizes: it reads `file:/abs` as a relative path and binds nothing.
 */
export function isManifestLocation(value: string): boolean {
   if (/^(gs|s3):\/\/[^/]+\/./.test(value)) return true;
   if (value.startsWith("file://")) {
      try {
         return path.isAbsolute(fileURLToPath(value));
      } catch {
         return false;
      }
   }
   return !value.includes("://") && path.isAbsolute(value);
}

/**
 * A `manifestLocation` a request binds a published version to: a manifest
 * location (see isManifestLocation), or null (also for an empty string) to
 * serve live; undefined when the request does not name one. Anything else is
 * refused with 400.
 */
export function versionManifestLocation(
   value: unknown,
): string | null | undefined {
   if (value === undefined) return undefined;
   if (value === null || value === "") return null;
   if (typeof value === "string" && isManifestLocation(value)) return value;
   throw new BadRequestError(
      `\`manifestLocation\` must be ${MANIFEST_LOCATION_FORMS}, or null to serve live.`,
   );
}
