// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a PATCH of a package with published versions may carry. The route is
 * deprecated: a version's content is immutable, so only the changes that are
 * not content are kept (see PackageController.updateVersionedPackage). Clients
 * send whole objects, though, so a body that merely says what `latest`
 * already is must not be read as a change.
 *
 * What a field reads as when unset (the spec defaults, isUnset, isEmpty) is
 * shared with the PATCH of a package installed from a location
 * (service/package_patch.ts). How a sent value is compared differs, and
 * package_patch.ts's sameValue says why.
 */

import {
   PACKAGE_FIELD_DEFAULTS,
   isEmpty,
   isUnset,
} from "../service/package_patch";

/** The fields a PATCH of a versioned package applies, rather than checks. */
export const APPLIED_TO_A_VERSIONED_PACKAGE = new Set([
   "resource",
   "manifestLocation",
   "description",
]);

/**
 * The Package fields the spec declares read-only: ignored when a request
 * sends them back.
 */
export const READ_ONLY_PACKAGE_FIELDS = new Set([
   "versionId",
   "latestVersion",
   "loaded",
   "archiveStatus",
   "exploresWarnings",
   "warnings",
   "manifestBindingStatus",
   "manifestEntryCount",
   "boundManifestUri",
   "status",
   "storageServeBindings",
   "buildPlan",
   "embeddingIndex",
]);

/**
 * Whether `sent` says only what `current` already is. An object is compared
 * on the fields it names, since a client generated from an older spec leaves
 * out the fields it does not know; below the top level an empty list or
 * object, and a null, mean empty, so they echo only an empty value.
 */
export function echoes(sent: unknown, current: unknown): boolean {
   if (isEmpty(sent)) return isEmpty(current);
   if (Array.isArray(sent)) {
      return (
         Array.isArray(current) &&
         current.length === sent.length &&
         sent.every((item, i) => echoes(item, current[i]))
      );
   }
   if (typeof sent === "object") {
      if (!current || typeof current !== "object" || Array.isArray(current)) {
         return false;
      }
      return Object.entries(sent as Record<string, unknown>).every(([k, v]) =>
         echoes(v, (current as Record<string, unknown>)[k]),
      );
   }
   return sent === current;
}

/**
 * The fields of a PATCH body that would change the package's `latest`
 * version, described now as `current`. Ignored: the fields the PATCH applies,
 * read-only fields, and a field left unset (see isUnset). `name` must be the
 * package's, and
 * `location` the one `latest` was published from. Any other field must echo
 * `latest`'s value (see echoes), or the spec's default for a field `latest`
 * leaves unset.
 */
export function changedFields(
   body: Record<string, unknown>,
   current: Record<string, unknown>,
   packageName: string,
   publishedFrom: string | null,
): string[] {
   return Object.entries(body)
      .filter(([key, value]) => {
         if (APPLIED_TO_A_VERSIONED_PACKAGE.has(key)) return false;
         if (READ_ONLY_PACKAGE_FIELDS.has(key)) return false;
         if (isUnset(value)) return false;
         if (key === "name") return value !== packageName;
         if (key === "location") return value !== publishedFrom;
         if (echoes(value, current[key])) return false;
         return !(
            isUnset(current[key]) && value === PACKAGE_FIELD_DEFAULTS[key]
         );
      })
      .map(([key]) => key);
}
