// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import { normalizeModelPath } from "../constants";

type ApiPackage = components["schemas"]["Package"];

/**
 * The fields a PATCH writes into a package's `publisher.json` that a reload
 * does not re-apply. A reload fetches a package installed from a `location`
 * again and replaces its files, keeping only its location and manifest
 * binding, so a change to any of these would be discarded.
 */
export const LOST_ON_A_RELOAD = [
   "description",
   "explores",
   "queryableSources",
   "scope",
   "queryMetadata",
   "materialization",
] as const;

export type LostOnAReload = (typeof LOST_ON_A_RELOAD)[number];

/** What a PATCH leaves each field a reload would lose set to. */
export interface PatchedSettings {
   description: ApiPackage["description"];
   /** The body's listing, normalized as the worker normalizes on-disk explores. */
   explores: ApiPackage["explores"];
   queryableSources: ApiPackage["queryableSources"];
   scope: ApiPackage["scope"];
   queryMetadata: ApiPackage["queryMetadata"] | null;
   materialization: ApiPackage["materialization"];
}

/**
 * What a PATCH `body` sets each field a reload would lose to, on a package
 * whose metadata is `existing` now: the one resolution both the PATCH that
 * applies it and the refusal that judges it read.
 *
 * - `description`: a string, `""` included, replaces it; null is omitted.
 * - `explores`: a list, `[]` included, replaces it, normalized; null is omitted.
 * - `queryableSources`, `scope`, `materialization`: a value replaces it; null
 *   is omitted, so a client serializing unset fields as null resets nothing.
 * - `queryMetadata`: read from either wire home, the canonical field first;
 *   null is omitted, and the clear is an empty bag. Written to both homes, so
 *   the serve path and the build path cannot disagree.
 */
export function resolvePatchedSettings(
   body: ApiPackage,
   existing: ApiPackage,
): PatchedSettings {
   const explores =
      body.explores != null
         ? body.explores.map(normalizeModelPath)
         : existing.explores;
   const queryMetadata =
      body.queryMetadata ??
      body.materialization?.queryMetadata ??
      existing.queryMetadata ??
      existing.materialization?.queryMetadata ??
      null;
   const materializationBase =
      body.materialization != null
         ? body.materialization
         : existing.materialization;
   const materialization =
      materializationBase || queryMetadata !== null
         ? { ...(materializationBase ?? {}), queryMetadata }
         : materializationBase;
   return {
      description:
         body.description != null ? body.description : existing.description,
      explores,
      queryableSources:
         body.queryableSources != null
            ? body.queryableSources
            : existing.queryableSources,
      scope: body.scope != null ? body.scope : existing.scope,
      queryMetadata,
      materialization,
   };
}

/** What a field reads as when nothing sets it: the spec's defaults. */
const DEFAULTS: Partial<Record<LostOnAReload, unknown>> = {
   queryableSources: "declared",
   scope: "package",
};

/** Nothing in it: null, absent, `""`, an empty list, or an object of nothing. */
function isBlank(value: unknown): boolean {
   if (value === undefined || value === null || value === "") return true;
   if (Array.isArray(value)) return value.length === 0;
   if (typeof value === "object") {
      return Object.values(value as object).every(isBlank);
   }
   return false;
}

/**
 * Whether two settings say the same thing: equal values, where an empty one
 * (see isBlank) equals any other empty one at every depth. Clearing a field
 * that is already empty changes nothing; clearing one that is set does.
 */
function sameSetting(a: unknown, b: unknown): boolean {
   if (isBlank(a) || isBlank(b)) return isBlank(a) && isBlank(b);
   if (Array.isArray(a) || Array.isArray(b)) {
      return (
         Array.isArray(a) &&
         Array.isArray(b) &&
         a.length === b.length &&
         a.every((item, i) => sameSetting(item, b[i]))
      );
   }
   if (typeof a === "object" && typeof b === "object") {
      const ra = a as Record<string, unknown>;
      const rb = b as Record<string, unknown>;
      const keys = new Set([...Object.keys(ra), ...Object.keys(rb)]);
      return [...keys].every((k) => sameSetting(ra[k], rb[k]));
   }
   return a === b;
}

/**
 * The fields a reload would lose that a PATCH `body` changes on a package
 * whose metadata is `existing`: those whose effective value after the PATCH
 * (see resolvePatchedSettings) differs from what it is now. A body that only
 * echoes the package back changes nothing, a clear of a field that is set is
 * a change, and a field the PATCH treats as omitted is not.
 */
export function settingsAReloadWouldLose(
   body: ApiPackage,
   existing: ApiPackage,
): LostOnAReload[] {
   const after = resolvePatchedSettings(body, existing);
   const now = resolvePatchedSettings({} as ApiPackage, existing);
   return LOST_ON_A_RELOAD.filter(
      (key) =>
         !sameSetting(after[key] ?? DEFAULTS[key], now[key] ?? DEFAULTS[key]),
   );
}
