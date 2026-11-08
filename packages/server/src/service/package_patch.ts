// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import { normalizeModelPath } from "../constants";
import { InstalledFromLocationError } from "../errors";

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

/**
 * What a Package field reads as when nothing sets it. `scope`'s is the one
 * the spec declares as a `default`, so a generated client sends it for a
 * field it was never given; `queryableSources` reads as "declared" wherever
 * it is unset. Shared by both PATCH judgements: this file's and the versioned
 * route's (controller/versioned_patch.ts).
 */
export const PACKAGE_FIELD_DEFAULTS: Readonly<Record<string, unknown>> = {
   scope: "package",
   queryableSources: "declared",
};

/**
 * A top-level field left unset: null, absent, an empty string, or an empty
 * list. A client generated with defaults starts every optional list empty and
 * sends it so (Java's okhttp-gson does), so at the top level `[]` cannot be
 * told from "not given". An empty object is not unset: no generated client
 * sends one for a field it was never given.
 */
export function isUnset(value: unknown): boolean {
   return (
      value === undefined ||
      value === null ||
      value === "" ||
      (Array.isArray(value) && value.length === 0)
   );
}

/**
 * Nothing in it, below the top level: null, absent, an empty list, or an
 * object of nothing. An empty string is a value there.
 */
export function isEmpty(value: unknown): boolean {
   if (value === undefined || value === null) return true;
   if (Array.isArray(value)) return value.length === 0;
   if (typeof value === "object") {
      return Object.values(value as object).every(isEmpty);
   }
   return false;
}

/**
 * Whether two values say the same thing: equal, where an empty value (see
 * isEmpty) equals any other empty one at every depth.
 *
 * Symmetric, unlike the versioned route's `echoes`, and the difference is
 * deliberate: that route never applies a body, so it judges an object only on
 * the keys a client names (an older client leaves out keys it does not
 * know); this file's PATCH applies the body, so a key its object leaves out
 * is really dropped and has to count.
 */
function sameValue(a: unknown, b: unknown): boolean {
   if (isEmpty(a) || isEmpty(b)) return isEmpty(a) && isEmpty(b);
   if (Array.isArray(a) || Array.isArray(b)) {
      return (
         Array.isArray(a) &&
         Array.isArray(b) &&
         a.length === b.length &&
         a.every((item, i) => sameValue(item, b[i]))
      );
   }
   if (typeof a === "object" && typeof b === "object") {
      const ra = a as Record<string, unknown>;
      const rb = b as Record<string, unknown>;
      const keys = new Set([...Object.keys(ra), ...Object.keys(rb)]);
      return [...keys].every((k) => sameValue(ra[k], rb[k]));
   }
   return a === b;
}

/**
 * Whether `a` and `b`, two values of the top-level field `key`, read the
 * same: an unset value (see isUnset) reads as the field's default, so
 * clearing a field that is already empty changes nothing and clearing one
 * that is set does.
 */
function sameSetting(key: string, a: unknown, b: unknown): boolean {
   const asRead = (value: unknown) =>
      isUnset(value) ? PACKAGE_FIELD_DEFAULTS[key] : value;
   return sameValue(asRead(a), asRead(b));
}

/** What a package whose metadata is `metadata` has each setting as now. */
const settingsOf = (metadata: ApiPackage) =>
   resolvePatchedSettings({} as ApiPackage, metadata);

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
   const now = settingsOf(existing);
   return LOST_ON_A_RELOAD.filter(
      (key) => !sameSetting(key, after[key], now[key]),
   );
}

/** `body` without what sets `key`, so the package's own value stands. */
function withoutSetting(body: ApiPackage, key: LostOnAReload): ApiPackage {
   const rest: ApiPackage = { ...body };
   delete rest[key];
   // queryMetadata has a second wire home, inside the materialization block.
   if (key === "queryMetadata" && rest.materialization?.queryMetadata != null) {
      const { queryMetadata: _moved, ...materialization } =
         rest.materialization;
      rest.materialization = materialization;
   }
   return rest;
}

/** Where the package a PATCH reinstalls is coming from, and what it replaces. */
export interface Reinstall {
   /** The new location the PATCH names. */
   location: string;
   /** The metadata of the package it replaces, if one was resident. */
   previous?: ApiPackage;
}

/**
 * The PATCH `body` to apply to a package installed from a location, or a
 * refusal (409 PACKAGE_INSTALLED_FROM_LOCATION) when it changes a setting a
 * reload would discard. A reload fetches the package again from its location
 * and replaces its files, `publisher.json` included, keeping only the
 * location and the manifest binding (see reloadPackageFrom), so a setting
 * survives it only when the fetched tree already declares it. Interim, until
 * package storage keeps server-side changes apart from the fetched copy
 * (#1560).
 *
 * `target` is the metadata of the tree the body is applied to: the resident
 * package, or, on a `reinstall`, the tree just fetched from the new location.
 * Called under the package lock, against the metadata the apply sees.
 *
 * - Same tree: a package with no location takes the body as it is; any other
 *   is refused a change to a setting (see settingsAReloadWouldLose). A body
 *   that echoes the package back changes nothing.
 * - Reinstall replacing a package installed from another location: clients
 *   send whole objects, so a field the body sets to the value the replaced
 *   package has is an echo. It is dropped from the body, so the new tree's
 *   own value stands and a reload loses nothing. A field that differs from
 *   both the replaced package's value and the new tree's is refused.
 * - First install from a location (nothing resident, or a package with no
 *   location): every field is judged against the fetched tree. What the
 *   package had was its own, not a fetched tree's, so dropping it unasked
 *   would lose it.
 */
export function admitPatch(
   packageName: string,
   body: ApiPackage,
   target: ApiPackage,
   reinstall?: Reinstall,
): ApiPackage {
   const location = reinstall ? reinstall.location : target.location;
   if (!location) return body;
   let admitted = body;
   const previous = reinstall?.previous;
   if (previous?.location) {
      const was = settingsOf(previous);
      const sent = resolvePatchedSettings(body, previous);
      for (const key of settingsAReloadWouldLose(body, target)) {
         if (sameSetting(key, sent[key], was[key])) {
            admitted = withoutSetting(admitted, key);
         }
      }
   }
   const changed = settingsAReloadWouldLose(admitted, target);
   if (changed.length === 0) return admitted;
   const where = reinstall
      ? `is being installed from ${location}, whose publisher.json says otherwise`
      : `was installed from ${location}`;
   throw new InstalledFromLocationError(
      `Package ${packageName} ${where}, and a reload fetches it again from there, which would discard this request's change to ${changed.join(", ")}. Make the change in the package's source and reload it, or publish it as a version.`,
   );
}
