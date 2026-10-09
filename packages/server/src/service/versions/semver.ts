// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Package versions are semantic versions (SemVer 2.0): MAJOR.MINOR.PATCH, an
 * optional pre-release (`-rc1`, `-alpha.2`) and optional build metadata
 * (`+20231120.deadbeef`).
 *
 * The pattern is the spec's `VersionIdPattern`, byte for byte, so the route
 * validation and the publish gate accept exactly the same versions.
 */
export const SEMVER_PATTERN =
   /^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$/;

export function isSemver(value: string): boolean {
   return SEMVER_PATTERN.test(value);
}

interface ParsedVersion {
   core: [bigint, bigint, bigint];
   preRelease: string[];
}

function parse(version: string): ParsedVersion {
   if (!isSemver(version)) {
      throw new Error(`Not a semantic version: ${JSON.stringify(version)}`);
   }
   const withoutBuild = version.split("+", 1)[0];
   const dash = withoutBuild.indexOf("-");
   const core = dash === -1 ? withoutBuild : withoutBuild.slice(0, dash);
   const preRelease = dash === -1 ? "" : withoutBuild.slice(dash + 1);
   // BigInt, because a version component is an unbounded integer and a
   // Number loses precision past 2^53, which would order two versions wrongly.
   const [major, minor, patch] = core.split(".").map((part) => BigInt(part));
   return {
      core: [major, minor, patch],
      preRelease: preRelease === "" ? [] : preRelease.split("."),
   };
}

const NUMERIC = /^[0-9]+$/;

function compareIdentifiers(a: string, b: string): number {
   const aNumeric = NUMERIC.test(a);
   const bNumeric = NUMERIC.test(b);
   if (aNumeric && bNumeric) {
      const diff = BigInt(a) - BigInt(b);
      return diff === 0n ? 0 : diff < 0n ? -1 : 1;
   }
   // A numeric identifier always has lower precedence than an alphanumeric one.
   if (aNumeric) return -1;
   if (bNumeric) return 1;
   return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * SemVer 2.0 precedence: negative when `a` is lower than `b`, positive when
 * higher, 0 when they share precedence. Build metadata does not count, so
 * `1.0.0+a` and `1.0.0+b` compare equal even though they are different
 * versions; callers that need a total order break the tie themselves.
 */
export function compareSemver(a: string, b: string): number {
   const left = parse(a);
   const right = parse(b);
   for (let i = 0; i < 3; i++) {
      if (left.core[i] !== right.core[i]) {
         return left.core[i] < right.core[i] ? -1 : 1;
      }
   }
   // A version with a pre-release has lower precedence than the same version
   // without one: 1.0.0-rc1 < 1.0.0.
   const leftIsRelease = left.preRelease.length === 0;
   const rightIsRelease = right.preRelease.length === 0;
   if (leftIsRelease || rightIsRelease) {
      if (leftIsRelease && rightIsRelease) return 0;
      return leftIsRelease ? 1 : -1;
   }
   const shared = Math.min(left.preRelease.length, right.preRelease.length);
   for (let i = 0; i < shared; i++) {
      const order = compareIdentifiers(left.preRelease[i], right.preRelease[i]);
      if (order !== 0) return order;
   }
   // Every shared identifier is equal: the longer set has higher precedence.
   return Math.sign(left.preRelease.length - right.preRelease.length);
}

/**
 * The directory a version's tree lives in, under its package directory.
 *
 * The version itself, except build metadata's `+`, which is legal in a version
 * but not in a path segment the publisher accepts. `+` becomes `_`, which no
 * semantic version can contain, so two versions never share a directory name.
 * They can still differ only by case (`1.0.0-RC1` and `1.0.0-rc1`), which a
 * case-insensitive filesystem would treat as one directory; the publish path
 * refuses that pair rather than relying on the filesystem.
 */
export function versionDirName(version: string): string {
   if (!isSemver(version)) {
      throw new Error(`Not a semantic version: ${JSON.stringify(version)}`);
   }
   return version.replace(/\+/g, "_");
}
