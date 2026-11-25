// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The names a request uses to find what it reads: an environment, a package
 * in it, a version of that package, and a model file in it. Each is a
 * branded string, made only by its `parse*` function, which runs the check
 * the server has always run on that identifier (path_safety.ts) and throws
 * the same error with the same message. A core lookup that takes one can
 * rely on the check having run; it does not take a bare string.
 *
 * The sink-side `assertSafe*` calls stay where they are (see path_safety.ts):
 * they are the barrier CodeQL recognises, and a cast cannot get past them.
 */

import {
   assertSafePackageName,
   assertSafeRelativeModelPath,
} from "../path_safety";
import {
   LATEST_VERSION_ALIAS,
   requestedVersionId,
} from "./versions/version_service";

declare const brand: unique symbol;
type Brand<T, B extends string> = T & { readonly [brand]: B };

/** An environment's name: one safe path segment. */
export type EnvironmentName = Brand<string, "EnvironmentName">;
/** A package's name within its environment: one safe path segment. */
export type PackageName = Brand<string, "PackageName">;
/** A model file's path relative to its package root. */
export type ModelPath = Brand<string, "ModelPath">;

/**
 * An environment name, or BadRequestError. Environment names have always
 * been held to the package-name rule, and are refused with its message.
 */
export function parseEnvironmentName(raw: unknown): EnvironmentName {
   assertSafePackageName(raw);
   return raw as EnvironmentName;
}

/** A package name, or BadRequestError ("Invalid package name: …"). */
export function parsePackageName(raw: unknown): PackageName {
   assertSafePackageName(raw);
   return raw as PackageName;
}

/** A package-relative model path, or BadRequestError ("Invalid model path"). */
export function parseModelPath(raw: unknown): ModelPath {
   assertSafeRelativeModelPath(raw);
   return raw as ModelPath;
}

/**
 * A package-relative model path, or undefined for one no package could serve.
 * For a lookup that answers "no such model" rather than 400, as the query
 * path always has: a package's models are keyed by paths that pass the check
 * (dotfiles and dot directories are never loaded), so a path that fails it
 * names nothing.
 */
export function tryParseModelPath(raw: unknown): ModelPath | undefined {
   try {
      return parseModelPath(raw);
   } catch {
      return undefined;
   }
}

/**
 * Which version of a package a request reads: its `latest`, or one named
 * published version (a semantic version). Parsing never throws, because a
 * server without package versioning ignores `versionId` altogether: a
 * malformed value is carried, and refused with VERSION_ID_INVALID (400) only
 * where versions are consulted, exactly as before (see {@link requested}).
 *
 * A class, not a branded object: a request's query string can carry an
 * object of any shape, never an instance.
 */
export class VersionRef {
   /** The package's `latest`: what an absent, empty or `latest` versionId names. */
   static readonly LATEST = new VersionRef(undefined, false, undefined);

   private constructor(
      private readonly versionId: string | undefined,
      private readonly malformed: boolean,
      private readonly raw: unknown,
   ) {}

   /** A request's `versionId`, as it arrived (body, query string or tool argument). */
   static parse(raw: unknown): VersionRef {
      if (raw instanceof VersionRef) return raw;
      let versionId: string | undefined;
      try {
         versionId = requestedVersionId(raw);
      } catch {
         return new VersionRef(undefined, true, raw);
      }
      return versionId === undefined
         ? VersionRef.LATEST
         : new VersionRef(versionId, false, raw);
   }

   /** A version the server itself names (a registry row); throws when it is not a semantic version. */
   static of(versionId: string): VersionRef {
      const ref = VersionRef.parse(versionId);
      ref.requested();
      return ref;
   }

   /** True for the package's `latest` (a malformed ref is not). */
   get isLatest(): boolean {
      return !this.malformed && this.versionId === undefined;
   }

   /**
    * The named version, or undefined for `latest`. Throws PackageVersionError
    * VERSION_ID_INVALID for a malformed ref, worded as it always was.
    */
   requested(): string | undefined {
      if (this.malformed) return requestedVersionId(this.raw);
      return this.versionId;
   }

   toString(): string {
      if (this.malformed) return String(this.raw);
      return this.versionId ?? LATEST_VERSION_ALIAS;
   }
}

/** One package (one version of it) in one environment. */
export interface PackageRef {
   readonly environment: EnvironmentName;
   readonly package: PackageName;
   readonly version: VersionRef;
}

/**
 * A package reference from a request's raw names, environment first. For an
 * edge that refuses a malformed name before any lookup.
 */
export function parsePackageRef(raw: {
   environmentName: unknown;
   packageName: unknown;
   versionId?: unknown;
}): PackageRef {
   return {
      environment: parseEnvironmentName(raw.environmentName),
      package: parsePackageName(raw.packageName),
      version: VersionRef.parse(raw.versionId),
   };
}
