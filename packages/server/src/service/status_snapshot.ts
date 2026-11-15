// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import { logger } from "../logger";
import type { Version } from "../storage/DatabaseInterface";
import type { Package } from "./package";

type ApiPackage = components["schemas"]["Package"];

/** One package's version rows, as `/status` lists them. */
export interface StatusVersionRecords {
   latest: string | null;
   /** In the registry's order: oldest publish first. */
   versions: Version[];
}

/**
 * What `GET /status` answers from, kept current as the environment changes so
 * that a poll builds nothing, reads no database and loads nothing.
 *
 * Two parts, each the expensive half of a status entry:
 *
 * - A package copy's metadata (`Package.getPackageMetadata()`, the warnings
 *   pass and the inline fields), captured when the copy is committed and again
 *   when an operation on its package settles. Kept by copy, so a copy that is
 *   retired is simply dropped. Only while PUBLISHER_PACKAGE_INLINE_FIELDS is
 *   on: the slim entry has no metadata.
 * - Each versioned package's registry rows and `latest`, read once when the
 *   environment's versions are bound and again after every write to them.
 *
 * The cheap half, what a reconcile reads (`status`, the manifest binding),
 * is read live on each poll from what the environment already holds.
 */
export class StatusSnapshot {
   /** Orders captures against operations; see {@link begin}. */
   private sequence = 0;
   private readonly metadata = new WeakMap<
      Package,
      { metadata: ApiPackage; capturedAt: number }
   >();
   private readonly versionRecords = new Map<string, StatusVersionRecords>();
   /** The newest refresh that landed, per package; an older one is dropped. */
   private readonly landed = new Map<string, number>();
   /** Polls that found a copy with no captured metadata (built on the poll). */
   private misses = 0;

   constructor(private readonly environmentName: string) {}

   /** Mark the start of an operation on a package; pass it to {@link settle}. */
   begin(): number {
      return ++this.sequence;
   }

   /** Capture `pkg`'s metadata now. */
   capture(pkg: Package): ApiPackage {
      const metadata = pkg.getPackageMetadata();
      this.metadata.set(pkg, { metadata, capturedAt: ++this.sequence });
      return metadata;
   }

   /** Capture `pkg`'s metadata unless it already has one. Never throws. */
   ensure(pkg: Package): void {
      if (!this.metadata.has(pkg)) this.tryCapture(pkg);
   }

   /**
    * An operation begun at `begun` settled: capture every copy whose metadata
    * predates it, since the operation may have changed it in place. A copy the
    * operation committed (and captured) is not built twice.
    */
   settle(copies: Iterable<Package | undefined>, begun: number): void {
      for (const pkg of new Set(copies)) {
         if (pkg === undefined) continue;
         const captured = this.metadata.get(pkg);
         if (captured === undefined || captured.capturedAt < begun) {
            this.tryCapture(pkg);
         }
      }
   }

   /**
    * The copy's captured metadata. A copy nothing captured (none should be
    * resident without one) is built here, once, and counted.
    */
   metadataOf(pkg: Package): ApiPackage {
      const captured = this.metadata.get(pkg);
      if (captured !== undefined) return captured.metadata;
      this.misses += 1;
      logger.debug("Status snapshot built a package's metadata on a poll", {
         environmentName: this.environmentName,
         packageName: pkg.getPackageName(),
      });
      return this.capture(pkg);
   }

   /** Visible for tests: polls that had to build a copy's metadata. */
   missesForTesting(): number {
      return this.misses;
   }

   /** Replace every package's version rows (when versions are bound). */
   replaceVersionRecords(
      rows: ReadonlyArray<Version & { latest: string | null }>,
   ): void {
      this.versionRecords.clear();
      for (const row of rows) {
         const records = this.versionRecords.get(row.packageName) ?? {
            latest: row.latest,
            versions: [],
         };
         const { latest: _latest, ...version } = row;
         records.versions.push(version);
         this.versionRecords.set(row.packageName, records);
      }
   }

   /**
    * Re-read one package's version rows with `read`. Refreshes of one package
    * land in the order they started, so a slow read never puts back rows a
    * later write replaced. A failed read keeps the rows held and is logged.
    */
   async refreshVersionRecords(
      packageName: string,
      read: () => Promise<StatusVersionRecords>,
   ): Promise<void> {
      const ticket = ++this.sequence;
      let records: StatusVersionRecords;
      try {
         records = await read();
      } catch (error) {
         logger.warn("Could not refresh a package's versions for /status", {
            environmentName: this.environmentName,
            packageName,
            error: error instanceof Error ? error.message : String(error),
         });
         return;
      }
      if ((this.landed.get(packageName) ?? 0) > ticket) return;
      this.landed.set(packageName, ticket);
      if (records.versions.length === 0) {
         this.versionRecords.delete(packageName);
      } else {
         this.versionRecords.set(packageName, records);
      }
   }

   /** Whether the package has any version row. */
   isVersioned(packageName: string): boolean {
      return this.versionRecords.has(packageName);
   }

   /**
    * Every version row with its package's `latest`, in the registry's order
    * (package name, then publish order).
    */
   versionRows(): Array<Version & { latest: string | null }> {
      return [...this.versionRecords.entries()]
         .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
         .flatMap(([, { latest, versions }]) =>
            versions.map((version) => ({ ...version, latest })),
         );
   }

   private tryCapture(pkg: Package): void {
      try {
         this.capture(pkg);
      } catch (error) {
         // Left to the poll, which builds it once; an observer must never
         // fail the operation it observes.
         this.metadata.delete(pkg);
         logger.warn("Could not capture a package's metadata for /status", {
            environmentName: this.environmentName,
            packageName: pkg.getPackageName(),
            error: error instanceof Error ? error.message : String(error),
         });
      }
   }
}
