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
 *   pass and the inline fields), captured when an operation that built or
 *   changed the copy settles. Kept by copy, so a copy that is
 *   retired is simply dropped. Only while PUBLISHER_PACKAGE_INLINE_FIELDS is
 *   on: the slim entry has no metadata.
 * - Each versioned package's registry rows and `latest`, read once when the
 *   environment's versions are bound and again after every write to them.
 *
 * The cheap half, what a reconcile reads (`status`, the manifest binding),
 * is read live on each poll from what the environment already holds.
 */
export class StatusSnapshot {
   private readonly metadata = new WeakMap<Package, ApiPackage>();
   private readonly versionRecords = new Map<string, StatusVersionRecords>();
   /** Orders version reads: each takes a ticket when it starts. */
   private tickets = 0;
   /** The ticket of the newest read that landed, per package. */
   private readonly landed = new Map<string, number>();
   /** Packages whose latest read failed, so their rows may be out of date. */
   private readonly dirty = new Set<string>();
   /** Polls that found a copy with no captured metadata (built on the poll). */
   private misses = 0;

   constructor(private readonly environmentName: string) {}

   /**
    * An operation on these copies settled: capture each one's metadata, since
    * the operation may have built it or changed it in place. Never throws.
    */
   settle(copies: Iterable<Package | undefined>): void {
      for (const pkg of new Set(copies)) {
         if (pkg !== undefined) this.tryCapture(pkg);
      }
   }

   /**
    * The copy's captured metadata. A copy no settled operation captured yet
    * (one a poll reaches while the operation that committed it is still
    * running) is built here, once, and counted; the operation's settle
    * captures it again when it ends.
    */
   metadataOf(pkg: Package): ApiPackage {
      const captured = this.metadata.get(pkg);
      if (captured !== undefined) return captured;
      this.misses += 1;
      logger.debug("Status snapshot built a package's metadata on a poll", {
         environmentName: this.environmentName,
         packageName: pkg.getPackageName(),
      });
      const metadata = pkg.getPackageMetadata();
      this.metadata.set(pkg, metadata);
      return metadata;
   }

   /** Visible for tests: polls that had to build a copy's metadata. */
   missesForTesting(): number {
      return this.misses;
   }

   /** Take a ticket before a read of version rows starts. */
   ticket(): number {
      return ++this.tickets;
   }

   /**
    * Every package's version rows, read with `ticket`. A package a newer read
    * already landed for keeps those rows; every other package, listed or not,
    * takes what this read found.
    */
   replaceVersionRecords(
      rows: ReadonlyArray<Version & { latest: string | null }>,
      ticket: number,
   ): void {
      const read = new Map<string, StatusVersionRecords>();
      for (const row of rows) {
         const records = read.get(row.packageName) ?? {
            latest: row.latest,
            versions: [],
         };
         const { latest: _latest, ...version } = row;
         records.versions.push(version);
         read.set(row.packageName, records);
      }
      for (const packageName of new Set([
         ...this.versionRecords.keys(),
         ...read.keys(),
         ...this.dirty,
      ])) {
         this.land(packageName, read.get(packageName), ticket);
      }
   }

   /**
    * Re-read one package's version rows with `read`. Reads of one package land
    * in the order they started, so a slow read never puts back rows a later
    * write replaced. A failed read keeps the rows held, and marks the package
    * dirty until a later read of it lands. Returns whether this read landed.
    */
   async refreshVersionRecords(
      packageName: string,
      read: () => Promise<StatusVersionRecords>,
   ): Promise<boolean> {
      const ticket = this.ticket();
      let records: StatusVersionRecords;
      try {
         records = await read();
      } catch (error) {
         if ((this.landed.get(packageName) ?? 0) < ticket) {
            this.dirty.add(packageName);
         }
         logger.warn("Could not refresh a package's versions for /status", {
            environmentName: this.environmentName,
            packageName,
            error: error instanceof Error ? error.message : String(error),
         });
         return false;
      }
      return this.land(packageName, records, ticket);
   }

   /** Packages whose rows a failed read left possibly out of date. */
   dirtyPackages(): string[] {
      return [...this.dirty];
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

   private land(
      packageName: string,
      records: StatusVersionRecords | undefined,
      ticket: number,
   ): boolean {
      if ((this.landed.get(packageName) ?? 0) > ticket) return false;
      this.landed.set(packageName, ticket);
      this.dirty.delete(packageName);
      if (records === undefined || records.versions.length === 0) {
         this.versionRecords.delete(packageName);
      } else {
         this.versionRecords.set(packageName, records);
      }
      return true;
   }

   private tryCapture(pkg: Package): void {
      try {
         this.metadata.set(pkg, pkg.getPackageMetadata());
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
