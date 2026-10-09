// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A load that finished after its version was evicted. Its result was
 * released rather than cached; the caller resolves the version again, which
 * now answers what the eviction meant (archived, deleted).
 */
export class VersionEvictedDuringLoadError extends Error {
   constructor(packageName: string, versionId: string) {
      super(
         `Version ${versionId} of package ${packageName} was unloaded while it was loading.`,
      );
      this.name = "VersionEvictedDuringLoadError";
   }
}

export interface VersionCacheHooks<T> {
   /** Load one version. Runs at most once at a time per version. */
   load(packageName: string, versionId: string): Promise<T>;
   /** Release a loaded version that leaves the cache (close its connections). */
   release(packageName: string, versionId: string, loaded: T): void;
}

/**
 * The loaded versions of an environment's packages, keyed by
 * `(package, version)`.
 *
 * A version's files never change, so a loaded version is served as it is,
 * with no lock: there is nothing to reload. Loads are single-flight per
 * version, so concurrent first reads of one version share one load while
 * other versions load in parallel. A version stays loaded until it is
 * archived or its package is deleted ({@link evict}); being superseded as
 * `latest` does not unload it.
 */
export class VersionCache<T> {
   private readonly loaded = new Map<string, T>();
   private readonly loading = new Map<string, Promise<T>>();
   /** Bumped by every evict, so a load in flight can tell it was overtaken. */
   private readonly generations = new Map<string, number>();

   constructor(private readonly hooks: VersionCacheHooks<T>) {}

   /** The loaded version, without loading it. */
   peek(packageName: string, versionId: string): T | undefined {
      return this.loaded.get(key(packageName, versionId));
   }

   isLoaded(packageName: string, versionId: string): boolean {
      return this.loaded.has(key(packageName, versionId));
   }

   /** The loaded version, loading it first if it is not. */
   async get(packageName: string, versionId: string): Promise<T> {
      const k = key(packageName, versionId);
      const ready = this.loaded.get(k);
      if (ready !== undefined) return ready;
      const inFlight = this.loading.get(k);
      if (inFlight) return inFlight;

      const generation = this.generations.get(k) ?? 0;
      const load = (async () => {
         try {
            const value = await this.hooks.load(packageName, versionId);
            if ((this.generations.get(k) ?? 0) !== generation) {
               this.hooks.release(packageName, versionId, value);
               throw new VersionEvictedDuringLoadError(packageName, versionId);
            }
            this.loaded.set(k, value);
            return value;
         } finally {
            this.loading.delete(k);
         }
      })();
      this.loading.set(k, load);
      return load;
   }

   /**
    * Take a version out of the cache and release it. A load in flight for it
    * releases its result instead of caching it.
    */
   evict(packageName: string, versionId: string): void {
      const k = key(packageName, versionId);
      this.generations.set(k, (this.generations.get(k) ?? 0) + 1);
      const value = this.loaded.get(k);
      if (value === undefined) return;
      this.loaded.delete(k);
      this.hooks.release(packageName, versionId, value);
   }

   /** Evict every version of a package, loaded or loading. */
   evictPackage(packageName: string): void {
      const prefix = `${packageName}@`;
      const keys = new Set([...this.loaded.keys(), ...this.loading.keys()]);
      for (const k of keys) {
         if (k.startsWith(prefix))
            this.evict(packageName, k.slice(prefix.length));
      }
   }

   /** The versions loaded now, as `[packageName, versionId, loaded]`. */
   entries(): [string, string, T][] {
      return [...this.loaded].map(([k, value]) => {
         const at = k.indexOf("@");
         return [k.slice(0, at), k.slice(at + 1), value];
      });
   }
}

/** Package names cannot contain `@`, so the first one splits the key. */
function key(packageName: string, versionId: string): string {
   return `${packageName}@${versionId}`;
}
