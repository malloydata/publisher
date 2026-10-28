// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   CacheManager,
   type CachedModel,
   type InvalidationKey,
   type ModelDef,
   type URLReader,
} from "@malloydata/malloy";

/**
 * The most files one package keeps compiled. Past it every entry is dropped:
 * a package has about one entry per file, so only path churn (a package moved
 * between directories) gets there.
 */
export const MAX_CACHED_MODELS_PER_PACKAGE = 512;

/**
 * The compiled ModelDefs of one package, shared by every Runtime that
 * compiles it, so a file whose text (and whose imports' text) is unchanged is
 * not translated again. Malloy checks an entry against file contents only, so
 * the whole cache is emptied when the connections it compiled against change
 * (the `connectionsEpoch`). Restricted-mode compiles bypass it in Malloy.
 */
export class PackageCompileCache {
   private models = new Map<string, CachedModel>();
   private manager = this.newManager();
   private epoch: number | undefined;

   constructor(private readonly maxModels = MAX_CACHED_MODELS_PER_PACKAGE) {}

   /** The shared manager, emptied first if the connections changed since the last call. */
   managerFor(connectionsEpoch: number): CacheManager {
      if (this.epoch !== connectionsEpoch) {
         this.clear();
         this.epoch = connectionsEpoch;
      }
      return this.manager;
   }

   /**
    * A manager for one compile of `url`, which says whether `url` itself was
    * served from the cache. With `translate`, it never is: a hit carries no
    * problems, so a caller that reports the file's problems translates it.
    */
   forModel(
      connectionsEpoch: number,
      url: URL,
      options: { translate: boolean },
   ): ModelCompileCache {
      return new ModelCompileCache(
         this.managerFor(connectionsEpoch),
         url.toString(),
         options.translate,
      );
   }

   clear(): void {
      this.models = new Map();
      this.manager = this.newManager();
   }

   private newManager(): CacheManager {
      // Each manager writes only the map it was created with, so a write
      // from a compile that outlives clear() cannot land in the new one.
      const models = this.models;
      return new CacheManager({
         getModel: async (url) => models.get(url.toString()),
         setModel: async (url, cachedModel) => {
            const key = url.toString();
            if (models.size >= this.maxModels && !models.has(key)) {
               models.clear();
            }
            models.set(key, cachedModel);
            return true;
         },
      });
   }
}

/** The shared manager as one compile of `topLevelUrl` sees it (see PackageCompileCache.forModel). */
export class ModelCompileCache extends CacheManager {
   /** `topLevelUrl` was served from the cache. */
   hit = false;
   /** The ModelDef this compile produced for `topLevelUrl`, with the keys it was checked or stored under. */
   private topLevel?: CachedModel;

   constructor(
      private readonly shared: CacheManager,
      private readonly topLevelUrl: string,
      private readonly translateTopLevel: boolean,
   ) {
      super({
         getModel: async () => undefined,
         setModel: async () => false,
      });
   }

   override async getCachedModelDef(
      urlReader: URLReader,
      url: string,
   ): ReturnType<CacheManager["getCachedModelDef"]> {
      if (url !== this.topLevelUrl) {
         return this.shared.getCachedModelDef(urlReader, url);
      }
      if (this.translateTopLevel) return undefined;
      const cached = await this.shared.getCachedModelDef(urlReader, url);
      this.hit = cached !== undefined;
      this.topLevel = cached;
      return cached;
   }

   override setCachedModelDef(
      url: string,
      cachedModel: CachedModel,
   ): Promise<boolean> {
      if (url === this.topLevelUrl) this.topLevel = cachedModel;
      return this.shared.setCachedModelDef(url, cachedModel);
   }

   /**
    * The invalidation keys `modelDef` was compiled against, if this compile
    * produced it, so another isolate can seed its cache with the same pair.
    * Taken from this compile, not the shared entry: a concurrent compile
    * importing the file may have replaced that entry.
    */
   invalidationKeysFor(
      modelDef: ModelDef,
   ): Record<string, InvalidationKey> | undefined {
      return this.topLevel?.modelDef === modelDef
         ? this.topLevel.invalidationKeys
         : undefined;
   }
}
