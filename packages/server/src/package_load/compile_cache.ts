// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   CacheManager,
   type CachedModel,
   type URLReader,
} from "@malloydata/malloy";
import { logger } from "../logger";

/**
 * The most compiled files one package keeps. Past it every entry is dropped.
 * Each model, each pre-aggregation companion and each new path a dry-run
 * compiles takes an entry, so a package of about 250 models with companions
 * reaches it on every load (logged once).
 */
export const MAX_CACHED_MODELS_PER_PACKAGE = 512;

/**
 * The compiled ModelDefs of one package, shared by every Runtime that
 * compiles it, so a file whose text (and whose imports' text) is unchanged is
 * not translated again. Malloy checks an entry against file contents only;
 * {@link beginLoad} empties the cache when the connections or the package's
 * data files change. Not covered: a schema change on the warehouse side (only a
 * reload with `refreshSchema` sees it, as with the worker's SchemaCache) and
 * data a model reads from outside the package directory. Restricted-mode compiles bypass it in Malloy.
 */
export class PackageCompileCache {
   private models = new Map<string, CachedModel>();
   private sharedManager = this.newManager();
   private stamp: string | undefined;
   private overflowLogged = false;

   constructor(
      /** Names the package in the overflow log. */
      private readonly label: string,
      private readonly maxModels = MAX_CACHED_MODELS_PER_PACKAGE,
   ) {}

   /**
    * Start a load. `stamp` names what the ModelDefs depend on besides file
    * text (the connections, the package's data files); a new one empties the cache.
    */
   beginLoad(stamp: string): void {
      if (this.stamp !== stamp) {
         this.clear();
         this.stamp = stamp;
      }
   }

   /** The shared manager. */
   get manager(): CacheManager {
      return this.sharedManager;
   }

   /**
    * A manager for one compile of `url`, which says whether `url` itself was
    * served from the cache. With `translate`, it never is: a hit carries no
    * problems, so a caller that reports the file's problems translates it.
    */
   forModel(url: URL, options: { translate: boolean }): ModelCompileCache {
      return new ModelCompileCache(
         this.sharedManager,
         url.toString(),
         options.translate,
      );
   }

   clear(): void {
      this.models = new Map();
      this.sharedManager = this.newManager();
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
               if (!this.overflowLogged) {
                  this.overflowLogged = true;
                  logger.warn(
                     "Package compile cache overflowed; every entry was dropped",
                     { package: this.label, maxModels: this.maxModels },
                  );
               }
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
      return cached;
   }

   override setCachedModelDef(
      url: string,
      cachedModel: CachedModel,
   ): Promise<boolean> {
      return this.shared.setCachedModelDef(url, cachedModel);
   }
}
