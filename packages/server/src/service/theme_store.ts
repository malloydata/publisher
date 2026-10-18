// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Mutex } from "async-mutex";
import { getInstanceTheme, sanitizeTheme, Theme } from "../config";
import { logger } from "../logger";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { StorageManager } from "../storage/StorageManager";

const THEME_ROW_ID = "default";

/**
 * Persistence for the instance-wide theme, one row in the DuckDB `themes`
 * table. As with environments and packages, `publisher.config.json`'s `theme`
 * block only seeds an empty table; runtime edits live in the DB.
 *
 * Stored payloads pass through `sanitizeTheme` on read so an outdated shape
 * never reaches the editor. A Mutex keeps the cache and the DB from diverging.
 */
export class ThemeStore {
   private mutex = new Mutex();
   private cached: Theme | undefined;
   private cacheLoaded = false;

   constructor(
      private storageManager: StorageManager,
      private serverRoot: string,
   ) {}

   private get db(): DuckDBConnection {
      return this.storageManager.getDuckDbConnection();
   }

   /** Idempotent. Loads the persisted theme, seeding an empty table from config. */
   async initialize(): Promise<void> {
      await this.mutex.runExclusive(async () => {
         await this.loadLocked();
      });
   }

   async get(): Promise<Theme | undefined> {
      if (!this.cacheLoaded) {
         await this.initialize();
      }
      return this.cached;
   }

   async set(theme: Theme): Promise<Theme> {
      return this.mutex.runExclusive(async () => {
         await this.persistLocked(theme);
         this.cached = theme;
         this.cacheLoaded = true;
         return theme;
      });
   }

   /** Drops the persisted row so the theme falls back to the config seed, if any. */
   async reset(): Promise<Theme | undefined> {
      return this.mutex.runExclusive(async () => {
         await this.db.run("DELETE FROM themes WHERE id = ?", [THEME_ROW_ID]);
         this.cached = undefined;
         this.cacheLoaded = false;
         await this.loadLocked();
         return this.cached;
      });
   }

   /** Loads the cache once. Must be called with the mutex held. */
   private async loadLocked(): Promise<void> {
      if (this.cacheLoaded) return;
      const row = await this.db.get<{ payload: string }>(
         "SELECT payload FROM themes WHERE id = ?",
         [THEME_ROW_ID],
      );
      if (row?.payload) {
         let parsed: unknown;
         try {
            parsed = JSON.parse(row.payload);
         } catch (error) {
            logger.warn(
               "ThemeStore: stored theme JSON is unparseable; treating as empty",
               { error },
            );
            this.cacheLoaded = true;
            return;
         }
         const sanitized = sanitizeTheme(parsed, "ThemeStore.load");
         const dropped = listSanitizerDrops(parsed, sanitized);
         if (dropped.length > 0) {
            // Tells an operator why custom values vanished from the editor
            // after an upgrade.
            logger.info(
               "ThemeStore: dropped stale fields from stored theme during load",
               { dropped },
            );
         }
         this.cached = sanitized;
      } else {
         const seed = getInstanceTheme(this.serverRoot);
         if (seed) {
            this.cached = seed;
            await this.persistLocked(seed);
         }
      }
      this.cacheLoaded = true;
   }

   private async persistLocked(theme: Theme): Promise<void> {
      const now = new Date().toISOString();
      const payload = JSON.stringify(theme);
      await this.db.run(
         `INSERT INTO themes (id, payload, created_at, updated_at)
          VALUES (?, ?, ?, ?)
          ON CONFLICT (id) DO UPDATE SET
            payload = EXCLUDED.payload,
            updated_at = EXCLUDED.updated_at`,
         [THEME_ROW_ID, payload, now, now],
      );
   }
}

/**
 * Theme keys present on `raw` but dropped by `sanitizeTheme`, as dotted paths
 * one level deep (e.g. `palette.series`).
 */
function listSanitizerDrops(
   raw: unknown,
   sanitized: Theme | undefined,
): string[] {
   if (!raw || typeof raw !== "object" || Array.isArray(raw)) return [];
   const r = raw as Record<string, unknown>;
   const drops: string[] = [];

   const topKeys = ["defaultMode", "allowUserToggle"] as const;
   for (const k of topKeys) {
      if (r[k] !== undefined && sanitized?.[k] === undefined) drops.push(k);
   }

   if (
      r.palette &&
      typeof r.palette === "object" &&
      !Array.isArray(r.palette)
   ) {
      const rp = r.palette as Record<string, unknown>;
      const sp = (sanitized?.palette ?? {}) as Record<string, unknown>;
      for (const k of Object.keys(rp)) {
         if (rp[k] !== undefined && sp[k] === undefined)
            drops.push(`palette.${k}`);
      }
   } else if (
      r.palette !== undefined &&
      (!sanitized || sanitized.palette === undefined)
   ) {
      drops.push("palette");
   }

   if (r.font && typeof r.font === "object" && !Array.isArray(r.font)) {
      const rf = r.font as Record<string, unknown>;
      const sf = (sanitized?.font ?? {}) as Record<string, unknown>;
      for (const k of Object.keys(rf)) {
         if (rf[k] !== undefined && sf[k] === undefined)
            drops.push(`font.${k}`);
      }
   } else if (
      r.font !== undefined &&
      (!sanitized || sanitized.font === undefined)
   ) {
      drops.push("font");
   }

   return drops;
}
