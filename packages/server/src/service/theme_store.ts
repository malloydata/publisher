import { Mutex } from "async-mutex";
import { getInstanceTheme, Theme } from "../config";
import { logger } from "../logger";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { StorageManager } from "../storage/StorageManager";

const THEME_ROW_ID = "default";

/**
 * Persistence for the instance-wide theme edited via the in-app Theme
 * Editor. Backed by a single row in the `themes` table (DuckDB).
 *
 * Boot semantics: on first read after process start, if the table is
 * empty, the row is seeded from `publisher.config.json`'s top-level
 * `theme` block (if any). Subsequent writes go to SQLite; the JSON file
 * remains the boot seed only. This mirrors how environments + packages
 * are handled — the JSON config is the seed; runtime mutations live in
 * the DB and survive across restarts.
 *
 * Single in-process consumer; a Mutex serializes set / reset against
 * concurrent reads to avoid the cache + DB diverging.
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

   /**
    * Idempotent. Loads the persisted theme into memory. If the table is
    * empty, seeds from `publisher.config.json`'s instance theme (if
    * present) and persists the seed so subsequent reads + the editor see
    * the same baseline.
    */
   async initialize(): Promise<void> {
      await this.mutex.runExclusive(async () => {
         if (this.cacheLoaded) return;
         const row = await this.db.get<{ payload: string }>(
            "SELECT payload FROM themes WHERE id = ?",
            [THEME_ROW_ID],
         );
         if (row?.payload) {
            try {
               this.cached = JSON.parse(row.payload) as Theme;
            } catch (error) {
               logger.warn(
                  "ThemeStore: stored theme JSON is unparseable; treating as empty",
                  { error },
               );
               this.cached = undefined;
            }
         } else {
            const seed = getInstanceTheme(this.serverRoot);
            if (seed) {
               this.cached = seed;
               await this.persistLocked(seed);
            }
         }
         this.cacheLoaded = true;
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

   /**
    * Drops the persisted row so the next read falls back to the boot
    * seed (or to no theme at all if the config doesn't define one).
    */
   async reset(): Promise<Theme | undefined> {
      return this.mutex.runExclusive(async () => {
         await this.db.run("DELETE FROM themes WHERE id = ?", [THEME_ROW_ID]);
         this.cached = undefined;
         // Re-run the seed-from-file path so the cache reflects whatever
         // the boot-seed currently is (or stays undefined if none).
         this.cacheLoaded = false;
         await this.initializeLocked();
         return this.cached;
      });
   }

   private async initializeLocked(): Promise<void> {
      if (this.cacheLoaded) return;
      const row = await this.db.get<{ payload: string }>(
         "SELECT payload FROM themes WHERE id = ?",
         [THEME_ROW_ID],
      );
      if (row?.payload) {
         try {
            this.cached = JSON.parse(row.payload) as Theme;
         } catch (error) {
            logger.warn(
               "ThemeStore: stored theme JSON is unparseable; treating as empty",
               { error },
            );
            this.cached = undefined;
         }
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
