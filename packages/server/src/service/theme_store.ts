import { Mutex } from "async-mutex";
import { getInstanceTheme, sanitizeTheme, Theme } from "../config";
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
 * `theme` block (if any). Subsequent writes go to DuckDB; the JSON file
 * remains the boot seed only. This mirrors how environments + packages
 * are handled. The JSON config is the seed; runtime mutations live in
 * the DB and survive across restarts.
 *
 * Every payload read out of the DB is run through `sanitizeTheme` so a
 * schema change can't put a stale shape in front of the editor (e.g.
 * old per-mode `series` arrays after the simplification revert). The
 * editor receives only fields that match the current type.
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

   /**
    * Drops the persisted row so the next read falls back to the boot
    * seed (or to no theme at all if the config doesn't define one).
    */
   async reset(): Promise<Theme | undefined> {
      return this.mutex.runExclusive(async () => {
         await this.db.run("DELETE FROM themes WHERE id = ?", [THEME_ROW_ID]);
         this.cached = undefined;
         this.cacheLoaded = false;
         await this.loadLocked();
         return this.cached;
      });
   }

   /**
    * Single source of truth for loading the cache. Must be called with
    * the mutex held. Idempotent: returns immediately if already loaded.
    */
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
         // Run through sanitizeTheme so stale schema shapes (e.g. an
         // old per-mode `series` object) get dropped before the editor
         // ever sees them. Operators can keep editing without hitting
         // Reset; fields that match the current shape survive.
         const sanitized = sanitizeTheme(parsed, "ThemeStore.load");
         const dropped = listSanitizerDrops(parsed, sanitized);
         if (dropped.length > 0) {
            // Surface what disappeared so operators reading the server
            // log can see why the editor doesn't show their old custom
            // values after an upgrade. Re-saving from the editor will
            // overwrite the on-disk payload with the current shape.
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
 * Return the list of top-level Theme keys present on `raw` but absent
 * from `sanitized` (i.e. dropped by `sanitizeTheme`). Reports dotted
 * paths down one level so the log says e.g. `palette.series` rather
 * than just `palette`. Returns an empty array when nothing was lost.
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
