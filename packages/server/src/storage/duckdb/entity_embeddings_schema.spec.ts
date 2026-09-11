import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DuckDBConnection } from "./DuckDBConnection";
import { createEntityEmbeddingsTable, initializeSchema } from "./schema";

/**
 * The schema pass runs on every boot, so both halves matter: it has to
 * replace a cache still keyed without `facet`, and it must not touch one
 * already faceted — otherwise every restart would silently discard every
 * cached vector and re-embed every package against a paid endpoint.
 */
const PRE_FACET_DDL = `
   CREATE TABLE entity_embeddings (
     environment_name VARCHAR NOT NULL,
     package_name VARCHAR NOT NULL,
     entity_kind VARCHAR NOT NULL,
     entity_source VARCHAR NOT NULL,
     entity_name VARCHAR NOT NULL,
     model_path VARCHAR NOT NULL,
     content_hash VARCHAR NOT NULL,
     embedding_model VARCHAR NOT NULL,
     dims INTEGER NOT NULL,
     embedding FLOAT[] NOT NULL,
     updated_at TIMESTAMP NOT NULL,
     PRIMARY KEY (environment_name, package_name, entity_kind, entity_source, entity_name)
   )`;

let tempDir: string;
let db: DuckDBConnection;

beforeEach(async () => {
   tempDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "entity-embeddings-schema-"),
   );
   db = new DuckDBConnection(path.join(tempDir, "test.db"));
   await db.initialize();
});

afterEach(async () => {
   await db.close();
   fs.rmSync(tempDir, { recursive: true, force: true });
});

async function facetColumns(): Promise<string[]> {
   const columns = await db.all<{ name: string }>(
      "PRAGMA table_info('entity_embeddings')",
   );
   return columns.map((c) => c.name);
}

async function rowCount(): Promise<number> {
   const rows = await db.all<{ n: number }>(
      "SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings",
   );
   return rows[0].n;
}

async function insertPreFacetRow(): Promise<void> {
   await db.run(
      `INSERT INTO entity_embeddings VALUES
        ('env', 'pkg', 'dimension', 'src', 'state', 'm.malloy', 'hash',
         'stub-model', 2, CAST('[1.0, 0.0]' AS FLOAT[]), ?)`,
      [new Date().toISOString()],
   );
}

describe("entity_embeddings facet re-keying", () => {
   it("replaces a pre-facet cache, and the faceted key then holds", async () => {
      await db.run(PRE_FACET_DDL);
      await insertPreFacetRow();
      expect(await rowCount()).toBe(1);

      await createEntityEmbeddingsTable(db);

      // Dropped, not migrated: the table is purely a vector cache, so the
      // whole cost of losing it is re-embedding, which the next getContext
      // call does on its own.
      expect(await facetColumns()).toContain("facet");
      expect(await rowCount()).toBe(0);

      // The new key admits two facets of one entity, which the old key,
      // being the reason for the drop, could not hold.
      const now = new Date().toISOString();
      for (const facet of ["name", "doc:0"]) {
         await db.run(
            `INSERT INTO entity_embeddings VALUES
              ('env', 'pkg', 'dimension', 'src', 'state', ?, 'm.malloy',
               'hash', 'stub-model', 2, CAST('[1.0, 0.0]' AS FLOAT[]), ?)`,
            [facet, now],
         );
      }
      expect(await rowCount()).toBe(2);
   });

   it("leaves an already-faceted cache and its vectors alone", async () => {
      // The expensive regression: re-dropping on every boot would re-embed
      // every package against a paid endpoint, once per restart.
      await createEntityEmbeddingsTable(db);
      await db.run(
         `INSERT INTO entity_embeddings VALUES
           ('env', 'pkg', 'dimension', 'src', 'state', 'name', 'm.malloy',
            'hash', 'stub-model', 2, CAST('[1.0, 0.0]' AS FLOAT[]), ?)`,
         [new Date().toISOString()],
      );

      await createEntityEmbeddingsTable(db);

      expect(await rowCount()).toBe(1);
   });

   it("creates the faceted table on a fresh database", async () => {
      await initializeSchema(db);
      expect(await facetColumns()).toContain("facet");
      expect(await rowCount()).toBe(0);
   });
});
