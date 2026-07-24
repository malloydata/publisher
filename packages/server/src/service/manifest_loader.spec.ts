/// <reference types="bun-types" />

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import os from "os";
import path from "path";
import { pathToFileURL } from "url";
import { fetchManifestEntries } from "./manifest_loader";

describe("fetchManifestEntries", () => {
   let dir: string;

   beforeEach(async () => {
      dir = await fs.mkdtemp(path.join(os.tmpdir(), "manifest-loader-"));
   });

   afterEach(async () => {
      await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
   });

   async function writeManifest(contents: unknown): Promise<string> {
      const file = path.join(dir, "manifest.json");
      await fs.writeFile(file, JSON.stringify(contents), "utf8");
      return file;
   }

   it("maps physicalTableName to the Malloy runtime tableName (path-C entries)", async () => {
      const file = await writeManifest({
         builtAt: new Date().toISOString(),
         strict: false,
         entries: {
            b1: { sourceEntityId: "b1", physicalTableName: "schema.orders_mz" },
            b2: {
               sourceEntityId: "b2",
               physicalTableName: "schema.daily_mz",
               connectionName: "bq",
            },
         },
      });

      const { tableNameManifest, storageEntries } =
         await fetchManifestEntries(file);

      expect(tableNameManifest).toEqual({
         b1: { tableName: "schema.orders_mz" },
         // connectionName is carried through so the bind step can quote the
         // table path for that connection's dialect (Package.quoteBoundTableNames).
         b2: { tableName: "schema.daily_mz", connectionName: "bq" },
      });
      expect(storageEntries).toEqual({});
   });

   it("routes storage= entries (with storageConnectionName) to storageEntries, not the tableName manifest", async () => {
      const schema = [
         { name: "d", type: "DATE" },
         { name: "n", type: "BIGINT" },
      ];
      const file = await writeManifest({
         entries: {
            // path-C: in-warehouse substitution
            wh: {
               sourceEntityId: "wh",
               physicalTableName: "orders_v1",
               connectionName: "bq",
            },
            // storage tier: cross-connection serve binding (full entry retained)
            lake: {
               sourceEntityId: "lake",
               sourceName: "daily_orders",
               physicalTableName: "daily_orders",
               connectionName: "bq",
               storageConnectionName: "lake",
               schema,
            },
         },
      });

      const { tableNameManifest, storageEntries } =
         await fetchManifestEntries(file);

      // The storage entry must NOT enter the same-connection tableName manifest.
      expect(tableNameManifest).toEqual({
         wh: { tableName: "orders_v1", connectionName: "bq" },
      });
      // …and is preserved as a full entry (schema + sourceName + destination).
      expect(storageEntries).toEqual({
         lake: {
            sourceEntityId: "lake",
            sourceName: "daily_orders",
            physicalTableName: "daily_orders",
            connectionName: "bq",
            storageConnectionName: "lake",
            schema,
         },
      });
   });

   it("carries the host-supplied freshness fields verbatim (path-C)", async () => {
      const dataAsOf = "2026-07-07T00:00:00.000Z";
      const file = await writeManifest({
         entries: {
            gated: {
               sourceEntityId: "gated",
               physicalTableName: "schema.gated_mz",
               dataAsOf,
               freshnessWindowSeconds: 3600,
               freshnessFallback: "stale_ok",
            },
            ungated: {
               sourceEntityId: "ungated",
               physicalTableName: "schema.ungated_mz",
            },
         },
      });

      const { tableNameManifest } = await fetchManifestEntries(file);

      // Filter-free: freshness fields are retained for the serve-path gate; a
      // window-less entry is left un-gated.
      expect(tableNameManifest).toEqual({
         gated: {
            tableName: "schema.gated_mz",
            dataAsOf,
            freshnessWindowSeconds: 3600,
            freshnessFallback: "stale_ok",
         },
         ungated: { tableName: "schema.ungated_mz" },
      });
   });

   it("reads via a file:// URI", async () => {
      const file = await writeManifest({
         entries: { b1: { sourceEntityId: "b1", physicalTableName: "t1" } },
      });

      const { tableNameManifest } = await fetchManifestEntries(
         pathToFileURL(file).href,
      );

      expect(tableNameManifest).toEqual({ b1: { tableName: "t1" } });
   });

   it("skips entries without a physicalTableName", async () => {
      const file = await writeManifest({
         entries: {
            good: { sourceEntityId: "good", physicalTableName: "t1" },
            bad: { sourceEntityId: "bad" },
         },
      });

      const { tableNameManifest } = await fetchManifestEntries(file);

      expect(tableNameManifest).toEqual({ good: { tableName: "t1" } });
   });

   it("returns empty maps when there are no entries", async () => {
      const file = await writeManifest({ entries: {} });
      expect(await fetchManifestEntries(file)).toEqual({
         tableNameManifest: {},
         storageEntries: {},
      });

      const file2 = await writeManifest({});
      expect(await fetchManifestEntries(file2)).toEqual({
         tableNameManifest: {},
         storageEntries: {},
      });
   });

   it("throws on malformed JSON", async () => {
      const file = path.join(dir, "bad.json");
      await fs.writeFile(file, "{ not json", "utf8");
      await expect(fetchManifestEntries(file)).rejects.toThrow(
         /Failed to parse build manifest/,
      );
   });

   it("throws when the file does not exist", async () => {
      await expect(
         fetchManifestEntries(path.join(dir, "missing.json")),
      ).rejects.toThrow();
   });

   it("rejects a malformed gs:// URI without a key", async () => {
      await expect(fetchManifestEntries("gs://bucket-only")).rejects.toThrow(
         /Malformed manifest URI/,
      );
   });
});
