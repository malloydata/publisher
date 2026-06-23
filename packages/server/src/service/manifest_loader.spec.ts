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

   it("maps physicalTableName to the Malloy runtime tableName", async () => {
      const file = await writeManifest({
         builtAt: new Date().toISOString(),
         strict: false,
         entries: {
            b1: { buildId: "b1", physicalTableName: "schema.orders_mz" },
            b2: {
               buildId: "b2",
               physicalTableName: "schema.daily_mz",
               connectionName: "bq",
            },
         },
      });

      const entries = await fetchManifestEntries(file);

      expect(entries).toEqual({
         b1: { tableName: "schema.orders_mz" },
         b2: { tableName: "schema.daily_mz" },
      });
   });

   it("reads via a file:// URI", async () => {
      const file = await writeManifest({
         entries: { b1: { buildId: "b1", physicalTableName: "t1" } },
      });

      const entries = await fetchManifestEntries(pathToFileURL(file).href);

      expect(entries).toEqual({ b1: { tableName: "t1" } });
   });

   it("skips entries without a physicalTableName", async () => {
      const file = await writeManifest({
         entries: {
            good: { buildId: "good", physicalTableName: "t1" },
            bad: { buildId: "bad" },
         },
      });

      const entries = await fetchManifestEntries(file);

      expect(entries).toEqual({ good: { tableName: "t1" } });
   });

   it("returns an empty map when there are no entries", async () => {
      const file = await writeManifest({ entries: {} });
      expect(await fetchManifestEntries(file)).toEqual({});

      const file2 = await writeManifest({});
      expect(await fetchManifestEntries(file2)).toEqual({});
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
