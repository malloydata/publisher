/// <reference types="bun-types" />

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import os from "os";
import path from "path";
import { pathToFileURL } from "url";
import {
   evaluateManifestFreshness,
   fetchManifestEntries,
} from "./manifest_loader";

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

   const hoursAgo = (h: number): string =>
      new Date(Date.now() - h * 3_600_000).toISOString();

   it("binds a fresh table within its freshness window", async () => {
      const file = await writeManifest({
         entries: {
            b1: {
               buildId: "b1",
               physicalTableName: "t_fresh",
               builtAt: hoursAgo(1),
               freshnessWindowSeconds: 24 * 3600,
               freshnessFallback: "live",
            },
         },
      });

      expect(await fetchManifestEntries(file)).toEqual({
         b1: { tableName: "t_fresh" },
      });
   });

   it("drops a stale table when the fallback is live (serve live)", async () => {
      const file = await writeManifest({
         entries: {
            b1: {
               buildId: "b1",
               physicalTableName: "t_stale",
               builtAt: hoursAgo(48),
               freshnessWindowSeconds: 24 * 3600,
               freshnessFallback: "live",
            },
         },
      });

      expect(await fetchManifestEntries(file)).toEqual({});
   });

   it("keeps a stale table when the fallback is stale_ok", async () => {
      const file = await writeManifest({
         entries: {
            b1: {
               buildId: "b1",
               physicalTableName: "t_stale_ok",
               builtAt: hoursAgo(48),
               freshnessWindowSeconds: 24 * 3600,
               freshnessFallback: "stale_ok",
            },
         },
      });

      expect(await fetchManifestEntries(file)).toEqual({
         b1: { tableName: "t_stale_ok" },
      });
   });

   it("drops a stale table when the fallback is fail", async () => {
      const file = await writeManifest({
         entries: {
            b1: {
               buildId: "b1",
               physicalTableName: "t_fail",
               builtAt: hoursAgo(48),
               freshnessWindowSeconds: 24 * 3600,
               freshnessFallback: "fail",
            },
         },
      });

      // A stale `fail` source drops its binding (serves live) rather than
      // serving stale data; per-query erroring needs Malloy-runtime support.
      expect(await fetchManifestEntries(file)).toEqual({});
   });

   it("binds when no freshness window is configured (today's behavior)", async () => {
      const file = await writeManifest({
         entries: {
            b1: {
               buildId: "b1",
               physicalTableName: "t_ungated",
               builtAt: hoursAgo(1000),
            },
         },
      });

      expect(await fetchManifestEntries(file)).toEqual({
         b1: { tableName: "t_ungated" },
      });
   });
});

describe("evaluateManifestFreshness", () => {
   const now = new Date("2026-06-27T12:00:00Z");
   const builtAt = (secondsAgo: number): string =>
      new Date(now.getTime() - secondsAgo * 1000).toISOString();

   it("serves the table when no window is set", () => {
      expect(evaluateManifestFreshness({ builtAt: builtAt(10_000) }, now)).toBe(
         "serve_table",
      );
   });

   it("serves the table when within the window", () => {
      expect(
         evaluateManifestFreshness(
            { builtAt: builtAt(100), freshnessWindowSeconds: 3600 },
            now,
         ),
      ).toBe("serve_table");
   });

   it("serves live when stale and fallback is live (default)", () => {
      expect(
         evaluateManifestFreshness(
            { builtAt: builtAt(7200), freshnessWindowSeconds: 3600 },
            now,
         ),
      ).toBe("serve_live");
      expect(
         evaluateManifestFreshness(
            {
               builtAt: builtAt(7200),
               freshnessWindowSeconds: 3600,
               freshnessFallback: "live",
            },
            now,
         ),
      ).toBe("serve_live");
   });

   it("serves the table when stale and fallback is stale_ok", () => {
      expect(
         evaluateManifestFreshness(
            {
               builtAt: builtAt(7200),
               freshnessWindowSeconds: 3600,
               freshnessFallback: "stale_ok",
            },
            now,
         ),
      ).toBe("serve_table");
   });

   it("fails when stale and fallback is fail", () => {
      expect(
         evaluateManifestFreshness(
            {
               builtAt: builtAt(7200),
               freshnessWindowSeconds: 3600,
               freshnessFallback: "fail",
            },
            now,
         ),
      ).toBe("fail");
   });

   it("fails open (serve_table) for a missing or unparseable builtAt", () => {
      expect(
         evaluateManifestFreshness({ freshnessWindowSeconds: 3600 }, now),
      ).toBe("serve_table");
      expect(
         evaluateManifestFreshness(
            { builtAt: "not-a-date", freshnessWindowSeconds: 3600 },
            now,
         ),
      ).toBe("serve_table");
   });
});
