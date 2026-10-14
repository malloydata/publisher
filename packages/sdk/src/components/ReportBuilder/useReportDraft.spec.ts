// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The draft key and dropping drafts from a host: a host that deleted, renamed
 * or replaced a report clears what the builder kept for it, without knowing
 * the server string the builder keyed it by.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { clearReportDraft, draftStore, reportDraftKey } from "./useReportDraft";

const PREFIX = "malloy-publisher.report-draft:";

/** A `Storage` over a map, so a spec can see every key. */
function memoryStorage(): Storage {
   const items = new Map<string, string>();
   return {
      get length() {
         return items.size;
      },
      key: (i) => [...items.keys()][i] ?? null,
      getItem: (key) => items.get(key) ?? null,
      setItem: (key, value) => void items.set(key, String(value)),
      removeItem: (key) => void items.delete(key),
      clear: () => items.clear(),
   };
}

const original = draftStore.storage;
let storage: Storage;
beforeEach(() => {
   storage = memoryStorage();
   draftStore.storage = () => storage;
});
afterEach(() => {
   draftStore.storage = original;
});

const keep = (key: string) => storage.setItem(PREFIX + key, "{}");
const kept = (): string[] =>
   Array.from({ length: storage.length }, (_, i) => storage.key(i)!)
      .filter((key) => key.startsWith(PREFIX))
      .map((key) => key.slice(PREFIX.length))
      .sort();

const SALES = {
   environmentName: "env",
   packageName: "pkg",
   path: "sales/q3.malloy",
};

describe("reportDraftKey", () => {
   it("names the server, report and version", () => {
      expect(reportDraftKey({ ...SALES, server: "https://a/api/v0" })).toBe(
         "https://a/api/v0|env/pkg@/sales/q3.malloy",
      );
      expect(
         reportDraftKey({
            ...SALES,
            server: "https://a/api/v0",
            versionId: "v2",
         }),
      ).toBe("https://a/api/v0|env/pkg@v2/sales/q3.malloy");
   });
});

describe("clearReportDraft", () => {
   beforeEach(() => {
      keep(reportDraftKey({ ...SALES, server: "https://a/api/v0" }));
      keep(reportDraftKey({ ...SALES, server: "https://b/api/v0" }));
      keep(
         reportDraftKey({
            ...SALES,
            server: "https://a/api/v0",
            versionId: "v2",
         }),
      );
      // Another report whose path ends the same way, and another package.
      keep(
         reportDraftKey({
            ...SALES,
            path: "q3.malloy",
            server: "https://a/api/v0",
         }),
      );
      keep(
         reportDraftKey({
            ...SALES,
            packageName: "other",
            server: "https://a/api/v0",
         }),
      );
      storage.setItem("unrelated", "1");
   });

   it("drops every draft of a report, on any server and version", () => {
      clearReportDraft(SALES);
      expect(kept().filter((key) => key.includes("|"))).toEqual([
         "https://a/api/v0|env/other@/sales/q3.malloy",
         "https://a/api/v0|env/pkg@/q3.malloy",
      ]);
      expect(storage.getItem("unrelated")).toBe("1");
   });

   it("narrows to one server or one version when told", () => {
      clearReportDraft({ ...SALES, server: "https://b/api/v0" });
      expect(
         storage.getItem(PREFIX + "https://b/api/v0|env/pkg@/sales/q3.malloy"),
      ).toBeNull();
      expect(
         storage.getItem(PREFIX + "https://a/api/v0|env/pkg@/sales/q3.malloy"),
      ).not.toBeNull();

      clearReportDraft({ ...SALES, versionId: "v2" });
      expect(
         storage.getItem(
            PREFIX + "https://a/api/v0|env/pkg@v2/sales/q3.malloy",
         ),
      ).toBeNull();
      expect(
         storage.getItem(PREFIX + "https://a/api/v0|env/pkg@/sales/q3.malloy"),
      ).not.toBeNull();
   });

   it("drops one draft by its key", () => {
      const key = reportDraftKey({ ...SALES, server: "https://a/api/v0" });
      clearReportDraft(key);
      expect(storage.getItem(PREFIX + key)).toBeNull();
      expect(
         storage.getItem(
            PREFIX + reportDraftKey({ ...SALES, server: "https://b/api/v0" }),
         ),
      ).not.toBeNull();
   });

   it("is exported for hosts", async () => {
      const sdk = await import("../index");
      expect(sdk.clearReportDraft).toBe(clearReportDraft);
      expect(sdk.reportDraftKey).toBe(reportDraftKey);
   });
});
