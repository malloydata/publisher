// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { hashPackageTree } from "./package_content_hash";

describe("hashPackageTree", () => {
   let roots: string[];

   beforeEach(() => {
      roots = [];
   });

   afterEach(() => {
      for (const root of roots)
         fs.rmSync(root, { recursive: true, force: true });
   });

   function tree(files: Record<string, string>): string {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "content-hash-"));
      roots.push(root);
      for (const [relative, text] of Object.entries(files)) {
         const target = path.join(root, relative);
         fs.mkdirSync(path.dirname(target), { recursive: true });
         fs.writeFileSync(target, text);
      }
      return root;
   }

   const PACKAGE = {
      "publisher.json": '{"name":"sales","version":"1.0.0"}',
      "sales.malloy": "source: s is duckdb.table('s.parquet')",
      "dashboards/overview.malloy": "## artifact { kind=dashboard }",
   };

   it("is the same for the same content written separately", async () => {
      const a = tree(PACKAGE);
      const b = tree(Object.fromEntries(Object.entries(PACKAGE).reverse()));
      expect(await hashPackageTree(a)).toBe(await hashPackageTree(b));
   });

   it("ignores modification times", async () => {
      const a = tree(PACKAGE);
      const b = tree(PACKAGE);
      const past = new Date("2020-01-01T00:00:00Z");
      fs.utimesSync(path.join(b, "sales.malloy"), past, past);
      expect(await hashPackageTree(a)).toBe(await hashPackageTree(b));
   });

   it("changes when one byte of one file changes", async () => {
      const a = tree(PACKAGE);
      const b = tree({
         ...PACKAGE,
         "sales.malloy": PACKAGE["sales.malloy"] + " ",
      });
      expect(await hashPackageTree(a)).not.toBe(await hashPackageTree(b));
   });

   it("changes when a file moves, even with every byte the same", async () => {
      const a = tree({ "a/x.malloy": "same" });
      const b = tree({ "b/x.malloy": "same" });
      expect(await hashPackageTree(a)).not.toBe(await hashPackageTree(b));
   });

   it("changes when bytes move between files", async () => {
      const a = tree({ "x.malloy": "ab", "y.malloy": "c" });
      const b = tree({ "x.malloy": "a", "y.malloy": "bc" });
      expect(await hashPackageTree(a)).not.toBe(await hashPackageTree(b));
   });

   it("changes when a file is added", async () => {
      const a = tree(PACKAGE);
      const b = tree({ ...PACKAGE, "notes.md": "" });
      expect(await hashPackageTree(a)).not.toBe(await hashPackageTree(b));
   });

   it("skips .git, which is repository state rather than package content", async () => {
      const a = tree(PACKAGE);
      const b = tree({ ...PACKAGE, ".git/HEAD": "ref: refs/heads/main" });
      expect(await hashPackageTree(a)).toBe(await hashPackageTree(b));
   });

   it("hashes a symbolic link as its target and does not follow it", async () => {
      const outside = tree({ "secret.txt": "outside content" });
      const a = tree(PACKAGE);
      const b = tree(PACKAGE);
      fs.symlinkSync(path.join(outside, "secret.txt"), path.join(a, "link"));
      fs.symlinkSync(path.join(outside, "secret.txt"), path.join(b, "link"));
      const before = await hashPackageTree(a);
      fs.writeFileSync(path.join(outside, "secret.txt"), "changed");
      expect(await hashPackageTree(a)).toBe(before);
      expect(await hashPackageTree(b)).toBe(before);
   });
});
