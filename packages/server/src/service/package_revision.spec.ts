// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { Model } from "./model";
import { Package } from "./package";
import {
   computeSourceContentSha,
   mintServedRevision,
} from "./package_revision";

describe("computeSourceContentSha", () => {
   let dir: string;

   beforeEach(() => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-revision-"));
   });

   afterEach(() => {
      fs.rmSync(dir, { recursive: true, force: true });
   });

   const write = (relativePath: string, content: string) => {
      const absolute = path.join(dir, relativePath);
      fs.mkdirSync(path.dirname(absolute), { recursive: true });
      fs.writeFileSync(absolute, content);
   };

   it("moves when a hashed file's content changes", () => {
      write("a.malloy", "source: x is duckdb.table('t')");
      const before = computeSourceContentSha(dir, ["a.malloy"]);
      write("a.malloy", "source: x is duckdb.table('u')");
      expect(computeSourceContentSha(dir, ["a.malloy"])).not.toBe(before);
   });

   it("does not move when a file outside the set changes", () => {
      write("a.malloy", "source: x is duckdb.table('t')");
      write("notes.md", "first");
      const before = computeSourceContentSha(dir, ["a.malloy"]);
      write("notes.md", "second");
      expect(computeSourceContentSha(dir, ["a.malloy"])).toBe(before);
   });

   it("moves when a path is added to the set", () => {
      write("a.malloy", "source: x is duckdb.table('t')");
      write("b.malloy", "source: y is duckdb.table('u')");
      const one = computeSourceContentSha(dir, ["a.malloy"]);
      const two = computeSourceContentSha(dir, ["a.malloy", "b.malloy"]);
      expect(two).not.toBe(one);
   });

   it("is independent of the order paths are supplied in", () => {
      write("a.malloy", "one");
      write("b.malloy", "two");
      expect(computeSourceContentSha(dir, ["a.malloy", "b.malloy"])).toBe(
         computeSourceContentSha(dir, ["b.malloy", "a.malloy"]),
      );
   });

   it("distinguishes a rename from unchanged bytes", () => {
      write("a.malloy", "same bytes");
      const asA = computeSourceContentSha(dir, ["a.malloy"]);
      write("b.malloy", "same bytes");
      expect(computeSourceContentSha(dir, ["b.malloy"])).not.toBe(asA);
   });

   it("distinguishes a missing file from an empty one", () => {
      // Both read as zero bytes of content. Without a distinct marker for the
      // unreadable case they would hash identically, and a package that had
      // started failing to read a model would look healthy.
      const missing = computeSourceContentSha(dir, ["gone.malloy"]);
      write("gone.malloy", "");
      expect(computeSourceContentSha(dir, ["gone.malloy"])).not.toBe(missing);
   });

   it("hashes no paths to a stable digest", () => {
      expect(computeSourceContentSha(dir, [])).toBe(
         computeSourceContentSha(dir, []),
      );
   });
});

describe("mintServedRevision", () => {
   it("returns a different value each call, so it identifies a load not content", () => {
      expect(mintServedRevision()).not.toBe(mintServedRevision());
   });
});

describe("Package serving identity", () => {
   let dir: string;

   beforeEach(() => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-identity-"));
      fs.writeFileSync(path.join(dir, "m.malloy"), "source: a is x");
   });

   afterEach(() => {
      fs.rmSync(dir, { recursive: true, force: true });
   });

   const build = () =>
      new Package(
         "env",
         "pkg",
         dir,
         { name: "pkg" },
         [],
         new Map([
            [
               "m.malloy",
               {
                  getPath: () => "m.malloy",
                  setDiscoveryCuration: () => {},
                  setQueryBoundary: () => {},
                  hasEmptyDiscoverySurface: () => false,
                  getDeclaredQueryMetadata: () => [],
                  getDeclaredSourceQueryMetadata: () => [],
                  preaggregateViolations: () => [],
               } as unknown as Model,
            ],
         ]),
      );

   it("reaches the API payload, which is where every caller reads it", () => {
      const metadata = build().getPackageMetadata();
      expect(metadata.sourceContentSha).toMatch(/^[0-9a-f]{64}$/);
      expect(metadata.servedRevision).toBeString();
   });

   it("gives the same sha to two loads of identical bytes, and a new revision", () => {
      const first = build();
      const second = build();
      expect(second.getSourceContentSha()).toBe(first.getSourceContentSha());
      // The pair is the point: same content, different load. A caller asking
      // "did my edit land" must read the sha, because the revision always moves.
      expect(second.getServedRevision()).not.toBe(first.getServedRevision());
   });

   it("moves the sha when a served model file changes on disk", () => {
      const before = build().getSourceContentSha();
      fs.writeFileSync(path.join(dir, "m.malloy"), "source: a is y");
      expect(build().getSourceContentSha()).not.toBe(before);
   });

   it("moves the sha when a package skill changes, so a skill edit has a receipt", () => {
      // eval-improve reads the sha to prove its edit reached the served copy.
      // If skill files were outside the hash, a skill-only edit would report
      // "nothing changed" while in fact being served: a false negative on the
      // one check that catches the publisher_data/ copy trap.
      const skillDir = path.join(dir, "skills", "house");
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(
         path.join(skillDir, "SKILL.md"),
         "---\nname: house\ndescription: d\n---\n\nPrefer net revenue.\n",
      );
      const before = build().getSourceContentSha();
      fs.writeFileSync(
         path.join(skillDir, "SKILL.md"),
         "---\nname: house\ndescription: d\n---\n\nPrefer gross revenue.\n",
      );
      expect(build().getSourceContentSha()).not.toBe(before);
   });

   it("moves the sha when a package skill is added", () => {
      const before = build().getSourceContentSha();
      const skillDir = path.join(dir, "skills", "new");
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(
         path.join(skillDir, "SKILL.md"),
         "---\nname: new\ndescription: d\n---\n\nbody\n",
      );
      expect(build().getSourceContentSha()).not.toBe(before);
   });

   it("leaves the sha alone when a non-model file in the package changes", () => {
      // The publisher_data/ copy trap makes this the load-bearing case: the sha
      // must track what the compiler read, so that an unchanged value is real
      // evidence the edit did not reach the served copy.
      const before = build().getSourceContentSha();
      fs.writeFileSync(path.join(dir, "notes.txt"), "irrelevant");
      expect(build().getSourceContentSha()).toBe(before);
   });
});
