// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { createHash } from "crypto";
import fs from "fs";
import os from "os";
import path from "path";
import { servingIdentityOutcome } from "../test_helpers/serving_identity";
import { Model } from "./model";
import { Package } from "./package";
import {
   boundedDeclaration,
   canonicalSha,
   computeSourceContentSha,
   mintServedRevision,
   UNREADABLE_SHA,
} from "./package_revision";

const sha = (text: string) => createHash("sha256").update(text).digest("hex");
const shasOf = (files: Record<string, string>) =>
   new Map(Object.entries(files).map(([file, text]) => [file, sha(text)]));

describe("computeSourceContentSha", () => {
   it("moves when a hashed file's sha changes", () => {
      expect(
         computeSourceContentSha(shasOf({ "a.malloy": "source: x is t" })),
      ).not.toBe(
         computeSourceContentSha(shasOf({ "a.malloy": "source: x is u" })),
      );
   });

   it("moves when a path is added to the set", () => {
      const one = computeSourceContentSha(shasOf({ "a.malloy": "one" }));
      const two = computeSourceContentSha(
         shasOf({ "a.malloy": "one", "b.malloy": "two" }),
      );
      expect(two).not.toBe(one);
   });

   it("is independent of the order paths are supplied in", () => {
      expect(
         computeSourceContentSha(
            shasOf({ "a.malloy": "one", "b.malloy": "two" }),
         ),
      ).toBe(
         computeSourceContentSha(
            shasOf({ "b.malloy": "two", "a.malloy": "one" }),
         ),
      );
   });

   it("distinguishes a rename from unchanged bytes", () => {
      expect(
         computeSourceContentSha(shasOf({ "a.malloy": "same bytes" })),
      ).not.toBe(computeSourceContentSha(shasOf({ "b.malloy": "same bytes" })));
   });

   it("distinguishes a file the load could not read from an empty one", () => {
      // Without a distinct marker a package that had started failing to read a
      // model would look as healthy as one with an empty model.
      const unreadable = computeSourceContentSha(
         new Map([["gone.malloy", UNREADABLE_SHA]]),
      );
      expect(unreadable).not.toBe(
         computeSourceContentSha(shasOf({ "gone.malloy": "" })),
      );
   });

   it("moves with the extra text, and hashes no paths to a stable digest", () => {
      expect(computeSourceContentSha(new Map())).toBe(
         computeSourceContentSha(new Map()),
      );
      expect(computeSourceContentSha(new Map(), "declared")).not.toBe(
         computeSourceContentSha(new Map()),
      );
   });

   it("reads no file: it hashes the shas it is given, whether or not a path exists", () => {
      const reads = spyOn(fs, "readFileSync");
      try {
         computeSourceContentSha(shasOf({ "not/on/disk.malloy": "x" }));
         expect(reads).not.toHaveBeenCalled();
      } finally {
         reads.mockRestore();
      }
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

   const build = (workflows?: unknown) => {
      const pkg = new Package(
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
                  getGivens: () => [],
                  getCompilationError: () => undefined,
                  hasEmptyDiscoverySurface: () => false,
                  getDeclaredQueryMetadata: () => [],
                  getDeclaredSourceQueryMetadata: () => [],
                  preaggregateViolations: () => [],
                  declaredFilterSourceNames: () => [],
                  declaredDeprecatedPersistAnnotations: () => [],
                  persistedViewBuilds: () => [],
                  servedCopyProblems: () => [],
                  gateLoadWarnings: () => [],
                  storedColumnTypeMismatches: () => [],
                  getModelDef: () => undefined,
               } as unknown as Model,
            ],
         ]),
      );
      // What the load worker ships, adopted as a load adopts it.
      (
         pkg as unknown as {
            adoptOutcomeMetadata(outcome: unknown): void;
         }
      ).adoptOutcomeMetadata(
         servingIdentityOutcome(dir, workflows, ["m.malloy"]),
      );
      return pkg;
   };

   const DEFAULT_WORKFLOW = { default: { skillSets: ["skill_sets/main"] } };
   const writeSkill = (name: string, body: string) => {
      const skillDir = path.join(dir, "skill_sets", "main", name);
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(
         path.join(skillDir, "SKILL.md"),
         `---\nname: ${name}\ndescription: d\n---\n\n${body}\n`,
      );
   };

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

   it("moves the sha when a skill of a workflow's skill set changes, so a skill edit has a receipt", () => {
      // eval-improve reads the sha to prove its edit reached the served copy.
      // If skill files were outside the hash, a skill-only edit would report
      // "nothing changed" while in fact being served: a false negative on the
      // one check that catches the publisher_data/ copy trap.
      writeSkill("house", "Prefer net revenue.");
      const before = build(DEFAULT_WORKFLOW).getSourceContentSha();
      writeSkill("house", "Prefer gross revenue.");
      expect(build(DEFAULT_WORKFLOW).getSourceContentSha()).not.toBe(before);
   });

   it("moves the sha when a skill is added to a workflow's skill set", () => {
      writeSkill("house", "Prefer net revenue.");
      const before = build(DEFAULT_WORKFLOW).getSourceContentSha();
      writeSkill("new", "body");
      expect(build(DEFAULT_WORKFLOW).getSourceContentSha()).not.toBe(before);
   });

   it("moves the sha when the workflows a package declares change", () => {
      writeSkill("house", "Prefer net revenue.");
      const before = build(DEFAULT_WORKFLOW).getSourceContentSha();
      const described = {
         default: { ...DEFAULT_WORKFLOW.default, description: "Now described" },
      };
      expect(build(described).getSourceContentSha()).not.toBe(before);
   });

   it("leaves the sha alone for a root skills/ directory no workflow lists", () => {
      writeSkill("house", "Prefer net revenue.");
      const before = build(DEFAULT_WORKFLOW).getSourceContentSha();
      const unread = path.join(dir, "skills", "unread");
      fs.mkdirSync(unread, { recursive: true });
      fs.writeFileSync(
         path.join(unread, "SKILL.md"),
         "---\nname: unread\ndescription: d\n---\n\nbody\n",
      );
      expect(build(DEFAULT_WORKFLOW).getSourceContentSha()).toBe(before);
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

describe("boundedDeclaration", () => {
   it("passes an ordinary value through with its sha, and replaces one too deep or too large", () => {
      const ok = { a: { description: "d" } };
      expect(boundedDeclaration(ok)).toEqual({
         value: ok,
         sha: canonicalSha(ok),
      });
      expect(boundedDeclaration(undefined)).toEqual({ value: undefined });
      let deep: unknown = [];
      for (let i = 0; i < 50_000; i++) deep = [deep];
      const tooDeep = boundedDeclaration(deep);
      expect(typeof tooDeep.value).toBe("string");
      expect(tooDeep.sha).toMatch(/^[0-9a-f]{64}$/);
      expect(typeof boundedDeclaration("x".repeat(2 * 1024 * 1024)).value).toBe(
         "string",
      );
   });

   it("gives a declaration the sha of its canonical form, whatever order its keys were written in", () => {
      expect(boundedDeclaration({ b: 1, a: 2 }).sha).toBe(
         boundedDeclaration({ a: 2, b: 1 }).sha,
      );
   });
});
