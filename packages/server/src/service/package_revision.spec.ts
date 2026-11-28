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
   MANIFEST_DECLARATION_MAX_BYTES,
   mintServedRevision,
   shasOf,
   UNREADABLE_DECLARATION,
   UNREADABLE_SHA,
} from "./package_revision";

const sha = (text: string) => createHash("sha256").update(text).digest("hex");
const shaMap = (files: Record<string, string>) =>
   new Map(Object.entries(files).map(([file, text]) => [file, sha(text)]));

describe("computeSourceContentSha", () => {
   it("moves when a hashed file's sha changes", () => {
      expect(
         computeSourceContentSha(shaMap({ "a.malloy": "source: x is t" })),
      ).not.toBe(
         computeSourceContentSha(shaMap({ "a.malloy": "source: x is u" })),
      );
   });

   it("moves when a path is added to the set", () => {
      const one = computeSourceContentSha(shaMap({ "a.malloy": "one" }));
      const two = computeSourceContentSha(
         shaMap({ "a.malloy": "one", "b.malloy": "two" }),
      );
      expect(two).not.toBe(one);
   });

   it("is independent of the order paths are supplied in", () => {
      expect(
         computeSourceContentSha(
            shaMap({ "a.malloy": "one", "b.malloy": "two" }),
         ),
      ).toBe(
         computeSourceContentSha(
            shaMap({ "b.malloy": "two", "a.malloy": "one" }),
         ),
      );
   });

   it("distinguishes a rename from unchanged bytes", () => {
      expect(
         computeSourceContentSha(shaMap({ "a.malloy": "same bytes" })),
      ).not.toBe(computeSourceContentSha(shaMap({ "b.malloy": "same bytes" })));
   });

   it("distinguishes a file the load could not read from an empty one", () => {
      // Without a distinct marker a package that had started failing to read a
      // model would look as healthy as one with an empty model.
      const unreadable = computeSourceContentSha(
         new Map([["gone.malloy", UNREADABLE_SHA]]),
      );
      expect(unreadable).not.toBe(
         computeSourceContentSha(shaMap({ "gone.malloy": "" })),
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
         computeSourceContentSha(shaMap({ "not/on/disk.malloy": "x" }));
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

   const build = (
      declared: { skillSets?: unknown; workflows?: unknown } = {},
   ) => {
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
         servingIdentityOutcome(dir, declared, ["m.malloy"]),
      );
      return pkg;
   };

   const DEFAULT_SET = { skillSets: { default: { path: "skill_sets/main" } } };
   const BRIEF = { instructions: "workflows/brief.md", description: "Brief" };
   const writeBrief = (text: string) => {
      fs.mkdirSync(path.join(dir, "workflows"), { recursive: true });
      fs.writeFileSync(path.join(dir, "workflows/brief.md"), text);
   };
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

   it("hashes a package that declares neither skillSets nor workflows as its models alone", () => {
      expect(build().getSourceContentSha()).toBe(
         computeSourceContentSha(shaMap({ "m.malloy": "source: a is x" })),
      );
   });

   it("moves the sha when a skill of a skill set changes, so a skill edit has a receipt", () => {
      // eval-improve reads the sha to prove its edit reached the served copy.
      // If skill files were outside the hash, a skill-only edit would report
      // "nothing changed" while in fact being served: a false negative on the
      // one check that catches the publisher_data/ copy trap.
      writeSkill("house", "Prefer net revenue.");
      const before = build(DEFAULT_SET).getSourceContentSha();
      writeSkill("house", "Prefer gross revenue.");
      expect(build(DEFAULT_SET).getSourceContentSha()).not.toBe(before);
   });

   it("moves the sha for a frontmatter-only edit to a skill, which the served text carries as written", () => {
      writeSkill("house", "Prefer net revenue.");
      const before = build(DEFAULT_SET).getSourceContentSha();
      fs.writeFileSync(
         path.join(dir, "skill_sets", "main", "house", "SKILL.md"),
         "---\nname: house\ndescription: d\nallowed-tools: Bash\n---\n\nPrefer net revenue.\n",
      );
      expect(build(DEFAULT_SET).getSourceContentSha()).not.toBe(before);
   });

   it("moves the sha when a skill is added to a skill set", () => {
      writeSkill("house", "Prefer net revenue.");
      const before = build(DEFAULT_SET).getSourceContentSha();
      writeSkill("new", "body");
      expect(build(DEFAULT_SET).getSourceContentSha()).not.toBe(before);
   });

   it("moves the sha when only the skillSets map changes, the files it names staying as they were", () => {
      writeSkill("house", "Prefer net revenue.");
      const before = build(DEFAULT_SET).getSourceContentSha();
      const described = {
         skillSets: {
            default: { path: "skill_sets/main", description: "Now described" },
         },
      };
      expect(build(described).getSourceContentSha()).not.toBe(before);
      const overridden = {
         skillSets: {
            default: { path: "skill_sets/main", overrides: ["house"] },
         },
      };
      expect(build(overridden).getSourceContentSha()).not.toBe(before);
   });

   it("moves the sha when the workflows a package declares change, and when only a workflow's instructions do", () => {
      writeBrief("Be careful.");
      const before = build({
         workflows: { brief: BRIEF },
      }).getSourceContentSha();
      const described = {
         workflows: { brief: { ...BRIEF, description: "Now described" } },
      };
      expect(build(described).getSourceContentSha()).not.toBe(before);
      writeBrief("Be very careful.");
      expect(
         build({ workflows: { brief: BRIEF } }).getSourceContentSha(),
      ).not.toBe(before);
   });

   it("leaves the sha alone for a root skills/ directory no set is declared at", () => {
      writeSkill("house", "Prefer net revenue.");
      const before = build(DEFAULT_SET).getSourceContentSha();
      const unread = path.join(dir, "skills", "unread");
      fs.mkdirSync(unread, { recursive: true });
      fs.writeFileSync(
         path.join(unread, "SKILL.md"),
         "---\nname: unread\ndescription: d\n---\n\nbody\n",
      );
      expect(build(DEFAULT_SET).getSourceContentSha()).toBe(before);
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
   it("passes an ordinary declaration through with its sha, and replaces one too deep or too large", () => {
      const ok = { workflows: { a: { description: "d" } } };
      expect(boundedDeclaration(ok)).toEqual({
         value: ok,
         sha: canonicalSha(ok),
      });
      let deep: unknown = [];
      for (let i = 0; i < 50_000; i++) deep = [deep];
      const tooDeep = boundedDeclaration({ workflows: deep });
      expect(tooDeep.value).toBe(UNREADABLE_DECLARATION);
      expect(tooDeep.sha).toMatch(/^[0-9a-f]{64}$/);
      expect(
         boundedDeclaration({ skillSets: "x".repeat(2 * 1024 * 1024) }).value,
      ).toBe(UNREADABLE_DECLARATION);
   });

   it("is no declaration, with no sha, when neither key is present", () => {
      expect(boundedDeclaration({})).toEqual({ value: undefined });
      expect(
         boundedDeclaration({ skillSets: undefined, workflows: undefined }),
      ).toEqual({ value: undefined });
   });

   it("bounds the two keys as one value, so each under the cap can together be over it", () => {
      const half = "x".repeat(MANIFEST_DECLARATION_MAX_BYTES * 0.6);
      expect(boundedDeclaration({ skillSets: { a: half } }).value).not.toBe(
         UNREADABLE_DECLARATION,
      );
      expect(boundedDeclaration({ workflows: { b: half } }).value).not.toBe(
         UNREADABLE_DECLARATION,
      );
      expect(
         boundedDeclaration({ skillSets: { a: half }, workflows: { b: half } })
            .value,
      ).toBe(UNREADABLE_DECLARATION);
   });

   it("gives a declaration the sha of its canonical form, whatever order its keys were written in", () => {
      expect(boundedDeclaration({ workflows: { b: 1, a: 2 } }).sha).toBe(
         boundedDeclaration({ workflows: { a: 2, b: 1 } }).sha,
      );
   });

   it("moves the sha when either key does", () => {
      const base = boundedDeclaration({
         skillSets: { a: 1 },
         workflows: { b: 1 },
      });
      expect(
         boundedDeclaration({ skillSets: { a: 2 }, workflows: { b: 1 } }).sha,
      ).not.toBe(base.sha);
      expect(
         boundedDeclaration({ skillSets: { a: 1 }, workflows: { b: 2 } }).sha,
      ).not.toBe(base.sha);
   });
});

describe("shasOf", () => {
   it("reads each path's sha from the map, in the order given", () => {
      const shas = new Map([
         ["a.malloy", "1"],
         ["b.malloy", "2"],
      ]);
      expect([...shasOf(["b.malloy", "a.malloy"], shas)]).toEqual([
         ["b.malloy", "2"],
         ["a.malloy", "1"],
      ]);
   });

   it("throws for a path with no sha, which the content hash would otherwise skip", () => {
      expect(() => shasOf(["a.malloy"], new Map())).toThrow(
         "no sha was read for 'a.malloy', which the content hash covers",
      );
   });
});
