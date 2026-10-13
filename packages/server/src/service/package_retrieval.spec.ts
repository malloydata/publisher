// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { PackageManifestError } from "../errors";
import {
   alsoLikelyOn,
   DEFAULT_FCJ_SETTINGS,
   DEFAULT_PACKAGE_RETRIEVAL,
   engineOf,
   fcjSettingsOf,
   assertRequiredStagesAvailable,
   parsePackageRetrieval,
   readPackageRetrieval,
   refineSettingsOf,
   rerankSettingsOf,
   resolvePromptPath,
   sourceMatchSettingsOf,
   sourceSummarySettingsOf,
} from "./package_retrieval";

describe("publisher.json retrieval block", () => {
   it("defaults to single, auto, no prompts", async () => {
      expect(await readPackageRetrieval("/nonexistent", undefined)).toEqual(
         DEFAULT_PACKAGE_RETRIEVAL,
      );
      expect(DEFAULT_PACKAGE_RETRIEVAL).toEqual({
         representation: "single",
         keyphrases: "auto",
         prompts: {},
      });
   });

   it("accepts each valid value", () => {
      expect(
         parsePackageRetrieval({
            representation: "facets",
            keyphrases: "always",
         }),
      ).toMatchObject({ representation: "facets", keyphrases: "always" });
      expect(parsePackageRetrieval({ keyphrases: "never" }).keyphrases).toBe(
         "never",
      );
   });

   it("rejects bad values with the valid list and a fix", () => {
      expect(() => parsePackageRetrieval({ representation: "multi" })).toThrow(
         'Invalid publisher.json retrieval.representation: expected one of single, facets, got "multi". Fix:',
      );
      expect(() => parsePackageRetrieval({ keyphrases: true })).toThrow(
         "Invalid publisher.json retrieval.keyphrases: expected one of auto, never, always, got true. Fix:",
      );
      expect(() => parsePackageRetrieval("on")).toThrow(
         "Invalid publisher.json retrieval: expected an object",
      );
   });

   it("reads the engine switch and the fcj knobs, defaulting each unset knob", () => {
      expect(engineOf(undefined)).toBe("legacy");
      const parsed = parsePackageRetrieval({
         engine: "fcj",
         fcj: { perTarget: 50, judge: false },
      });
      expect(parsed.engine).toBe("fcj");
      expect(parsed.fcj).toEqual({
         ...DEFAULT_FCJ_SETTINGS,
         perTarget: 50,
         judge: false,
      });
      expect(fcjSettingsOf(undefined)).toEqual(DEFAULT_FCJ_SETTINGS);
      expect(DEFAULT_FCJ_SETTINGS).toEqual({
         perTarget: 30,
         hopDiscount: 0.9,
         coverFraction: 0.5,
         measureWeight: 1.5,
         maxClusters: 3,
         clusterTopK: 10,
         reachDepth: 3,
         keyword: true,
         questions: "auto",
         docParts: "auto",
         values: true,
         valueCap: 50_000,
         judge: "auto",
         contenderShare: 0,
         collapseCopies: false,
         connect: "clusters",
         listTopK: 15,
         questionsPrompt: "questions",
         questionsPerNode: 3,
         alsoLikely: "auto",
         alsoLikelyTwins: 2,
         joinedCopies: "indexed",
         listRoutes: 3,
         dropRoutesAt: 3,
         unservedCopies: 3,
         splitBundles: false,
      });
      expect(
         parsePackageRetrieval({ fcj: { connect: "list", listTopK: 12 } }).fcj,
      ).toEqual({ ...DEFAULT_FCJ_SETTINGS, connect: "list", listTopK: 12 });
      expect(
         parsePackageRetrieval({
            fcj: { questionsPrompt: "phrases", questionsPerNode: 5 },
         }).fcj,
      ).toEqual({
         ...DEFAULT_FCJ_SETTINGS,
         questionsPrompt: "phrases",
         questionsPerNode: 5,
      });
   });

   it("reads alsoLikely, alsoLikelyTwins and joinedCopies, and says when alsoLikely is on", () => {
      expect(
         parsePackageRetrieval({
            fcj: {
               alsoLikely: false,
               alsoLikelyTwins: 1,
               joinedCopies: "routes",
            },
         }).fcj,
      ).toEqual({
         ...DEFAULT_FCJ_SETTINGS,
         alsoLikely: false,
         alsoLikelyTwins: 1,
         joinedCopies: "routes",
      });
      expect(alsoLikelyOn({ alsoLikely: "auto", connect: "list" })).toBe(true);
      expect(alsoLikelyOn({ alsoLikely: "auto", connect: "clusters" })).toBe(
         false,
      );
      expect(alsoLikelyOn({ alsoLikely: false, connect: "list" })).toBe(false);
      expect(alsoLikelyOn({ alsoLikely: true, connect: "clusters" })).toBe(
         true,
      );
      expect(() =>
         parsePackageRetrieval({ fcj: { alsoLikely: "yes" } }),
      ).toThrow(
         'Invalid publisher.json retrieval.fcj.alsoLikely: expected "auto", true or false, got "yes". Fix: "alsoLikely": "auto" (on with connect "list", off with "clusters").',
      );
      expect(() =>
         parsePackageRetrieval({ fcj: { alsoLikelyTwins: 11 } }),
      ).toThrow(
         'Invalid publisher.json retrieval.fcj.alsoLikelyTwins: expected an integer from 0 to 10, got 11. Fix: "alsoLikelyTwins": 2',
      );
      expect(() =>
         parsePackageRetrieval({ fcj: { joinedCopies: "deep" } }),
      ).toThrow(
         'Invalid publisher.json retrieval.fcj.joinedCopies: expected one of indexed, routes, got "deep". Fix: "joinedCopies": "indexed" (joined copies are searchable rows) or "routes" (each field is searched on its own source and shown with its routes).',
      );
   });

   it("reads unservedCopies from 0 to 20, 3 by default, and rejects anything else with a fix", () => {
      expect(DEFAULT_FCJ_SETTINGS.unservedCopies).toBe(3);
      expect(
         parsePackageRetrieval({ fcj: { unservedCopies: 0 } }).fcj
            ?.unservedCopies,
      ).toBe(0);
      expect(
         parsePackageRetrieval({ fcj: { unservedCopies: 20 } }).fcj
            ?.unservedCopies,
      ).toBe(20);
      expect(() =>
         parsePackageRetrieval({ fcj: { unservedCopies: 21 } }),
      ).toThrow(
         'Invalid publisher.json retrieval.fcj.unservedCopies: expected an integer from 0 to 20, got 21. Fix: "unservedCopies": 3',
      );
   });

   it("reads dropRoutesAt from 0 to 20, 3 by default, and rejects anything else with a fix", () => {
      expect(DEFAULT_FCJ_SETTINGS.dropRoutesAt).toBe(3);
      expect(parsePackageRetrieval({ fcj: { dropRoutesAt: 0 } }).fcj).toEqual({
         ...DEFAULT_FCJ_SETTINGS,
         dropRoutesAt: 0,
      });
      expect(
         parsePackageRetrieval({ fcj: { dropRoutesAt: 20 } }).fcj?.dropRoutesAt,
      ).toBe(20);
      expect(() =>
         parsePackageRetrieval({ fcj: { dropRoutesAt: 21 } }),
      ).toThrow(
         'Invalid publisher.json retrieval.fcj.dropRoutesAt: expected an integer from 0 to 20, got 21. Fix: "dropRoutesAt": 3',
      );
      expect(() =>
         parsePackageRetrieval({ fcj: { dropRoutesAt: 2.5 } }),
      ).toThrow(
         'Invalid publisher.json retrieval.fcj.dropRoutesAt: expected an integer from 0 to 20, got 2.5. Fix: "dropRoutesAt": 3',
      );
   });

   it("reads listRoutes from 0 to 20 and rejects anything else with a fix", () => {
      expect(parsePackageRetrieval({ fcj: { listRoutes: 0 } }).fcj).toEqual({
         ...DEFAULT_FCJ_SETTINGS,
         listRoutes: 0,
      });
      expect(
         parsePackageRetrieval({ fcj: { listRoutes: 20 } }).fcj?.listRoutes,
      ).toBe(20);
      expect(() => parsePackageRetrieval({ fcj: { listRoutes: 21 } })).toThrow(
         'Invalid publisher.json retrieval.fcj.listRoutes: expected an integer from 0 to 20, got 21. Fix: "listRoutes": 3',
      );
      expect(() => parsePackageRetrieval({ fcj: { listRoutes: -1 } })).toThrow(
         'Invalid publisher.json retrieval.fcj.listRoutes: expected an integer from 0 to 20, got -1. Fix: "listRoutes": 3',
      );
   });

   it("rejects an unknown question wording or a row count out of range, with a fix", () => {
      expect(() =>
         parsePackageRetrieval({ fcj: { questionsPrompt: "keyphrases" } }),
      ).toThrow(
         'Invalid publisher.json retrieval.fcj.questionsPrompt: expected one of questions, phrases, values, got "keyphrases". Fix: "questionsPrompt": "questions" (what a person asks), "phrases" (what an agent types as a search target) or "values" (questions that filter by the field\'s values).',
      );
      expect(() =>
         parsePackageRetrieval({ fcj: { questionsPerNode: 9 } }),
      ).toThrow(
         'Invalid publisher.json retrieval.fcj.questionsPerNode: expected an integer from 1 to 8, got 9. Fix: "questionsPerNode": 3',
      );
   });

   it("rejects a bad engine or knob with the range and a fix", () => {
      expect(() => parsePackageRetrieval({ engine: "new" })).toThrow(
         'Invalid publisher.json retrieval.engine: expected one of legacy, fcj, got "new". Fix: "engine": "legacy" (the shipped search) or "fcj" (find, connect, judge).',
      );
      expect(() =>
         parsePackageRetrieval({ fcj: { coverFraction: 1.5 } }),
      ).toThrow(
         'Invalid publisher.json retrieval.fcj.coverFraction: expected a number from 0 to 1, got 1.5. Fix: "coverFraction": 0.5',
      );
      expect(() => parsePackageRetrieval({ fcj: { perTarget: 2.5 } })).toThrow(
         'Invalid publisher.json retrieval.fcj.perTarget: expected an integer from 1 to 500, got 2.5. Fix: "perTarget": 30',
      );
      expect(() => parsePackageRetrieval({ fcj: { window: 3 } })).toThrow(
         "Invalid publisher.json retrieval.fcj: unknown key 'window'. Valid keys: perTarget, hopDiscount, coverFraction, measureWeight, maxClusters, clusterTopK, reachDepth, keyword, questions, docParts, values, valueCap, judge, contenderShare, collapseCopies, connect, listTopK, questionsPrompt, questionsPerNode, alsoLikely, alsoLikelyTwins, joinedCopies, listRoutes, dropRoutesAt, unservedCopies, splitBundles.",
      );
      expect(() => parsePackageRetrieval({ fcj: { connect: "tree" } })).toThrow(
         'Invalid publisher.json retrieval.fcj.connect: expected one of clusters, list, got "tree". Fix: "connect": "clusters" (source groups) or "list" (the find stage\'s top listTopK per target).',
      );
      expect(() => parsePackageRetrieval({ fcj: { listTopK: 0 } })).toThrow(
         'Invalid publisher.json retrieval.fcj.listTopK: expected an integer from 1 to 500, got 0. Fix: "listTopK": 15',
      );
   });

   it("an unknown key is an error that names the valid keys, so a typo or a later release's key is not silently ignored", () => {
      let error: unknown;
      try {
         parsePackageRetrieval({ rephrase: { enabled: true } });
      } catch (e) {
         error = e;
      }
      expect(error).toBeInstanceOf(PackageManifestError);
      expect((error as Error).message).toBe(
         "Invalid publisher.json retrieval: unknown key 'rephrase'. Valid keys: representation, keyphrases, refine, rerank, sourceMatch, sourceSummary, prompts, engine, fcj.",
      );
      expect(() =>
         parsePackageRetrieval({ prompts: { rephrase: "p.md" } }),
      ).toThrow(
         "retrieval.prompts: unknown key 'rephrase'. Valid keys: keyphrase, refine, rerank, sourceMatch, sourceSummary, judge, docParts, questions.",
      );
   });
});

describe("publisher.json retrieval.refine and retrieval.rerank", () => {
   it("are absent by default, so the defaults object is unchanged", () => {
      const parsed = parsePackageRetrieval({ keyphrases: "never" });
      expect("refine" in parsed).toBe(false);
      expect("rerank" in parsed).toBe(false);
      expect(refineSettingsOf(DEFAULT_PACKAGE_RETRIEVAL)).toEqual({
         enabled: "auto",
         minLevel: "MEDIUM",
      });
      expect(rerankSettingsOf(DEFAULT_PACKAGE_RETRIEVAL)).toEqual({
         enabled: "auto",
         topSources: 8,
      });
   });

   it("accept each valid value", () => {
      expect(
         parsePackageRetrieval({
            refine: { enabled: true, minLevel: "HIGH" },
            rerank: { enabled: false, topSources: 3 },
         }),
      ).toMatchObject({
         refine: { enabled: true, minLevel: "HIGH" },
         rerank: { enabled: false, topSources: 3 },
      });
      expect(
         parsePackageRetrieval({ refine: {}, rerank: { enabled: "auto" } }),
      ).toMatchObject({
         refine: { enabled: "auto", minLevel: "MEDIUM" },
         rerank: { enabled: "auto", topSources: 8 },
      });
   });

   it("reject bad values with the valid list and a fix", () => {
      expect(() =>
         parsePackageRetrieval({ refine: { enabled: "yes" } }),
      ).toThrow(
         'Invalid publisher.json retrieval.refine.enabled: expected "auto", true or false, got "yes". Fix:',
      );
      expect(() =>
         parsePackageRetrieval({ refine: { minLevel: "NONE" } }),
      ).toThrow(
         'Invalid publisher.json retrieval.refine.minLevel: expected one of LOW, MEDIUM, HIGH, got "NONE". Fix:',
      );
      expect(() =>
         parsePackageRetrieval({ rerank: { topSources: 0 } }),
      ).toThrow(
         "Invalid publisher.json retrieval.rerank.topSources: expected a positive integer, got 0. Fix:",
      );
      expect(() =>
         parsePackageRetrieval({ rerank: { topSources: 2.5 } }),
      ).toThrow("expected a positive integer, got 2.5");
      expect(() => parsePackageRetrieval({ refine: { top: 1 } })).toThrow(
         "retrieval.refine: unknown key 'top'. Valid keys: enabled, minLevel.",
      );
      expect(() => parsePackageRetrieval({ rerank: "on" })).toThrow(
         "Invalid publisher.json retrieval.rerank: expected an object",
      );
   });

   it("enabled: true without an LLM stops the load and names both fixes", () => {
      const settings = readSync({ refine: { enabled: true } });
      expect(() => assertRequiredStagesAvailable(settings, false)).toThrow(
         PackageManifestError,
      );
      expect(() => assertRequiredStagesAvailable(settings, false)).toThrow(
         /retrieval\.refine\.enabled: true needs an LLM.*retrieval\.llm.*LLM_API_KEY.*"auto"/s,
      );
      const rerank = readSync({ rerank: { enabled: true } });
      expect(() => assertRequiredStagesAvailable(rerank, false)).toThrow(
         "retrieval.rerank.enabled: true needs an LLM",
      );
   });

   it("auto and false never fail, and true is fine when an LLM is configured", () => {
      for (const raw of [
         undefined,
         { refine: { enabled: "auto" }, rerank: { enabled: false } },
      ]) {
         expect(() =>
            assertRequiredStagesAvailable(readSync(raw), false),
         ).not.toThrow();
      }
      expect(() =>
         assertRequiredStagesAvailable(
            readSync({ refine: { enabled: true } }),
            true,
         ),
      ).not.toThrow();
   });

   it("sourceMatch defaults to auto and parses enabled", () => {
      expect(sourceMatchSettingsOf(DEFAULT_PACKAGE_RETRIEVAL)).toEqual({
         enabled: "auto",
      });
      expect("sourceMatch" in parsePackageRetrieval({})).toBe(false);
      expect(
         parsePackageRetrieval({ sourceMatch: { enabled: false } }).sourceMatch,
      ).toEqual({ enabled: false });
      expect(parsePackageRetrieval({ sourceMatch: {} }).sourceMatch).toEqual({
         enabled: "auto",
      });
      expect(() =>
         parsePackageRetrieval({ sourceMatch: { enabled: "yes" } }),
      ).toThrow(
         'Invalid publisher.json retrieval.sourceMatch.enabled: expected "auto", true or false, got "yes". Fix:',
      );
      expect(() =>
         parsePackageRetrieval({ sourceMatch: { topSources: 3 } }),
      ).toThrow(
         "retrieval.sourceMatch: unknown key 'topSources'. Valid keys: enabled.",
      );
   });

   it("sourceMatch enabled: true without an LLM stops the load and names both fixes", () => {
      const settings = readSync({ sourceMatch: { enabled: true } });
      expect(() => assertRequiredStagesAvailable(settings, false)).toThrow(
         PackageManifestError,
      );
      expect(() => assertRequiredStagesAvailable(settings, false)).toThrow(
         /retrieval\.sourceMatch\.enabled: true needs an LLM.*retrieval\.llm.*LLM_API_KEY.*"auto"/s,
      );
      expect(() => assertRequiredStagesAvailable(settings, true)).not.toThrow();
      expect(() =>
         assertRequiredStagesAvailable(
            readSync({ sourceMatch: { enabled: "auto" } }),
            false,
         ),
      ).not.toThrow();
   });
});

describe("retrieval.sourceSummary", () => {
   it("defaults to auto and parses enabled", () => {
      expect(sourceSummarySettingsOf(DEFAULT_PACKAGE_RETRIEVAL)).toEqual({
         enabled: "auto",
      });
      expect("sourceSummary" in parsePackageRetrieval({})).toBe(false);
      for (const enabled of ["auto", true, false] as const) {
         expect(
            parsePackageRetrieval({ sourceSummary: { enabled } }).sourceSummary,
         ).toEqual({ enabled });
      }
      expect(
         parsePackageRetrieval({ sourceSummary: {} }).sourceSummary,
      ).toEqual({ enabled: "auto" });
   });

   it("rejects a bad value or an unknown key, naming the key and a fix", () => {
      expect(() =>
         parsePackageRetrieval({ sourceSummary: { enabled: "yes" } }),
      ).toThrow(
         'Invalid publisher.json retrieval.sourceSummary.enabled: expected "auto", true or false, got "yes". Fix:',
      );
      expect(() =>
         parsePackageRetrieval({ sourceSummary: { maxFields: 3 } }),
      ).toThrow(
         "retrieval.sourceSummary: unknown key 'maxFields'. Valid keys: enabled.",
      );
      expect(() => parsePackageRetrieval({ sourceSummary: true })).toThrow(
         "Invalid publisher.json retrieval.sourceSummary: expected an object, got true. Fix:",
      );
   });

   it("enabled: true without an LLM stops the load and names both fixes", () => {
      const settings = readSync({ sourceSummary: { enabled: true } });
      expect(() => assertRequiredStagesAvailable(settings, false)).toThrow(
         PackageManifestError,
      );
      expect(() => assertRequiredStagesAvailable(settings, false)).toThrow(
         /retrieval\.sourceSummary\.enabled: true needs an LLM.*retrieval\.llm.*LLM_API_KEY.*"auto"/s,
      );
      expect(() => assertRequiredStagesAvailable(settings, true)).not.toThrow();
      for (const enabled of ["auto", false]) {
         expect(() =>
            assertRequiredStagesAvailable(
               readSync({ sourceSummary: { enabled } }),
               false,
            ),
         ).not.toThrow();
      }
   });

   it("reads the sourceSummary prompt, with the same path rules as the others", async () => {
      const base = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-summary-"));
      const root = path.join(base, "pkg");
      fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
      try {
         fs.writeFileSync(path.join(root, "prompts", "s.md"), "Summarize.");
         const s = await readPackageRetrieval(root, {
            prompts: { sourceSummary: "prompts/s.md" },
         });
         expect(s.prompts.sourceSummary).toMatchObject({
            path: "prompts/s.md",
            text: "Summarize.",
         });
         expect(s.prompts.sourceSummary?.hash).toMatch(/^[0-9a-f]{64}$/);
         await expect(
            readPackageRetrieval(root, {
               prompts: { sourceSummary: "../x.md" },
            }),
         ).rejects.toThrow(
            /retrieval\.prompts\.sourceSummary: "\.\.\/x\.md" resolves outside the package directory\. Fix: use a path inside the package, e\.g\. "prompts\/sourceSummary\.md"/,
         );
         await expect(
            readPackageRetrieval(root, {
               prompts: { sourceSummary: path.join(base, "x.md") },
            }),
         ).rejects.toThrow("is an absolute path");
         await expect(
            readPackageRetrieval(root, {
               prompts: { sourceSummary: "prompts/none.md" },
            }),
         ).rejects.toThrow("retrieval.prompts.sourceSummary: cannot read");
      } finally {
         fs.rmSync(base, { recursive: true, force: true });
      }
   });
});

/** Settings as readPackageRetrieval would return them, with no prompt files. */
function readSync(raw: unknown) {
   const parsed = parsePackageRetrieval(raw);
   return {
      representation: parsed.representation,
      keyphrases: parsed.keyphrases,
      ...(parsed.refine ? { refine: parsed.refine } : {}),
      ...(parsed.rerank ? { rerank: parsed.rerank } : {}),
      ...(parsed.sourceMatch ? { sourceMatch: parsed.sourceMatch } : {}),
      ...(parsed.sourceSummary ? { sourceSummary: parsed.sourceSummary } : {}),
      prompts: {},
   };
}

describe("prompt file", () => {
   let root: string;
   let outside: string;
   beforeEach(() => {
      const base = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-retrieval-"));
      root = path.join(base, "pkg");
      outside = path.join(base, "secret.txt");
      fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
      fs.writeFileSync(outside, "TOP SECRET");
   });
   afterEach(() => {
      fs.rmSync(path.dirname(root), { recursive: true, force: true });
   });

   it("reads the file at load and hashes its text", async () => {
      fs.writeFileSync(path.join(root, "prompts", "k.md"), "Write a phrase.");
      const s = await readPackageRetrieval(root, {
         prompts: { keyphrase: "prompts/k.md" },
      });
      expect(s.prompts.keyphrase).toMatchObject({
         path: "prompts/k.md",
         text: "Write a phrase.",
      });
      expect(s.prompts.keyphrase?.hash).toMatch(/^[0-9a-f]{64}$/);
      fs.writeFileSync(path.join(root, "prompts", "k.md"), "Different.");
      const again = await readPackageRetrieval(root, {
         prompts: { keyphrase: "prompts/k.md" },
      });
      expect(again.prompts.keyphrase?.hash).not.toBe(s.prompts.keyphrase?.hash);
   });

   it("reads the sourceMatch prompt, with the same path rules as the others", async () => {
      fs.writeFileSync(path.join(root, "prompts", "s.md"), "Pick sources.");
      const s = await readPackageRetrieval(root, {
         prompts: { sourceMatch: "prompts/s.md" },
      });
      expect(s.prompts.sourceMatch).toMatchObject({
         path: "prompts/s.md",
         text: "Pick sources.",
      });
      await expect(
         readPackageRetrieval(root, { prompts: { sourceMatch: "../x.md" } }),
      ).rejects.toThrow(
         /retrieval\.prompts\.sourceMatch: "\.\.\/x\.md" resolves outside the package directory\. Fix: use a path inside the package, e\.g\. "prompts\/sourceMatch\.md"/,
      );
      await expect(
         readPackageRetrieval(root, { prompts: { sourceMatch: outside } }),
      ).rejects.toThrow("is an absolute path");
   });

   it("rejects an absolute path", async () => {
      await expect(
         readPackageRetrieval(root, { prompts: { keyphrase: outside } }),
      ).rejects.toThrow("is an absolute path");
   });

   it("rejects a path that climbs out of the package", async () => {
      await expect(
         readPackageRetrieval(root, {
            prompts: { keyphrase: "../secret.txt" },
         }),
      ).rejects.toThrow("resolves outside the package directory");
      await expect(
         readPackageRetrieval(root, {
            prompts: { keyphrase: "prompts/../../secret.txt" },
         }),
      ).rejects.toThrow("resolves outside the package directory");
   });

   it("rejects a symlink that points outside the package", async () => {
      fs.symlinkSync(outside, path.join(root, "prompts", "link.md"));
      await expect(
         readPackageRetrieval(root, {
            prompts: { keyphrase: "prompts/link.md" },
         }),
      ).rejects.toThrow("through a link");
   });

   it("a missing or empty file stops the load with a message naming the key", async () => {
      await expect(
         readPackageRetrieval(root, {
            prompts: { keyphrase: "prompts/no.md" },
         }),
      ).rejects.toThrow("retrieval.prompts.keyphrase: cannot read");
      fs.writeFileSync(path.join(root, "prompts", "e.md"), "  \n");
      await expect(
         readPackageRetrieval(root, { prompts: { keyphrase: "prompts/e.md" } }),
      ).rejects.toThrow("is empty");
   });

   it("resolvePromptPath refuses the package root itself", () => {
      expect(() => resolvePromptPath(root, ".")).toThrow("outside");
   });

   it("accepts a directory or a file whose name only starts with two dots", async () => {
      // `..foo` is an ordinary name, not a step up. Only `..` itself, as a whole
      // path segment, climbs out of the package.
      fs.mkdirSync(path.join(root, "..prompts"), { recursive: true });
      fs.writeFileSync(path.join(root, "..prompts", "k.md"), "Dotted dir.");
      fs.writeFileSync(path.join(root, "..k.md"), "Dotted file.");
      const inDir = await readPackageRetrieval(root, {
         prompts: { keyphrase: "..prompts/k.md" },
      });
      expect(inDir.prompts.keyphrase?.text).toBe("Dotted dir.");
      const file = await readPackageRetrieval(root, {
         prompts: { keyphrase: "..k.md" },
      });
      expect(file.prompts.keyphrase?.text).toBe("Dotted file.");
      expect(resolvePromptPath(root, "..prompts/k.md")).toBe(
         path.join(path.resolve(root), "..prompts", "k.md"),
      );
   });

   it("still refuses a path that steps up, however it is written", () => {
      for (const rel of ["..", "../k.md", "prompts/../../k.md"]) {
         expect(() => resolvePromptPath(root, rel)).toThrow();
      }
   });
});
