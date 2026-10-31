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
   fcjSettingsOf,
   assertRequiredStagesAvailable,
   ignoredRetrievalWarnings,
   parsePackageRetrieval,
   readPackageRetrieval,
   resolvePromptPath,
   sourceSummarySettingsOf,
} from "./package_retrieval";

describe("publisher.json retrieval block", () => {
   it("defaults to no overrides, so every package gets the engine's defaults", async () => {
      expect(await readPackageRetrieval("/nonexistent", undefined)).toEqual(
         DEFAULT_PACKAGE_RETRIEVAL,
      );
      expect(DEFAULT_PACKAGE_RETRIEVAL).toEqual({ prompts: {} });
      expect(fcjSettingsOf(DEFAULT_PACKAGE_RETRIEVAL)).toEqual(
         DEFAULT_FCJ_SETTINGS,
      );
   });

   it("rejects a block that is not an object, with a fix", () => {
      expect(() => parsePackageRetrieval("on")).toThrow(
         "Invalid publisher.json retrieval: expected an object",
      );
   });

   it("reads the fcj knobs, defaulting each unset knob", () => {
      const parsed = parsePackageRetrieval({
         fcj: { perTarget: 50, judge: false },
      });
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
         valueEmbeddings: true,
         judge: "auto",
         contenderShare: 0,
         collapseCopies: false,
         connect: "list",
         listTopK: 30,
         questionsPrompt: "questions",
         questionsPerNode: 3,
         alsoLikely: "auto",
         alsoLikelyTwins: 2,
         joinedCopies: "indexed",
         listRoutes: 3,
         dropRoutesAt: 4,
         unservedCopies: 3,
         splitBundles: false,
         splitItemPicks: 0,
         dateRange: false,
         listUnion: 30,
         readingsRule: true,
         twinRule: false,
         judgeLlm: null,
      });
      expect(
         parsePackageRetrieval({ fcj: { connect: "clusters", listTopK: 12 } })
            .fcj,
      ).toEqual({ ...DEFAULT_FCJ_SETTINGS, connect: "clusters", listTopK: 12 });
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

   it("reads dropRoutesAt from 0 to 20, 4 by default, and rejects anything else with a fix", () => {
      expect(DEFAULT_FCJ_SETTINGS.dropRoutesAt).toBe(4);
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
         'Invalid publisher.json retrieval.fcj.dropRoutesAt: expected an integer from 0 to 20, got 21. Fix: "dropRoutesAt": 4',
      );
      expect(() =>
         parsePackageRetrieval({ fcj: { dropRoutesAt: 2.5 } }),
      ).toThrow(
         'Invalid publisher.json retrieval.fcj.dropRoutesAt: expected an integer from 0 to 20, got 2.5. Fix: "dropRoutesAt": 4',
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

   it("rejects a bad knob with the range and a fix", () => {
      expect(() =>
         parsePackageRetrieval({ fcj: { coverFraction: 1.5 } }),
      ).toThrow(
         'Invalid publisher.json retrieval.fcj.coverFraction: expected a number from 0 to 1, got 1.5. Fix: "coverFraction": 0.5',
      );
      expect(() => parsePackageRetrieval({ fcj: { perTarget: 2.5 } })).toThrow(
         'Invalid publisher.json retrieval.fcj.perTarget: expected an integer from 1 to 500, got 2.5. Fix: "perTarget": 30',
      );
      expect(() => parsePackageRetrieval({ fcj: { window: 3 } })).toThrow(
         "Invalid publisher.json retrieval.fcj: unknown key 'window'. Valid keys: perTarget, hopDiscount, coverFraction, measureWeight, maxClusters, clusterTopK, reachDepth, keyword, questions, docParts, values, valueCap, valueEmbeddings, judge, contenderShare, collapseCopies, connect, listTopK, questionsPrompt, questionsPerNode, alsoLikely, alsoLikelyTwins, joinedCopies, listRoutes, dropRoutesAt, unservedCopies, splitBundles, splitItemPicks, dateRange, listUnion, readingsRule, twinRule, judgeLlm.",
      );
      expect(() => parsePackageRetrieval({ fcj: { connect: "tree" } })).toThrow(
         'Invalid publisher.json retrieval.fcj.connect: expected one of clusters, list, got "tree". Fix: "connect": "clusters" (source groups) or "list" (the find stage\'s top listTopK per target).',
      );
      expect(() => parsePackageRetrieval({ fcj: { listTopK: 0 } })).toThrow(
         'Invalid publisher.json retrieval.fcj.listTopK: expected an integer from 1 to 500, got 0. Fix: "listTopK": 30',
      );
   });

   it("reads judgeLlm, the judge's own model, and rejects a bad one", () => {
      expect(
         parsePackageRetrieval({
            fcj: {
               judgeLlm: { provider: "anthropic", model: " claude-haiku-5-5 " },
            },
         }).fcj?.judgeLlm,
      ).toEqual({ provider: "anthropic", model: "claude-haiku-5-5" });
      expect(
         parsePackageRetrieval({
            fcj: {
               judgeLlm: {
                  provider: "vertex",
                  model: "gemini-3.1-flash-lite",
                  projectId: "p",
                  location: "global",
               },
            },
         }).fcj?.judgeLlm,
      ).toEqual({
         provider: "vertex",
         model: "gemini-3.1-flash-lite",
         projectId: "p",
         location: "global",
      });
      expect(
         parsePackageRetrieval({ fcj: { judgeLlm: null } }).fcj?.judgeLlm,
      ).toBeNull();
      expect(() =>
         parsePackageRetrieval({
            fcj: { judgeLlm: { provider: "claude", model: "x" } },
         }),
      ).toThrow(
         'Invalid publisher.json retrieval.fcj.judgeLlm.provider: expected one of openai, openai-compatible, ollama, anthropic, google, vertex, got "claude". Fix: "judgeLlm": { "provider": "anthropic", "model": "claude-haiku-5-5" }',
      );
      expect(() =>
         parsePackageRetrieval({
            fcj: { judgeLlm: { provider: "anthropic" } },
         }),
      ).toThrow(
         "Invalid publisher.json retrieval.fcj.judgeLlm.model: expected a non-empty string, got nothing.",
      );
      expect(() =>
         parsePackageRetrieval({
            fcj: { judgeLlm: { provider: "vertex", model: "g" } },
         }),
      ).toThrow(
         "retrieval.fcj.judgeLlm.projectId: expected a non-empty string",
      );
      expect(() =>
         parsePackageRetrieval({
            fcj: { judgeLlm: { provider: "anthropic", model: "m", key: "k" } },
         }),
      ).toThrow("retrieval.fcj.judgeLlm: unknown key 'key'.");
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
         "Invalid publisher.json retrieval: unknown key 'rephrase'. Valid keys: sourceSummary, prompts, fcj.",
      );
      expect(() =>
         parsePackageRetrieval({ prompts: { rephrase: "p.md" } }),
      ).toThrow(
         "retrieval.prompts: unknown key 'rephrase'. Valid keys: sourceSummary, judge, docParts, questions.",
      );
   });
});

describe("keys of the retrieval block that an earlier release read", () => {
   const OLD_BLOCK = {
      engine: "legacy",
      representation: "facets",
      keyphrases: "always",
      refine: { enabled: true, minLevel: "HIGH" },
      rerank: { enabled: true, topSources: 3 },
      sourceMatch: { enabled: true },
      prompts: {
         keyphrase: "prompts/k.md",
         refine: "prompts/r.md",
         rerank: "prompts/rr.md",
         sourceMatch: "prompts/s.md",
      },
   };

   it("load, whatever their value, and leave the settings as the defaults", async () => {
      // None of the prompt files exists, and the block has a stage turned on
      // that needs an LLM: neither stops the load, because none is read.
      const settings = await readPackageRetrieval("/nonexistent", OLD_BLOCK);
      expect(settings).toEqual(DEFAULT_PACKAGE_RETRIEVAL);
      expect(() =>
         assertRequiredStagesAvailable(settings, false),
      ).not.toThrow();
      // Not even a value the old release refused.
      expect(
         parsePackageRetrieval({ engine: "new", refine: "on", rerank: 3 }),
      ).toEqual({ promptPaths: {} });
   });

   it("keep the keys that still work beside them", async () => {
      const parsed = parsePackageRetrieval({
         ...OLD_BLOCK,
         fcj: { perTarget: 50 },
         sourceSummary: { enabled: false },
      });
      expect(parsed.fcj?.perTarget).toBe(50);
      expect(parsed.sourceSummary).toEqual({ enabled: false });
   });

   it("are reported once each, naming the key and saying it no longer does anything", () => {
      const warnings = ignoredRetrievalWarnings(OLD_BLOCK);
      expect(warnings).toEqual([
         "publisher.json retrieval.engine no longer does anything and is ignored: get_context has one search engine now, and every package uses it. Fix: remove the key.",
         "publisher.json retrieval.representation no longer does anything and is ignored: the search index no longer keeps a per-entity text representation. Fix: remove the key.",
         "publisher.json retrieval.keyphrases no longer does anything and is ignored: the search index no longer writes keyphrases. Fix: remove the key.",
         "publisher.json retrieval.refine no longer does anything and is ignored: the engine's judge step replaces it (retrieval.fcj.judge turns that step off). Fix: remove the key.",
         "publisher.json retrieval.rerank no longer does anything and is ignored: the engine's judge step replaces it (retrieval.fcj.judge turns that step off). Fix: remove the key.",
         "publisher.json retrieval.sourceMatch no longer does anything and is ignored: the engine's judge step replaces it (retrieval.fcj.judge turns that step off). Fix: remove the key.",
         "publisher.json retrieval.prompts.keyphrase no longer does anything and is ignored: the search index no longer writes keyphrases. Fix: remove the key.",
         "publisher.json retrieval.prompts.refine no longer does anything and is ignored: the refine step is gone; retrieval.prompts.judge replaces the judge's instructions. Fix: remove the key.",
         "publisher.json retrieval.prompts.rerank no longer does anything and is ignored: the rerank step is gone; retrieval.prompts.judge replaces the judge's instructions. Fix: remove the key.",
         "publisher.json retrieval.prompts.sourceMatch no longer does anything and is ignored: the source match step is gone; retrieval.prompts.judge replaces the judge's instructions. Fix: remove the key.",
      ]);
   });

   it("say nothing for a block that sets none of them", () => {
      expect(ignoredRetrievalWarnings(undefined)).toEqual([]);
      expect(ignoredRetrievalWarnings("on")).toEqual([]);
      expect(
         ignoredRetrievalWarnings({
            fcj: { perTarget: 50 },
            sourceSummary: { enabled: "auto" },
            prompts: { judge: "prompts/j.md" },
         }),
      ).toEqual([]);
   });

   it("do not make a typo pass: an unknown key is still an error", () => {
      expect(() => parsePackageRetrieval({ rerankk: {} })).toThrow(
         "unknown key 'rerankk'",
      );
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
         prompts: { judge: "prompts/k.md" },
      });
      expect(s.prompts.judge).toMatchObject({
         path: "prompts/k.md",
         text: "Write a phrase.",
      });
      expect(s.prompts.judge?.hash).toMatch(/^[0-9a-f]{64}$/);
      fs.writeFileSync(path.join(root, "prompts", "k.md"), "Different.");
      const again = await readPackageRetrieval(root, {
         prompts: { judge: "prompts/k.md" },
      });
      expect(again.prompts.judge?.hash).not.toBe(s.prompts.judge?.hash);
   });

   it("reads the questions prompt, with the same path rules as the others", async () => {
      fs.writeFileSync(path.join(root, "prompts", "s.md"), "Write questions.");
      const s = await readPackageRetrieval(root, {
         prompts: { questions: "prompts/s.md" },
      });
      expect(s.prompts.questions).toMatchObject({
         path: "prompts/s.md",
         text: "Write questions.",
      });
      await expect(
         readPackageRetrieval(root, { prompts: { questions: "../x.md" } }),
      ).rejects.toThrow(
         /retrieval\.prompts\.questions: "\.\.\/x\.md" resolves outside the package directory\. Fix: use a path inside the package, e\.g\. "prompts\/questions\.md"/,
      );
      await expect(
         readPackageRetrieval(root, { prompts: { questions: outside } }),
      ).rejects.toThrow("is an absolute path");
   });

   it("rejects an absolute path", async () => {
      await expect(
         readPackageRetrieval(root, { prompts: { judge: outside } }),
      ).rejects.toThrow("is an absolute path");
   });

   it("rejects a path that climbs out of the package", async () => {
      await expect(
         readPackageRetrieval(root, {
            prompts: { judge: "../secret.txt" },
         }),
      ).rejects.toThrow("resolves outside the package directory");
      await expect(
         readPackageRetrieval(root, {
            prompts: { judge: "prompts/../../secret.txt" },
         }),
      ).rejects.toThrow("resolves outside the package directory");
   });

   it("rejects a symlink that points outside the package", async () => {
      fs.symlinkSync(outside, path.join(root, "prompts", "link.md"));
      await expect(
         readPackageRetrieval(root, {
            prompts: { judge: "prompts/link.md" },
         }),
      ).rejects.toThrow("through a link");
   });

   it("a missing or empty file stops the load with a message naming the key", async () => {
      await expect(
         readPackageRetrieval(root, {
            prompts: { judge: "prompts/no.md" },
         }),
      ).rejects.toThrow("retrieval.prompts.judge: cannot read");
      fs.writeFileSync(path.join(root, "prompts", "e.md"), "  \n");
      await expect(
         readPackageRetrieval(root, { prompts: { judge: "prompts/e.md" } }),
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
         prompts: { judge: "..prompts/k.md" },
      });
      expect(inDir.prompts.judge?.text).toBe("Dotted dir.");
      const file = await readPackageRetrieval(root, {
         prompts: { judge: "..k.md" },
      });
      expect(file.prompts.judge?.text).toBe("Dotted file.");
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
