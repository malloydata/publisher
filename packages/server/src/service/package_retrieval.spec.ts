// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { PackageManifestError } from "../errors";
import {
   DEFAULT_PACKAGE_RETRIEVAL,
   assertRequiredStagesAvailable,
   parsePackageRetrieval,
   readPackageRetrieval,
   refineSettingsOf,
   rerankSettingsOf,
   resolvePromptPath,
   sourceMatchSettingsOf,
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

   it("an unknown key is an error that names the valid keys, so a typo or a later release's key is not silently ignored", () => {
      let error: unknown;
      try {
         parsePackageRetrieval({ rephrase: { enabled: true } });
      } catch (e) {
         error = e;
      }
      expect(error).toBeInstanceOf(PackageManifestError);
      expect((error as Error).message).toBe(
         "Invalid publisher.json retrieval: unknown key 'rephrase'. Valid keys: representation, keyphrases, refine, rerank, sourceMatch, prompts.",
      );
      expect(() =>
         parsePackageRetrieval({ prompts: { rephrase: "p.md" } }),
      ).toThrow(
         "retrieval.prompts: unknown key 'rephrase'. Valid keys: keyphrase, refine, rerank, sourceMatch.",
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

/** Settings as readPackageRetrieval would return them, with no prompt files. */
function readSync(raw: unknown) {
   const parsed = parsePackageRetrieval(raw);
   return {
      representation: parsed.representation,
      keyphrases: parsed.keyphrases,
      ...(parsed.refine ? { refine: parsed.refine } : {}),
      ...(parsed.rerank ? { rerank: parsed.rerank } : {}),
      ...(parsed.sourceMatch ? { sourceMatch: parsed.sourceMatch } : {}),
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
      ).rejects.toThrow(/resolves outside the package directory\. Fix/);
      await expect(
         readPackageRetrieval(root, {
            prompts: { keyphrase: "prompts/../../secret.txt" },
         }),
      ).rejects.toThrow(/resolves outside the package directory\. Fix/);
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
});
