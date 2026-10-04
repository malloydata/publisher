// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { PackageManifestError } from "../errors";
import {
   DEFAULT_PACKAGE_RETRIEVAL,
   parsePackageRetrieval,
   readPackageRetrieval,
   resolvePromptPath,
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
         parsePackageRetrieval({ refine: { enabled: true } });
      } catch (e) {
         error = e;
      }
      expect(error).toBeInstanceOf(PackageManifestError);
      expect((error as Error).message).toBe(
         "Invalid publisher.json retrieval: unknown key 'refine'. Valid keys: representation, keyphrases, prompts.",
      );
      expect(() =>
         parsePackageRetrieval({ prompts: { rerank: "p.md" } }),
      ).toThrow(
         "retrieval.prompts: unknown key 'rerank'. Valid keys: keyphrase.",
      );
   });
});

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
