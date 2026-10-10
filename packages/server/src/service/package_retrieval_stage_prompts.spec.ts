// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { readPackageRetrieval } from "./package_retrieval";

describe("refine and rerank prompt files", () => {
   let root: string;
   beforeEach(() => {
      root = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-stage-prompts-"));
      fs.mkdirSync(path.join(root, "prompts"));
      fs.writeFileSync(path.join(root, "prompts", "r.md"), "Rate fields.");
   });
   afterEach(() => {
      fs.rmSync(root, { recursive: true, force: true });
   });

   it("are read and hashed like the keyphrase prompt", async () => {
      const s = await readPackageRetrieval(root, {
         prompts: { refine: "prompts/r.md", rerank: "prompts/r.md" },
      });
      expect(s.prompts.refine?.text).toBe("Rate fields.");
      expect(s.prompts.refine?.path).toBe("prompts/r.md");
      expect(s.prompts.rerank?.hash).toMatch(/^[0-9a-f]{64}$/);
      expect(s.prompts.keyphrase).toBeUndefined();
   });

   it("refuse an absolute path, naming the key", async () => {
      await expect(
         readPackageRetrieval(root, { prompts: { refine: "/etc/passwd" } }),
      ).rejects.toThrow(
         'Invalid publisher.json retrieval.prompts.refine: "/etc/passwd" is an absolute path. Fix: use a path inside the package, e.g. "prompts/refine.md".',
      );
   });

   it("refuse a path that climbs out of the package", async () => {
      await expect(
         readPackageRetrieval(root, { prompts: { rerank: "../x.md" } }),
      ).rejects.toThrow(
         'retrieval.prompts.rerank: "../x.md" resolves outside the package directory',
      );
   });

   it("refuse a symlink that points outside the package", async () => {
      const outside = path.join(path.dirname(root), "outside-secret.txt");
      fs.writeFileSync(outside, "SECRET");
      try {
         fs.symlinkSync(outside, path.join(root, "prompts", "link.md"));
         await expect(
            readPackageRetrieval(root, {
               prompts: { refine: "prompts/link.md" },
            }),
         ).rejects.toThrow("retrieval.prompts.refine");
      } finally {
         fs.rmSync(outside, { force: true });
      }
   });

   it("refuse a missing or empty file", async () => {
      await expect(
         readPackageRetrieval(root, { prompts: { rerank: "prompts/no.md" } }),
      ).rejects.toThrow("retrieval.prompts.rerank: cannot read");
      fs.writeFileSync(path.join(root, "prompts", "e.md"), " \n");
      await expect(
         readPackageRetrieval(root, { prompts: { refine: "prompts/e.md" } }),
      ).rejects.toThrow("retrieval.prompts.refine");
   });
});
