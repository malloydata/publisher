// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   BUNDLE_FORMAT,
   checkBundleStamp,
   computeSourceTreeSha,
   decodeBundle,
   decodeBundleStamp,
   encodeBundle,
   writeBundleFile,
   type BundleStamp,
} from "./bundle";
import type { LoadPackageResult } from "./protocol";

const stamp: BundleStamp = {
   format: BUNDLE_FORMAT,
   compilerVersion: "0.0.436",
   publisherVersion: "0.12.5",
   packageName: "pkg",
   sourceTreeSha: "abc",
   protocolVersion: 2,
   modelCount: 1,
   createdAt: "2026-11-09T00:00:00.000Z",
};

/** An outcome whose two models share one note object, as `#(authorize)` inheritance does. */
function outcomeWithAliasing(): LoadPackageResult {
   const note = { text: "#(authorize) 'analyst' = $ROLE" };
   return {
      type: "load-package-result",
      requestId: "r",
      protocolVersion: 2,
      packageMetadata: { name: "pkg" },
      models: [
         {
            modelPath: "a.malloy",
            modelType: "model",
            modelDef: { notes: [note] },
         },
         {
            modelPath: "b.malloy",
            modelType: "model",
            modelDef: { notes: [note] },
         },
      ],
      loadDurationMs: 1,
      timings: {
         compileDurationMs: 1,
         schemaFetchDurationMs: 0,
         schemaFetchCount: 0,
      },
   } as unknown as LoadPackageResult;
}

describe("compiled bundle", () => {
   it("round-trips the outcome and keeps object aliasing inside it", () => {
      const bytes = encodeBundle(stamp, outcomeWithAliasing());
      const { stamp: read, result } = decodeBundle(bytes);
      expect(read).toEqual(stamp);
      const [a, b] = result.models.map(
         (m) => (m.modelDef as { notes: unknown[] }).notes[0],
      );
      expect(a).toBe(b);
   });

   it("reads the stamp without inflating the outcome, and refuses another format", () => {
      const bytes = encodeBundle(stamp, outcomeWithAliasing());
      expect(decodeBundleStamp(bytes).packageName).toBe("pkg");
      const other = Buffer.concat([
         Buffer.from(
            JSON.stringify({ ...stamp, format: "something-else/9" }) + "\n",
         ),
         bytes.subarray(bytes.indexOf(0x0a) + 1),
      ]);
      expect(() => decodeBundleStamp(other)).toThrow(/format/);
   });

   it("is a hit only for the same compiler, protocol and source tree", () => {
      const expected = {
         compilerVersion: "0.0.436",
         sourceTreeSha: "abc",
         protocolVersion: 2,
      };
      expect(checkBundleStamp(stamp, expected)).toBe("hit");
      expect(
         checkBundleStamp(stamp, { ...expected, compilerVersion: "0.0.437" }),
      ).toBe("stale_compiler");
      expect(checkBundleStamp(stamp, { ...expected, protocolVersion: 3 })).toBe(
         "stale_compiler",
      );
      expect(
         checkBundleStamp(stamp, { ...expected, sourceTreeSha: "def" }),
      ).toBe("stale_source");
   });

   it("hashes the tree's files and not the root publisher.json an install rewrites", () => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bundle-sha-"));
      try {
         fs.writeFileSync(
            path.join(dir, "publisher.json"),
            JSON.stringify({ name: "p" }),
         );
         fs.writeFileSync(
            path.join(dir, "a.malloy"),
            "source: a is duckdb.sql('select 1')",
         );
         fs.mkdirSync(path.join(dir, ".credible"));
         fs.writeFileSync(path.join(dir, ".credible", "config.json"), "{}");
         const before = computeSourceTreeSha(dir);
         fs.writeFileSync(
            path.join(dir, "publisher.json"),
            JSON.stringify({ name: "p", modelBundleLocation: "/x.bundle" }),
         );
         expect(computeSourceTreeSha(dir)).toBe(before);
         fs.writeFileSync(
            path.join(dir, "a.malloy"),
            "source: a is duckdb.sql('select 2')",
         );
         expect(computeSourceTreeSha(dir)).not.toBe(before);
      } finally {
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("writes through a temp file, so a reader sees a whole bundle or none", () => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bundle-write-"));
      try {
         const target = path.join(dir, "nested", "p.bundle");
         writeBundleFile(target, encodeBundle(stamp, outcomeWithAliasing()));
         expect(fs.readdirSync(path.dirname(target))).toEqual(["p.bundle"]);
         expect(decodeBundleStamp(fs.readFileSync(target)).modelCount).toBe(1);
      } finally {
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});
