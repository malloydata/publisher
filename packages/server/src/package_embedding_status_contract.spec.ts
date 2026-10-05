// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { readFileSync } from "fs";
import { resolve } from "path";

/**
 * `embeddingIndex.status` on the package resource is a public value. A client
 * generated from api-doc.yaml switches on it, and removing a value from the
 * enum is a breaking change for that client even when the server no longer
 * sends it. `cooldown` and `too-many-entities` were statuses of their own
 * before `error` carried them as a `reason`, so they stay in the enum, marked
 * deprecated, and the server never sends them.
 */

const REPO_ROOT = resolve(import.meta.dir, "../../..");

/** The lines of `PackageEmbeddingIndex.properties.status`. */
function statusBlock(apiDoc: string): string[] {
   // api-doc.yaml is checked out with CRLF line endings on Windows.
   const lines = apiDoc.split(/\r?\n/);
   const start = lines.findIndex((l) => l === "    PackageEmbeddingIndex:");
   if (start < 0) throw new Error("PackageEmbeddingIndex not found");
   const statusAt = lines.findIndex(
      (l, i) => i > start && l === "        status:",
   );
   if (statusAt < 0) throw new Error("status property not found");
   const end = lines.findIndex((l, i) => i > statusAt && /^ {8}\S/.test(l));
   return lines.slice(statusAt, end < 0 ? undefined : end);
}

function enumValues(block: string[]): string[] {
   const at = block.findIndex((l) => l.trim() === "enum:");
   const values: string[] = [];
   for (const l of block.slice(at + 1)) {
      const m = l.match(/^ {12}- (\S+)$/);
      if (!m) break;
      values.push(m[1]);
   }
   return values;
}

describe("PackageEmbeddingIndex.status in api-doc.yaml", () => {
   const block = statusBlock(
      readFileSync(resolve(REPO_ROOT, "api-doc.yaml"), "utf8"),
   );

   it("lists the current values", () => {
      const values = enumValues(block);
      for (const v of ["lexical", "indexing", "ready", "error"]) {
         expect(values).toContain(v);
      }
   });

   it("keeps the two values that were removed from use, so no client breaks", () => {
      const values = enumValues(block);
      expect(values).toContain("cooldown");
      expect(values).toContain("too-many-entities");
   });

   it("says the two kept values are deprecated and are never sent", () => {
      const text = block.join("\n");
      expect(text).toMatch(/deprecated/i);
      expect(text).toMatch(/never (sent|returned|emitted)/i);
   });
});
