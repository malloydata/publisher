// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   backoffAfter,
   FIRST_BACKOFF_MS,
   localPackageFingerprint,
   MAX_BACKOFF_MS,
} from "./configured_publish_retry";

describe("backoffAfter", () => {
   it("waits nothing before the first retry, then doubles to a cap", () => {
      expect(backoffAfter(0)).toBe(0);
      expect(backoffAfter(1)).toBe(FIRST_BACKOFF_MS);
      expect(backoffAfter(2)).toBe(FIRST_BACKOFF_MS * 2);
      expect(backoffAfter(30)).toBe(MAX_BACKOFF_MS);
   });
});

describe("localPackageFingerprint", () => {
   let dir: string;
   afterEach(() => fs.rmSync(dir, { recursive: true, force: true }));

   it("changes with a model file or publisher.json, not with data or vendored files", async () => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "fingerprint-"));
      fs.writeFileSync(path.join(dir, "publisher.json"), "{}");
      fs.writeFileSync(path.join(dir, "m.malloy"), "source: a is b");
      fs.mkdirSync(path.join(dir, "node_modules"));
      const first = await localPackageFingerprint(dir);
      expect(first).toBeDefined();

      fs.writeFileSync(path.join(dir, "data.csv"), "a\n1\n");
      fs.writeFileSync(path.join(dir, "node_modules", "x.malloy"), "x");
      expect(await localPackageFingerprint(dir)).toBe(first);

      fs.writeFileSync(path.join(dir, "m.malloy"), "source: a is bb");
      expect(await localPackageFingerprint(dir)).not.toBe(first);
   });

   it("is undefined for a location that cannot be read", async () => {
      dir = path.join(os.tmpdir(), "no-such-fingerprint-dir-xyz");
      expect(await localPackageFingerprint(dir)).toBeUndefined();
   });
});
