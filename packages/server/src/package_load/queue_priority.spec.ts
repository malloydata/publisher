// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { nextQueuedJobIndex } from "./package_load_pool";

const job = (background?: boolean) => ({ request: { background } });

describe("nextQueuedJobIndex", () => {
   it("takes the oldest foreground job ahead of queued background jobs", () => {
      // A connection edit queued three re-derivations, then a publish arrived:
      // with one worker, the publish runs next, not after all three.
      expect(nextQueuedJobIndex([job(true), job(true), job(true), job()])).toBe(
         3,
      );
      expect(nextQueuedJobIndex([job(), job(true), job()])).toBe(0);
   });

   it("takes the oldest background job when nothing else waits", () => {
      expect(nextQueuedJobIndex([job(true), job(true)])).toBe(0);
   });

   it("is first-in, first-out for foreground jobs", () => {
      expect(nextQueuedJobIndex([job(false), job()])).toBe(0);
   });
});
