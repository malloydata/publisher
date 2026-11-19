// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { storageServes, storageWritable } from "./placement";

describe("the storage tier under each PERSIST_STORAGE_MODE", () => {
   it("is written under write-only and on, and read only under on", () => {
      expect(
         (["off", "write-only", "on"] as const).map((mode) => [
            mode,
            storageWritable(mode),
            storageServes(mode),
         ]),
      ).toEqual([
         ["off", false, false],
         ["write-only", true, false],
         ["on", true, true],
      ]);
   });
});
