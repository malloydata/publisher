// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it, test } from "bun:test";
import { modelSourcesLine, sourceCountLabel } from "./modelHeaderPresentation";

describe("modelSourcesLine", () => {
   test("singular for one source", () => {
      expect(modelSourcesLine(1)).toBe("1 source in this model");
   });

   test("plural otherwise", () => {
      expect(modelSourcesLine(10)).toBe("10 sources in this model");
   });

   // A model that compiles but declares no sources: the header still has to say
   // something, and "0 sources in this model" reads like a count that failed to
   // load rather than a fact about the file.
   test("names the empty case in words", () => {
      expect(modelSourcesLine(0)).toBe("No sources in this model");
   });
});

describe("sourceCountLabel", () => {
   it("counts a model's sources, and the root index.malloy's as public", () => {
      expect(sourceCountLabel(5)).toBe("5 sources");
      expect(sourceCountLabel(1)).toBe("1 source");
      expect(sourceCountLabel(4, true)).toBe("4 public sources");
      expect(sourceCountLabel(0)).toBe("No sources");
   });
});
