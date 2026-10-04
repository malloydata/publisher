// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { loadMalloy, unwrapMalloy } from "./loadMalloy";

type Namespace = Parameters<typeof unwrapMalloy>[0];

describe("unwrapMalloy", () => {
   class MalloyTranslator {}

   it("returns an ESM-style namespace as it is", () => {
      const ns = { MalloyTranslator, default: { other: 1 } };
      expect(unwrapMalloy(ns as unknown as Namespace)).toBe(
         ns as unknown as Namespace,
      );
   });

   it("unwraps a default-only namespace from a bundled CommonJS import", () => {
      const cjs = { MalloyTranslator };
      const unwrapped = unwrapMalloy({ default: cjs } as unknown as Namespace);
      expect(unwrapped.MalloyTranslator as unknown).toBe(MalloyTranslator);
   });

   it("loads the real package with a constructible translator", async () => {
      const malloy = await loadMalloy();
      expect(typeof malloy.MalloyTranslator).toBe("function");
   });
});
