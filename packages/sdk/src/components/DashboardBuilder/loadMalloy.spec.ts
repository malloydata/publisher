// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   loadMalloy,
   loadMalloyTag,
   loadQueryBuilder,
   unwrapMalloy,
   unwrapMalloyTag,
   unwrapQueryBuilder,
} from "./loadMalloy";

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

describe("unwrapMalloyTag", () => {
   type Tag = Parameters<typeof unwrapMalloyTag>[0];
   const parseAnnotation = () => {};

   it("returns an ESM-style namespace as it is", () => {
      const ns = { parseAnnotation, default: { other: 1 } } as unknown as Tag;
      expect(unwrapMalloyTag(ns)).toBe(ns);
   });

   it("unwraps a default-only namespace from a bundled CommonJS import", () => {
      const unwrapped = unwrapMalloyTag({
         default: { parseAnnotation },
      } as unknown as Tag);
      expect(unwrapped.parseAnnotation as unknown).toBe(parseAnnotation);
   });

   it("loads the real package with a callable parser", async () => {
      const tag = await loadMalloyTag();
      expect(typeof tag.parseAnnotation).toBe("function");
   });
});

describe("unwrapQueryBuilder", () => {
   type Builder = Parameters<typeof unwrapQueryBuilder>[0];
   class ASTQuery {}

   it("returns an ESM-style namespace as it is", () => {
      const ns = { ASTQuery, default: { other: 1 } } as unknown as Builder;
      expect(unwrapQueryBuilder(ns)).toBe(ns);
   });

   it("unwraps a default-only namespace from a bundled CommonJS import", () => {
      const unwrapped = unwrapQueryBuilder({
         default: { ASTQuery },
      } as unknown as Builder);
      expect(unwrapped.ASTQuery as unknown).toBe(ASTQuery);
   });

   it("loads the real package with a constructible query", async () => {
      const builder = await loadQueryBuilder();
      expect(typeof builder.ASTQuery).toBe("function");
   });
});
