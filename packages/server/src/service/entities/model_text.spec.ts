// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, test } from "bun:test";
import type { Field } from "@malloydata/malloy";
import { extractCodeFromEntity } from "./model_text";

/** A field whose range spans `code`; an `internal.malloy` URL reads `code` itself. */
function entitySpanning(code: string): Field {
   const lines = code.split("\n");
   return {
      name: "f",
      location: {
         url: "file:///internal.malloy",
         range: {
            start: { line: 0, character: 0 },
            end: { line: lines.length - 1, character: lines.at(-1)!.length },
         },
      },
   } as unknown as Field;
}

describe("extractCodeFromEntity", () => {
   test("keeps a declaration whole across a blank line inside it", () => {
      const code =
         "source: s is t extend {\n  dimension: a is 1\n\n  measure: c is count()\n}";
      expect(extractCodeFromEntity(entitySpanning(code), code)).toBe(code);
   });

   test("stops at the end of the text when the range runs past it", () => {
      const code = "dimension: a is 1";
      const entity = entitySpanning(code + "\nmore");
      expect(extractCodeFromEntity(entity, code)).toBe(code);
   });
});
