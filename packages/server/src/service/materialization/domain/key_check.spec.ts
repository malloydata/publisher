// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   KEY_RULES,
   requireUnique,
   verdictOf,
   type KeyCheckKind,
} from "./key_check";

/**
 * The NULL rule and what a repeat does, per kind, as decided: a check that
 * stands in for a join a NULL never meets ignores a NULL key; a check that
 * counts rows per key fails on one. Each emitted text follows its row
 * (`text/key_check_text.ts`; run per writer in
 * `run/key_check_null_rule.spec.ts`).
 */
const DECIDED: Record<KeyCheckKind, [string, string]> = {
   inline_join_one: ["ignored", "fail_build"],
   pair_level: ["fails", "fail_build"],
   rollup_key: ["ignored", "record"],
   parent_key: ["fails", "record"],
   grain_join_key: ["ignored", "record"],
   stored_key: ["fails", "record"],
};

describe("key rules", () => {
   for (const [kind, [nullKey, onRepeat]] of Object.entries(DECIDED)) {
      it(`${kind}: a NULL key ${nullKey === "fails" ? "fails" : "is ignored"}, a repeat ${onRepeat === "fail_build" ? "fails the build" : "is recorded"}`, () => {
         expect(KEY_RULES[kind as KeyCheckKind] as object).toEqual({
            nullKey,
            onRepeat,
         });
      });
   }

   it("reads only a unique key as unique", () => {
      expect(requireUnique("unique")).toBe(true);
      expect(requireUnique("repeated")).toBe(false);
      expect(requireUnique("unknown")).toBe(false);
      expect(verdictOf(true)).toBe("unique");
      expect(verdictOf(false)).toBe("repeated");
      expect(verdictOf(undefined)).toBe("unknown");
   });
});
