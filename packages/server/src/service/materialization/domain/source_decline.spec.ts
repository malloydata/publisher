// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   SOURCE_DECLINE_REASONS,
   servedCopyOutcomes,
   servesInPart,
} from "./source_decline";

/**
 * The one table of reasons a stored source is not served, and how one
 * compile's problems become one counted outcome per source.
 */

describe("the decline reasons", () => {
   it("lists every reason once, in the table's order", () => {
      expect(SOURCE_DECLINE_REASONS).toEqual([
         "build_plan_unavailable",
         "refused",
         "missing_table",
         "unquotable_name",
         "compile_failed",
         "cross_connection_unbuilt",
         "column_types",
         "no_declaration",
         "range_not_found",
         "definition_not_parsed",
         "pair_join_not_found",
         "pair_not_compiled",
         "fields_not_carried",
         "read_terms_not_kept",
         "cross_connection_join",
         "inline_join_kept",
         "reroot_not_compiled",
         "classification_not_kept",
      ]);
   });

   it("serves in part only for a join read as written or left out", () => {
      expect(SOURCE_DECLINE_REASONS.filter(servesInPart)).toEqual([
         "cross_connection_join",
         "inline_join_kept",
      ]);
   });
});

describe("servedCopyOutcomes", () => {
   it("counts nothing for a compile with no problems", () => {
      expect(servedCopyOutcomes([], new Set())).toEqual([]);
   });

   it("declines a source with any reason that leaves it out, by the first such reason", () => {
      expect(
         servedCopyOutcomes(
            [
               { sourceID: "a@f", reason: "inline_join_kept" },
               { sourceID: "a@f", reason: "column_types" },
               { sourceID: "a@f", reason: "compile_failed" },
            ],
            new Set(["a@f"]),
         ),
      ).toEqual([{ outcome: "declined", reason: "column_types" }]);
   });

   it("counts a source served in part as partial only while it is still re-rooted", () => {
      const problems = [
         { sourceID: "a@f", reason: "cross_connection_join" as const },
         { sourceID: "a@f", reason: "inline_join_kept" as const },
      ];
      expect(servedCopyOutcomes(problems, new Set(["a@f"]))).toEqual([
         { outcome: "partial", reason: "cross_connection_join" },
      ]);
      expect(servedCopyOutcomes(problems, new Set())).toEqual([
         { outcome: "declined", reason: "cross_connection_join" },
      ]);
   });

   it("groups by name when a problem has no sourceID, and never counts one partial", () => {
      expect(
         servedCopyOutcomes(
            [
               { sourceName: "a", reason: "inline_join_kept" },
               { sourceName: "a", reason: "inline_join_kept" },
            ],
            // A name is never a re-rooted sourceID.
            new Set(["a"]),
         ),
      ).toEqual([{ outcome: "declined", reason: "inline_join_kept" }]);
   });

   it("keeps a sourceID and a name apart, even when they spell the same", () => {
      expect(
         servedCopyOutcomes(
            [
               { sourceID: "a", reason: "inline_join_kept" },
               { sourceName: "a", reason: "compile_failed" },
            ],
            new Set(["a"]),
         ),
      ).toEqual([
         { outcome: "partial", reason: "inline_join_kept" },
         { outcome: "declined", reason: "compile_failed" },
      ]);
   });

   it("counts each problem that names no source as a declined of its own, first", () => {
      expect(
         servedCopyOutcomes(
            [
               { sourceID: "a@f", reason: "inline_join_kept" },
               { reason: "compile_failed" },
               { reason: "compile_failed" },
            ],
            new Set(["a@f"]),
         ),
      ).toEqual([
         { outcome: "declined", reason: "compile_failed" },
         { outcome: "declined", reason: "compile_failed" },
         { outcome: "partial", reason: "inline_join_kept" },
      ]);
   });
});
