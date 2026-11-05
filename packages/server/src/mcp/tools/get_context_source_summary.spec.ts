// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which cards of a get_context answer carry a source's full stored summary,
 * and how a card finds its summary when a source is defined in two files.
 */

import { describe, expect, it } from "bun:test";
import { summaryViewFor } from "./get_context_response";
import { summaryKey } from "./source_summaries";

describe("a source defined in two files", () => {
   const stored = new Map([
      [summaryKey("a.malloy", "orders"), { summary: "A", oneLineSummary: "a" }],
      [summaryKey("b.malloy", "orders"), { summary: "B", oneLineSummary: "b" }],
   ]);
   const card = (modelPath: string, withSourceRow: boolean) => ({
      key: summaryKey(modelPath, "orders"),
      source: "orders",
      rows: withSourceRow
         ? [{ kind: "source" } as unknown as { kind: string }]
         : [],
   });

   it("a pinned source name opens the full summary on both files' cards", () => {
      const view = summaryViewFor(stored, { sourceName: "orders" }, [
         card("a.malloy", false),
         card("b.malloy", false),
      ] as never);
      expect([...(view?.full ?? [])].sort()).toEqual([
         summaryKey("a.malloy", "orders"),
         summaryKey("b.malloy", "orders"),
      ]);
   });

   it("a card is looked up by file and name, so each file shows its own", () => {
      const view = summaryViewFor(stored, {}, [
         card("a.malloy", true),
         card("b.malloy", true),
      ] as never);
      expect(view?.stored.get(summaryKey("a.malloy", "orders"))?.summary).toBe(
         "A",
      );
      expect(view?.stored.get(summaryKey("b.malloy", "orders"))?.summary).toBe(
         "B",
      );
   });
});
