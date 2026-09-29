// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   asList,
   extractJson,
   parseKeyphraseBatchReply,
   parseKeyphraseReply,
   parseRefineReply,
   parseRerankReply,
   parseSummaryReply,
   parseValueRefineReply,
} from "./llm_json";

describe("extractJson", () => {
   it("parses a bare array and object", () => {
      expect(extractJson('[{"a":1}]')).toEqual({ ok: true, value: [{ a: 1 }], salvaged: false });
      expect(extractJson('{"a":1}')).toMatchObject({ ok: true, value: { a: 1 } });
   });

   it("unwraps a code fence, with or without the language tag", () => {
      expect(extractJson('```json\n[1,2]\n```')).toMatchObject({ value: [1, 2] });
      expect(extractJson('```\n[1,2]\n```')).toMatchObject({ value: [1, 2] });
   });

   it("finds JSON inside prose", () => {
      expect(
         extractJson('Sure! Here you go:\n[{"index":0}]\nHope that helps.'),
      ).toMatchObject({ value: [{ index: 0 }] });
   });

   it("ignores <think> blocks, including brackets inside them", () => {
      expect(
         extractJson('<think>maybe [wrong]</think>[{"index":1}]'),
      ).toMatchObject({ value: [{ index: 1 }] });
   });

   it("survives brackets and quotes inside strings", () => {
      const r = extractJson('[{"reason":"a ] b \\" c }"}]');
      expect(r).toMatchObject({ ok: true, value: [{ reason: 'a ] b " c }' }] });
   });

   it("drops trailing commas", () => {
      expect(extractJson('[{"a":1,},]')).toMatchObject({ value: [{ a: 1 }] });
      expect(extractJson('{"a":[1,2,],}')).toMatchObject({ value: { a: [1, 2] } });
   });

   it("keeps a comma inside a string that precedes a bracket", () => {
      expect(extractJson('["a,]"]')).toMatchObject({ value: ["a,]"] });
   });

   it("salvages an array cut off mid-element", () => {
      const r = extractJson('[{"index":0,"score":"HIGH"},{"index":1,"sco');
      expect(r).toEqual({ ok: true, value: [{ index: 0, score: "HIGH" }], salvaged: true });
   });

   it("salvages an unterminated fence", () => {
      const r = extractJson('```json\n[{"index":0},{"index":1}');
      expect(r).toMatchObject({ ok: true, salvaged: true, value: [{ index: 0 }, { index: 1 }] });
   });

   it("fails cleanly on no JSON, and on an unrepairable one", () => {
      expect(extractJson("I cannot help with that.")).toEqual({
         ok: false,
         error: "no JSON found in the reply",
      });
      expect(extractJson("[1, 2, oops]").ok).toBe(false);
      expect(extractJson("{\"a\":").ok).toBe(false);
   });
});

describe("asList", () => {
   it("takes an array, or the first array an object wraps", () => {
      expect(asList([1])).toEqual([1]);
      expect(asList({ results: [1, 2] })).toEqual([1, 2]);
      expect(asList({ note: "x", items: [3] })).toEqual([3]);
      expect(asList({ a: 1 })).toBeNull();
      expect(asList("x")).toBeNull();
   });
});

describe("parseRefineReply", () => {
   const reply = (items: unknown) => JSON.stringify(items);

   it("reads levels case-insensitively and keeps the reason", () => {
      const p = parseRefineReply(
         reply([
            { index: 0, score: "high", reason: "exact match" },
            { index: 2, score: "LOW", reason: "  loose  " },
         ]),
         3,
      );
      expect(p.usable).toBe(true);
      expect(p.items).toEqual([
         { index: 0, level: "HIGH", reason: "exact match" },
         { index: 2, level: "LOW", reason: "loose" },
      ]);
   });

   it("accepts a wrapped array and string indices", () => {
      const p = parseRefineReply(
         reply({ results: [{ index: "1", score: "MEDIUM" }] }),
         2,
      );
      expect(p.items).toEqual([{ index: 1, level: "MEDIUM", reason: "" }]);
   });

   it("drops out-of-range, non-integer, duplicate and unscored entries", () => {
      const p = parseRefineReply(
         reply([
            { index: 0, score: "HIGH" },
            { index: 0, score: "LOW" }, // duplicate
            { index: 5, score: "HIGH" }, // out of range
            { index: 1.5, score: "HIGH" },
            { index: -1, score: "HIGH" },
            { index: 1, score: "MAYBE" },
            "junk",
         ]),
         3,
      );
      expect(p.items).toEqual([{ index: 0, level: "HIGH", reason: "" }]);
      expect(p.invalid).toBe(6);
      expect(p.usable).toBe(false);
   });

   it("treats NONE as an answer, not an error", () => {
      const p = parseRefineReply(
         reply([
            { index: 0, score: "NONE" },
            { index: 1, score: "HIGH" },
         ]),
         2,
      );
      expect(p.items).toHaveLength(1);
      expect(p.invalid).toBe(0);
      expect(p.usable).toBe(true);
   });

   it("treats an empty list as a usable 'nothing relevant'", () => {
      const p = parseRefineReply("[]", 4);
      expect(p).toMatchObject({ items: [], invalid: 0, usable: true });
   });

   it("is unusable when there is no list or the JSON is broken", () => {
      expect(parseRefineReply("no idea", 4).usable).toBe(false);
      expect(parseRefineReply('{"a":1}', 4).usable).toBe(false);
   });

   it("is unusable when more than half the entries are garbage", () => {
      const p = parseRefineReply(
         reply([
            { index: 0, score: "HIGH" },
            { index: 9, score: "HIGH" },
            { index: 8, score: "HIGH" },
         ]),
         3,
      );
      expect(p.usable).toBe(false);
   });

   it("clips a long reason", () => {
      const p = parseRefineReply(
         reply([{ index: 0, score: "HIGH", reason: "x".repeat(1000) }]),
         1,
      );
      expect(p.items[0].reason).toHaveLength(300);
   });

   it("reports a salvaged truncation", () => {
      const p = parseRefineReply(
         '[{"index":0,"score":"HIGH"},{"index":1,"score":"ME',
         2,
      );
      expect(p.salvaged).toBe(true);
      expect(p.items).toHaveLength(1);
   });
});

describe("parseRerankReply", () => {
   it("keeps the order and reads numeric, string and named scores", () => {
      const p = parseRerankReply(
         JSON.stringify([
            { index: 1, score: 3 },
            { index: 0, score: "2" },
            { index: 2, score: "LOW" },
         ]),
         3,
      );
      expect(p.items).toEqual([
         { index: 1, score: 3 },
         { index: 0, score: 2 },
         { index: 2, score: 1 },
      ]);
   });

   it("rejects a score outside 0-3 and a repeated index", () => {
      const p = parseRerankReply(
         JSON.stringify([
            { index: 0, score: 4 },
            { index: 1, score: 2 },
            { index: 1, score: 3 },
         ]),
         2,
      );
      expect(p.items).toEqual([{ index: 1, score: 2 }]);
      expect(p.invalid).toBe(2);
   });
});

describe("parseValueRefineReply", () => {
   it("returns levels without reasons", () => {
      const p = parseValueRefineReply(
         JSON.stringify([{ index: 0, score: "HIGH", reason: "r" }]),
         1,
      );
      expect(p.items).toEqual([{ index: 0, level: "HIGH" }]);
   });
});

describe("keyphrase and summary replies", () => {
   it("reads a single keyphrase", () => {
      expect(parseKeyphraseReply('{"keyphrase":"  order total. "}')).toBe("order total.");
      expect(parseKeyphraseReply('{"keyphrase":""}')).toBeNull();
      expect(parseKeyphraseReply("nope")).toBeNull();
   });

   it("reads a batch, skipping bad and repeated entries", () => {
      const m = parseKeyphraseBatchReply(
         JSON.stringify([
            { index: 0, keyphrase: "a" },
            { index: 1, keyphrase: "" },
            { index: 0, keyphrase: "dup" },
            { index: 7, keyphrase: "out" },
            { index: 2, keyphrase: "c" },
         ]),
         3,
      );
      expect([...m.entries()]).toEqual([
         [0, "a"],
         [2, "c"],
      ]);
   });

   it("reads a summary and collapses whitespace", () => {
      expect(
         parseSummaryReply('{"summary":"one\\n two","one_line_summary":" short. "}'),
      ).toEqual({ summary: "one two", oneLine: "short." });
      expect(parseSummaryReply('{"summary":"x"}')).toBeNull();
      expect(parseSummaryReply("[]")).toBeNull();
   });
});
