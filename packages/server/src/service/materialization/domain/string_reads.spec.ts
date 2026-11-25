// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `unboundStringReadsRefusal`: a `storage=` build over a `#(materialize)`
 * upstream whose table is not bound yet is judged at plan by how that
 * upstream's source compares each string it reads live, mapped to the
 * contract's terms as the build's contract records them (`liveCompare`):
 * binary compares `exact`, deterministic `equality`, anything else `none`.
 * A capability not read yet, and a field it does not list, refuse.
 */

import { describe, expect, it } from "bun:test";
import type { Collation } from "./collation";
import { liveCompare } from "./column_contract";
import {
   capabilityFingerprint,
   type StringReads,
   unboundStringReadsRefusal,
   type UpstreamStringCapability,
} from "./string_reads";

const reads = (
   need: "exact" | "equality",
   names: string[] = ["cat"],
   upstream = "m",
): StringReads => ({ reads: [{ upstream, names, need }] });

const read = (fields: Record<string, Collation>): UpstreamStringCapability => ({
   fields: new Map(Object.entries(fields)),
});

const unbound = (capability: UpstreamStringCapability) =>
   new Map([["m", { name: "m", capability }]]);

describe("liveCompare", () => {
   it("maps a live collation to the compare a byte-exact table records", () => {
      expect(liveCompare("binary")).toBe("exact");
      expect(liveCompare("deterministic")).toBe("equality");
      expect(liveCompare("unknown")).toBe("none");
      expect(liveCompare(undefined)).toBe("none");
   });
});

describe("unboundStringReadsRefusal", () => {
   it("keeps an order or a group of a binary column", () => {
      const binary = unbound(read({ cat: "binary" }));
      expect(unboundStringReadsRefusal(reads("exact"), binary)).toBeUndefined();
      expect(
         unboundStringReadsRefusal(reads("equality"), binary),
      ).toBeUndefined();
   });

   it("keeps a group of a deterministic column, and refuses its order", () => {
      const det = unbound(read({ cat: "deterministic" }));
      expect(unboundStringReadsRefusal(reads("equality"), det)).toBeUndefined();
      expect(unboundStringReadsRefusal(reads("exact"), det)).toBe(
         "it orders 'cat' from 'm', whose table is not bound yet and whose " +
            "source compares it by equality only",
      );
   });

   it("refuses any read of a nondeterministic or unlisted column", () => {
      expect(
         unboundStringReadsRefusal(
            reads("equality"),
            unbound(read({ cat: "unknown" })),
         ),
      ).toBe(
         "it groups or compares 'cat' from 'm', whose table is not bound yet " +
            "and whose source compares it unlike byte order",
      );
      expect(
         unboundStringReadsRefusal(reads("equality"), unbound(read({}))),
      ).toContain("compares it unlike byte order");
   });

   it("refuses while the capability has not been read", () => {
      expect(
         unboundStringReadsRefusal(reads("equality"), unbound("unread")),
      ).toBe(
         "it groups or compares 'cat' from 'm', whose table is not bound yet " +
            "and whose source's string comparison has not been read yet",
      );
   });

   it("reads a column by the last name it goes by that the source lists", () => {
      const cap = unbound(read({ cat: "binary" }));
      expect(
         unboundStringReadsRefusal(reads("exact", ["cat", "label"]), cap),
      ).toBeUndefined();
   });

   it("judges only reads from an upstream it names, and nothing refused", () => {
      expect(
         unboundStringReadsRefusal(
            reads("exact", ["cat"], "other"),
            unbound("unread"),
         ),
      ).toBeUndefined();
      expect(
         unboundStringReadsRefusal({ refused: "x" }, unbound("unread")),
      ).toBeUndefined();
   });
});

describe("capabilityFingerprint", () => {
   it("is stable under field order, and differs when a judgement would", () => {
      const a = capabilityFingerprint(
         read({ cat: "deterministic", cc: "binary" }),
      );
      expect(
         capabilityFingerprint(read({ cc: "binary", cat: "deterministic" })),
      ).toBe(a);
      expect(
         capabilityFingerprint(read({ cat: "binary", cc: "binary" })),
      ).not.toBe(a);
      expect(capabilityFingerprint("unread")).not.toBe(a);
      expect(capabilityFingerprint("bound")).not.toBe(
         capabilityFingerprint("unread"),
      );
   });
});
