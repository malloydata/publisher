// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   extractSourceAccess,
   filteredEmptyCaption,
   isRestricted,
   isRowFiltered,
   refusalMessage,
   restrictedToWords,
} from "./sourceAccess";

const gate = (...authorize: string[]) => ({ authorize, accessFilter: [] });

describe("isRestricted", () => {
   it('reads the sentinels element-wise: only an all-"true" list is unlocked', () => {
      expect(isRestricted(gate())).toBe(false);
      expect(isRestricted(gate("true"))).toBe(false);
      expect(isRestricted(gate("true", "'finance' in $GROUPS"))).toBe(true);
      expect(isRestricted(gate("false"))).toBe(true);
   });
});

describe("isRowFiltered", () => {
   it("is any access filter at all", () => {
      expect(isRowFiltered({ authorize: [], accessFilter: [] })).toBe(false);
      expect(
         isRowFiltered({ authorize: [], accessFilter: ["region in $GROUPS"] }),
      ).toBe(true);
   });
});

describe("restrictedToWords", () => {
   it("names the groups a membership rule admits", () => {
      expect(restrictedToWords(gate("'finance' in $GROUPS"))).toBe(
         "members of finance",
      );
      expect(restrictedToWords(gate(`"hr" in $GROUPS`))).toBe("members of hr");
      expect(
         restrictedToWords(
            gate("'a' in $GROUPS or 'b' in $GROUPS or 'c' in $GROUPS"),
         ),
      ).toBe("members of a, b or c");
      expect(restrictedToWords(gate("'a' in $GROUPS and 'b' in $GROUPS"))).toBe(
         "members of all of a, b",
      );
   });

   it("says no one for the false sentinel, and never guesses at another given", () => {
      expect(restrictedToWords(gate("false"))).toBe("no one");
      expect(restrictedToWords(gate("$ROLE = 'analyst'"))).toBe(
         "readers its access rule admits",
      );
   });

   it("joins several rules, which all apply, and skips the true sentinel", () => {
      expect(
         restrictedToWords(
            gate("true", "'finance' in $GROUPS", "'eu' in $GROUPS"),
         ),
      ).toBe("members of finance, and members of eu");
   });
});

describe("refusalMessage", () => {
   it("says the reader can't access the source, the rule in words, and the way out", () => {
      expect(refusalMessage("payroll", gate("'finance' in $GROUPS"))).toBe(
         "You can't access payroll. It is restricted to members of finance; ask an administrator of this package for access.",
      );
   });
});

describe("extractSourceAccess", () => {
   it("ignores a body with no sources and malformed entries", () => {
      expect(extractSourceAccess({}).size).toBe(0);
      expect(
         extractSourceAccess({
            sources: [
               null,
               { authorize: ["x"] },
               { name: "a", authorize: [1, ""] },
            ],
         }).size,
      ).toBe(0);
   });
});

describe("a refusal on a source with no rule this page knows", () => {
   it("still says the reader can't access it, with the way out", () => {
      expect(
         refusalMessage("orders", { authorize: [], accessFilter: [] }),
      ).toBe(
         "You can't access orders. Ask an administrator of this package for access.",
      );
      expect(
         refusalMessage(undefined, { authorize: [], accessFilter: [] }),
      ).toBe(
         "You don't have access to run this. Ask an administrator of this package for access.",
      );
   });
});

describe("filteredEmptyCaption", () => {
   const empty = JSON.stringify({
      data: { kind: "array_cell", array_value: [] },
   });
   const oneRow = JSON.stringify({
      data: {
         kind: "array_cell",
         array_value: [{ kind: "record_cell", record_value: [] }],
      },
   });
   const filtered = { authorize: [], accessFilter: ["region in $GROUPS"] };

   it("explains an empty result on a row-filtered source", () => {
      expect(filteredEmptyCaption(filtered, empty)).toBe(
         "Filtered to your rows: this source shows each reader only the rows they're allowed to see.",
      );
   });

   it("says nothing for rows, for an unfiltered source, or for no result", () => {
      expect(filteredEmptyCaption(filtered, oneRow)).toBeUndefined();
      expect(
         filteredEmptyCaption({ authorize: [], accessFilter: [] }, empty),
      ).toBeUndefined();
      expect(filteredEmptyCaption(filtered, undefined)).toBeUndefined();
      expect(filteredEmptyCaption(filtered, "not json")).toBeUndefined();
   });
});
