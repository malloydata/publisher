// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, it } from "bun:test";
import { documentPreamble, isForbidden, withPreamble } from "./textSource";

it("builds the preamble from definition cells only, in order, leaving out restricted ones", () => {
   expect(
      documentPreamble([
         { type: "code", kind: "definition", text: "source: a is orders" },
         { type: "markdown", kind: "markdown", text: "prose" },
         { type: "code", kind: "query", text: "run: a -> { select: * }" },
         {
            type: "code",
            kind: "definition",
            text: "source: b is secret",
            restricted: true,
         },
         { type: "code", kind: "definition", text: "source: c is a" },
      ]),
   ).toBe("source: a is orders\n\nsource: c is a");
   expect(documentPreamble(undefined)).toBe("");
});

it("puts the preamble ahead of the run, and adds nothing when there is none", () => {
   expect(withPreamble("source: a is orders", "run: a -> v")).toBe(
      "source: a is orders\n\nrun: a -> v",
   );
   expect(withPreamble("", "run: a -> v")).toBe("run: a -> v");
});

it("reads a 403 off the API error and nothing else", () => {
   expect(isForbidden({ status: 403 })).toBe(true);
   expect(isForbidden({ status: 404 })).toBe(false);
   expect(isForbidden(undefined)).toBe(false);
});
