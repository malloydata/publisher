// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   buildDerivationBaseMap,
   buildSourceAliasMap,
   extractRunTargetSourceName,
   extractRunTargetSourceNames,
   locateProblemsInCallerText,
   stripMalloyCommentsAndLiterals,
} from "./query_text";

describe("service/query_text", () => {
   describe("extractRunTargetSourceName", () => {
      it("returns undefined for empty/absent text", () => {
         expect(extractRunTargetSourceName(undefined)).toBeUndefined();
         expect(extractRunTargetSourceName("")).toBeUndefined();
      });

      it("reads the source from a `run:` query", () => {
         expect(extractRunTargetSourceName("run: flights -> { ... }")).toBe(
            "flights",
         );
      });

      it("reads the source from a bare `source -> view` query", () => {
         expect(extractRunTargetSourceName("flights -> by_carrier")).toBe(
            "flights",
         );
      });

      it("unwraps a backtick-quoted (e.g. hyphenated) source name", () => {
         expect(
            extractRunTargetSourceName("run: `customer-orders` -> { ... }"),
         ).toBe("customer-orders");
         expect(extractRunTargetSourceName("`gated-source` -> view")).toBe(
            "gated-source",
         );
      });

      it("prefers the `run:` target over a leading arrow line", () => {
         expect(
            extractRunTargetSourceName("run: flights -> { aggregate: c }"),
         ).toBe("flights");
      });

      it("returns undefined when there is no run target", () => {
         expect(
            extractRunTargetSourceName("source: x is y + { dimension: a }"),
         ).toBeUndefined();
      });
   });

   describe("buildSourceAliasMap", () => {
      it("maps a single derivation declaration", () => {
         expect(buildSourceAliasMap("source: a is b")).toEqual(
            new Map([["a", "b"]]),
         );
      });

      it("maps multiple declarations", () => {
         const map = buildSourceAliasMap(
            "source: a is b\nsource: c is d\nrun: a -> view",
         );
         expect(map.get("a")).toBe("b");
         expect(map.get("c")).toBe("d");
      });

      it("unwraps backticks on either side of `is`", () => {
         const map = buildSourceAliasMap(
            "source: `my-alias` is `customer-orders`",
         );
         expect(map.get("my-alias")).toBe("customer-orders");
      });

      it("keeps the last declaration when an alias is redefined", () => {
         expect(buildSourceAliasMap("source: a is b\nsource: a is c")).toEqual(
            new Map([["a", "c"]]),
         );
      });

      it("returns an empty map when no declarations are present", () => {
         expect(buildSourceAliasMap("run: flights -> by_carrier")).toEqual(
            new Map(),
         );
      });

      it("strips its own input, so raw caller text cannot forge or erase an edge", () => {
         // The map decides which protected source's filters an ad-hoc query
         // inherits, and nothing re-checks that downstream, so both misreads
         // are silent. Asserted against raw text on purpose: the guarantee is
         // the function's, not the caller's.

         // A declaration inside a literal is not a declaration. Read raw, this
         // one is last-wins and REPLACES the real base.
         expect(
            buildSourceAliasMap(
               "source: a is protected extend {\n" +
                  "  dimension: note is 'source: a is unprotected'\n" +
                  "}",
            ).get("a"),
         ).toBe("protected");

         // A comment does not break a declaration the compiler reads around.
         expect(
            buildSourceAliasMap("source: a is -- c\n protected").get("a"),
         ).toBe("protected");
         expect(
            buildSourceAliasMap("source: a is /* c */ protected").get("a"),
         ).toBe("protected");
      });

      it("is unchanged by a caller that already stripped", () => {
         // Stripping blanks to spaces, so it is idempotent -- which is what
         // makes the internal strip free for the caller that already does it.
         const raw =
            "source: a is protected extend { dimension: n is 'source: a is x' }";
         expect(
            buildSourceAliasMap(stripMalloyCommentsAndLiterals(raw)),
         ).toEqual(buildSourceAliasMap(raw));
      });

      it("reads the declaration shapes the compiler links", () => {
         // Each of these is legal grammar whose edge a narrower pattern
         // declined to link, and a missing edge is silent in the unsafe
         // direction: `resolveFilterSource` returns undefined, no filter is
         // injected, and the caller gets unfiltered rows with no error.
         expect(
            buildSourceAliasMap("source: mine is (protected extend {})").get(
               "mine",
            ),
         ).toBe("protected");
         expect(
            buildSourceAliasMap("source: mine(p::string) is protected").get(
               "mine",
            ),
         ).toBe("protected");
         // `\w` is ASCII-only, so a non-ASCII identifier matched nothing.
         expect(
            buildSourceAliasMap("source: café is protected").get("café"),
         ).toBe("protected");
      });
   });

   describe("stripMalloyCommentsAndLiterals", () => {
      const strip = stripMalloyCommentsAndLiterals;

      it("blanks a `--` line comment but keeps the newline", () => {
         expect(strip("run: a -- hide\nrun: b")).toBe("run: a        \nrun: b");
      });

      it("is idempotent across every span it recognises", () => {
         // `buildSourceAliasMap` strips its own input, and the argument that
         // this costs an already-stripping caller nothing rests on running it
         // twice being the same as running it once. Asserted directly rather
         // than through a map comparison, where both sides strip and the
         // property holds whether or not it is true.
         for (const source of [
            "source: a is protected extend { dimension: n is 'source: a is x' }",
            "where: s = 'it''s escaped' and t = \"double\"",
            "source: `quoted name` is protected -- trailing\n run: x",
            "a /* unterminated",
            "a -- unterminated",
            "where: s = 'unterminated",
            "run: x // one\n/* two */ run: y -- three",
         ]) {
            expect(strip(strip(source))).toBe(strip(source));
         }
      });

      it("blanks `//` and block comments, including an unterminated one", () => {
         expect(strip("a // x\nb")).toBe("a     \nb");
         expect(strip("a /* x */ b")).toBe("a         b");
         expect(strip("a /* x")).toBe("a     ");
      });

      it("blanks a string-literal body but keeps both delimiters", () => {
         expect(strip("where: s = 'run: hidden'")).toBe(
            "where: s = '           '",
         );
         expect(strip('where: s = "run: hidden"')).toBe(
            'where: s = "           "',
         );
      });

      it("does not treat `--` inside a literal as a comment, nor a quote inside a comment as a literal", () => {
         // If the `--` were read as a comment the trailing `x` would vanish.
         expect(strip("s = '-- not a comment' and x")).toBe(
            "s = '                ' and x",
         );
         // An apostrophe inside a comment must not open a literal that then
         // swallows the following line.
         expect(strip("-- don't\nrun: a")).toBe("        \nrun: a");
      });

      it("preserves backtick-quoted identifiers, which carry real names", () => {
         expect(strip("source: `my-src` is X")).toBe("source: `my-src` is X");
      });

      it("skips a backtick span WHOLE — a comment or quote character inside a legal identifier is not syntax", () => {
         // Malloy lexes a backticked identifier as one token. Scanning inside
         // one let each of these blank the declarations that follow, which was
         // a bypass; the whole tail must survive untouched.
         for (const name of ["a'", 'a"', "z--q", "z//q", "z/*q"]) {
            const t = `source: \`${name}\` is X\nrun: mine -> { select: id }`;
            expect(strip(t)).toBe(t);
         }
      });

      it("does not let a backticked FIELD name erase later declarations", () => {
         const t =
            "source: probe is Open extend { dimension: `q'` is 1 }\nsource: mine is X extend { except: authorized }";
         expect(strip(t)).toBe(t);
      });

      it("stops at an unterminated backtick rather than blanking the rest", () => {
         expect(strip("source: `oops is X")).toBe("source: `oops is X");
      });

      it("does not shift any offset — output length always matches input", () => {
         for (const t of [
            "run: a -- c\nrun: b",
            "s = 'x' /* y */ z",
            "-- only a comment",
            "source: `q` is X",
         ]) {
            expect(strip(t).length).toBe(t.length);
         }
      });
   });

   describe("buildDerivationBaseMap", () => {
      it("collects `source:` and `query:` declarations alike", () => {
         expect(
            buildDerivationBaseMap("source: a is b\nquery: q is a -> { x }"),
         ).toEqual(
            new Map([
               ["a", new Set(["b"])],
               ["q", new Set(["a"])],
            ]),
         );
      });

      it("keeps EVERY base declared for a name, not the last", () => {
         // Load-bearing: a second declaration of a name must be able to add a
         // base to check, never to replace the real one.
         expect(
            buildDerivationBaseMap("source: a is gated\nsource: a is open"),
         ).toEqual(new Map([["a", new Set(["gated", "open"])]]));
      });

      it("reads backtick-quoted names on both sides", () => {
         expect(
            buildDerivationBaseMap("source: `my-src` is `their-src`"),
         ).toEqual(new Map([["my-src", new Set(["their-src"])]]));
      });

      it("links a declaration a comment split, whoever stripped it", () => {
         // This is the shape that evaded a scan: a comment between `is` and the
         // base, which the compiler reads around. The edge is linked whether the
         // caller stripped or not, because the function strips its own input --
         // and both spellings agree, which is the idempotence the change rests
         // on.
         const text = "source: mine is -- c\n X extend { except: g }";
         expect(buildDerivationBaseMap(text).get("mine")).toEqual(
            new Set(["X"]),
         );
         expect(
            buildDerivationBaseMap(stripMalloyCommentsAndLiterals(text)).get(
               "mine",
            ),
         ).toEqual(new Set(["X"]));
      });

      it("reads a non-ASCII identifier", () => {
         expect(buildDerivationBaseMap("source: café is X")).toEqual(
            new Map([["café", new Set(["X"])]]),
         );
      });

      it("reads a parenthesised base and a parameter list on the name", () => {
         expect(
            buildDerivationBaseMap("source: mine is (X extend { except: g })"),
         ).toEqual(new Map([["mine", new Set(["X"])]]));
         expect(
            buildDerivationBaseMap("source: mine(p::string) is Open"),
         ).toEqual(new Map([["mine", new Set(["Open"])]]));
      });

      it("does not read a forged declaration out of a string literal", () => {
         const text = "run: mine -> { where: s = 'source: mine is open' }";
         expect(
            buildDerivationBaseMap(stripMalloyCommentsAndLiterals(text)).size,
         ).toBe(0);
      });

      it("does not let a literal re-point a name away from its real base", () => {
         // The two hardenings that separate this map from buildSourceAliasMap,
         // stated against the exact text that defeated that one: a real
         // derivation from `hidden`, plus a literal spelling a derivation from
         // `curated`. Last-wins over raw text resolved `mine` to `curated`,
         // which is what a boundary keyed on this map would have admitted.
         // Here `mine` must still resolve to `hidden` and nothing else.
         const text =
            "source: mine is hidden extend {\n" +
            "  dimension: note is 'source: mine is curated'\n" +
            "}\nrun: mine -> { group_by: note }";
         expect(
            buildDerivationBaseMap(stripMalloyCommentsAndLiterals(text)).get(
               "mine",
            ),
         ).toEqual(new Set(["hidden"]));
      });
   });

   describe("extractRunTargetSourceNames", () => {
      it("reads EVERY `run:` target in order, not just the first", () => {
         // Malloy runs the last statement; a check over the first alone would
         // let a leading decoy stand in for the statement that executes.
         expect(
            extractRunTargetSourceNames(
               "run: customers -> { aggregate: total }\nrun: helper -> { group_by: id }",
            ),
         ).toEqual(["customers", "helper"]);
      });

      it("reads a run target that is an expression over the name", () => {
         expect(
            extractRunTargetSourceNames(
               "run: customers extend { join_one: h is helper on id = h.id } -> { group_by: h.id }",
            ),
         ).toEqual(["customers"]);
      });

      it("reads backtick-quoted targets and the bare leading-arrow form", () => {
         expect(
            extractRunTargetSourceNames(
               "run: `customer-orders` -> { group_by: id }\nflights -> by_carrier",
            ),
         ).toEqual(["customer-orders", "flights"]);
      });

      it("ignores a `run:` inside a comment or a string literal", () => {
         expect(
            extractRunTargetSourceNames(
               "// run: helper -> { group_by: id }\n" +
                  "source: mine is customers extend { dimension: s is 'run: helper' }\n" +
                  "run: mine -> { group_by: s }",
            ),
         ).toEqual(["mine"]);
      });

      it("is undefined when a `run:` has a target it cannot read as a name", () => {
         // Refusing is the safe answer: a caller that needs every target must
         // not check a subset because one was unreadable.
         expect(
            extractRunTargetSourceNames(
               "run: customers -> { aggregate: total }\nrun: (helper) -> { group_by: id }",
            ),
         ).toBeUndefined();
      });

      it("returns no targets for text with no statement to run", () => {
         expect(
            extractRunTargetSourceNames("source: mine is customers extend {}"),
         ).toEqual([]);
      });
   });

   describe("locateProblemsInCallerText", () => {
      const at = (url: string, startLine: number, endLine = startLine) => ({
         url,
         range: {
            start: { line: startLine, character: 4 },
            end: { line: endLine, character: 8 },
         },
      });
      const problem = (location?: ReturnType<typeof at>) => ({
         message: "'nope' is not defined",
         severity: "error" as const,
         code: "field-not-found",
         ...(location ? { at: location } : {}),
      });

      it("moves a problem in the compiled document back by the prefix's lines", () => {
         const [located] = locateProblemsInCallerText(
            [problem(at("internal://query/0f1e", 2))],
            "source: x is customers extend {}\nrun: x -> { group_by: nope }",
            1,
         );
         expect(located.at?.range).toEqual({
            start: { line: 1, character: 4 },
            end: { line: 1, character: 8 },
         });
      });

      it("drops the location of a problem outside the caller's lines", () => {
         // Line 0 is the prefix; line 2 is a refinement the server appended
         // after the caller's one-line text (an injected source filter).
         const located = locateProblemsInCallerText(
            [
               problem(at("internal://query/0f1e", 0)),
               problem(at("internal://query/0f1e", 2)),
            ],
            "run: customers -> { group_by: id }",
            1,
         );
         expect(located.map((p) => p.at)).toEqual([undefined, undefined]);
         expect(located.map((p) => p.message)).toEqual([
            "'nope' is not defined",
            "'nope' is not defined",
         ]);
      });

      it("drops the location of a problem in another document", () => {
         const [located] = locateProblemsInCallerText(
            [problem(at("file:///pkg/index.malloy", 1))],
            "run: customers -> { group_by: id }\n",
            1,
         );
         expect(located.at).toBeUndefined();
      });

      it("keeps a problem that never had a location", () => {
         expect(
            locateProblemsInCallerText([problem()], "run: customers -> {}", 1),
         ).toEqual([problem()]);
      });
   });
});
