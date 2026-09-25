// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { derivationTerminals } from "./caller_joins";
import {
   buildDerivationBaseMap,
   buildIsEdgeMap,
   buildJoinBaseMap,
   buildSourceAliasMap,
   extractRunTargetSourceName,
   stripMalloyCommentsAndLiterals,
   UNREADABLE_BASE,
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

      it("reads `run:` in any keyword case, keeping the name's own case", () => {
         // Malloy keywords are case-insensitive; a lowercase-only reader let
         // `RUN:` skip every pre-compile check keyed on the run target.
         expect(extractRunTargetSourceName("RUN: Flights -> { x }")).toBe(
            "Flights",
         );
         expect(extractRunTargetSourceName("Run: flights -> { x }")).toBe(
            "flights",
         );
         expect(extractRunTargetSourceName("rUn :`my-src` -> { x }")).toBe(
            "my-src",
         );
      });

      it("reads a non-ASCII run target", () => {
         expect(extractRunTargetSourceName("run: café -> { x }")).toBe("café");
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

   describe("buildSourceAliasMap keyword case", () => {
      it("reads `SOURCE:` / `IS` in any case", () => {
         expect(buildSourceAliasMap("SOURCE: a IS b")).toEqual(
            new Map([["a", "b"]]),
         );
         expect(buildSourceAliasMap("Source: A Is B extend {}")).toEqual(
            new Map([["A", "B"]]),
         );
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

      it("reads `SOURCE:` / `QUERY:` / `IS` in any case", () => {
         expect(
            buildDerivationBaseMap(
               "SOURCE: a IS b EXTEND {}\nQuery: q iS a -> { x }",
            ),
         ).toEqual(
            new Map([
               ["a", new Set(["b"])],
               ["q", new Set(["a"])],
            ]),
         );
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

   describe("buildJoinBaseMap", () => {
      const bases = (text: string, alias: string) =>
         buildJoinBaseMap(text).get(alias);

      it("reads an aliased join and a shorthand join", () => {
         const map = buildJoinBaseMap(
            "run: s extend { join_one: g is gated on id = g.id; join_many: helper on id = helper.id } -> { group_by: g.x }",
         );
         expect(map.get("g")).toEqual(new Set(["gated"]));
         expect(map.get("helper")).toEqual(new Set(["helper"]));
      });

      it("reads JOIN_ONE: and Join_Cross: in any case", () => {
         expect(
            bases("RUN: s EXTEND { JOIN_ONE: g IS gated ON id = g.id }", "g"),
         ).toEqual(new Set(["gated"]));
         expect(bases("run: s extend { Join_Cross: c is gated }", "c")).toEqual(
            new Set(["gated"]),
         );
      });

      it("reads the base of an inline extend, and not the fields inside it", () => {
         const map = buildJoinBaseMap(
            "run: s extend { join_one: e is gated extend { dimension: z is name } on id = e.id }",
         );
         expect(map.get("e")).toEqual(new Set(["gated"]));
         expect(map.has("z")).toBe(false);
      });

      it("never maps an aliased item to itself as if it were shorthand", () => {
         expect(
            bases(
               "run: s extend { join_one: e is gated extend {} on id = e.id }",
               "e",
            ),
         ).toEqual(new Set(["gated"]));
         // A base the scan cannot read is an unreadable edge, never `e -> e`.
         expect(bases("run: s extend { join_one: e is 'x' }", "e")).toEqual(
            new Set([UNREADABLE_BASE]),
         );
      });

      it("reads a parenthesized base and a backtick-quoted alias", () => {
         expect(
            bases(
               "run: s extend { join_one: `my-e` is ((gated extend {})) on id = `my-e`.id }",
               "my-e",
            ),
         ).toEqual(new Set(["gated"]));
      });

      it("reads later items separated by a comma or by nothing", () => {
         const map = buildJoinBaseMap(
            "run: s extend { join_one: a is x with k, b is y on id = b.id c is z extend {} on id = c.id }",
         );
         expect(map.get("a")).toEqual(new Set(["x"]));
         expect(map.get("b")).toEqual(new Set(["y"]));
         expect(map.get("c")).toEqual(new Set(["z"]));
      });

      it("reads annotations before an item and on either side of is", () => {
         expect(
            bases(
               "run: s extend { join_one:\n # tag\n e # x\n is # y\n gated extend {} on id = e.id }",
               "e",
            ),
         ).toEqual(new Set(["gated"]));
      });

      it("ignores comments and literals, and reads through a comment before the base", () => {
         const map = buildJoinBaseMap(
            "-- join_one: fake is forged\nrun: s -> { where: n = 'join_one: lit is forged'; join_one: g is -- c\n gated on id = g.id }",
         );
         expect(map.has("fake")).toBe(false);
         expect(map.has("lit")).toBe(false);
         expect(map.get("g")).toEqual(new Set(["gated"]));
      });

      it("keeps every base of a repeated alias", () => {
         expect(
            bases(
               "source: a is s extend { join_one: e is x on id = e.id }\nrun: s extend { join_one: e is y on id = e.id }",
               "e",
            ),
         ).toEqual(new Set(["x", "y"]));
      });

      it("stops a statement at the next keyword", () => {
         const map = buildJoinBaseMap(
            "run: s extend { join_one: g is gated on id = g.id\n dimension: d is name }",
         );
         expect(map.has("d")).toBe(false);
      });
   });

   describe("buildIsEdgeMap", () => {
      it("reads every NAME is BASE edge, including a join item no statement scan reads", () => {
         const map = buildIsEdgeMap(
            "run: s extend { join_one: a is x on f(a.id, 1) = 1 e is gated extend {} on id = e.id }",
         );
         expect(map.get("e")).toEqual(new Set(["gated"]));
         expect(map.get("a")).toEqual(new Set(["x"]));
      });

      it("is not blanked by a phantom literal an annotation opens", () => {
         const map = buildIsEdgeMap(
            "run: s extend {\n # don't\n join_one: e is rowgated extend {} on id = e.id\n dimension: q is 'x' }",
         );
         expect(map.get("e")).toEqual(new Set(["rowgated"]));
      });

      it("reads through comments and annotations around is", () => {
         expect(
            buildIsEdgeMap(
               "join_one: e -- a\n # b\n is /* c */ (gated extend {})",
            ).get("e"),
         ).toEqual(new Set(["gated"]));
      });

      it("does not backtrack exponentially on crafted trivia", () => {
         const text =
            "join_one: e " + "# a -- b // c -- d\n".repeat(64) + "isnt";
         const start = performance.now();
         buildIsEdgeMap(text);
         buildJoinBaseMap(text);
         expect(performance.now() - start).toBeLessThan(1000);
      });
   });

   describe("what the text readers cannot be fed", () => {
      it("blanks a # annotation to end of line, outside strings and backticks", () => {
         expect(
            stripMalloyCommentsAndLiterals("# source: a is b\nsource: c is d"),
         ).toBe("                \nsource: c is d");
         expect(stripMalloyCommentsAndLiterals("dimension: `a#b` is 1")).toBe(
            "dimension: `a#b` is 1",
         );
         expect(stripMalloyCommentsAndLiterals("where: n = '#x'\ny")).toBe(
            "where: n = '  '\ny",
         );
      });

      it("reads a base the annotations around is used to hide", () => {
         expect(
            buildDerivationBaseMap(
               "# source: mine is plain\nsource: mine is\n# note\ngated extend {}",
            ).get("mine"),
         ).toEqual(new Set(["gated"]));
      });

      it("ignores a declaration, a join, or an edge spelled inside a backtick name", () => {
         expect(
            buildDerivationBaseMap(
               "source: mine is ((gated)) extend { dimension: `source: mine is plain` is 1 }",
            ).get("mine"),
         ).toEqual(new Set(["gated"]));
         expect(
            buildJoinBaseMap(
               "run: s extend { dimension: `join_one: g is plain` is 1 }",
            ).has("g"),
         ).toBe(false);
         expect(
            buildIsEdgeMap("dimension: `mine is plain` is 1").has("mine"),
         ).toBe(false);
      });

      it("reads every item of a source: statement", () => {
         expect(
            buildDerivationBaseMap(
               "source: a is plain extend {} mine is gated extend {}",
            ).get("mine"),
         ).toEqual(new Set(["gated"]));
      });

      it("leaves a name whose real base it cannot read unproven beside a forged one", () => {
         const map = buildDerivationBaseMap(
            "source: mine is 'x'\nsource: mine is plain",
         );
         expect(
            derivationTerminals("mine", map, (name) => name === "plain").proven,
         ).toBe(false);
      });

      it("is linear on a 1MB adversarial body", () => {
         const dashes = "a -- ".repeat(100_000);
         const hashes = "a # ".repeat(125_000);
         const text = `run: plain extend { join_one: e is plain extend {} on id = e.id } -> { where: name = '${dashes}'; group_by: e.id }\n# ${hashes}\n`;
         expect(text.length).toBeGreaterThan(1_000_000);
         const start = performance.now();
         stripMalloyCommentsAndLiterals(text);
         buildDerivationBaseMap(text);
         buildJoinBaseMap(text);
         buildIsEdgeMap(text);
         expect(performance.now() - start).toBeLessThan(500);
      });
   });
});
