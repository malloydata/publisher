import { describe, expect, it } from "bun:test";
import {
   buildDerivationBaseMap,
   buildSourceAliasMap,
   extractRunTargetSourceName,
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
   });

   describe("stripMalloyCommentsAndLiterals", () => {
      const strip = stripMalloyCommentsAndLiterals;

      it("blanks a `--` line comment but keeps the newline", () => {
         expect(strip("run: a -- hide\nrun: b")).toBe("run: a        \nrun: b");
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

      it("links a declaration a comment split, once the comment is stripped", () => {
         const text = "source: mine is -- c\n X extend { except: g }";
         // The raw text does not link (this is the shape that evaded a scan);
         // stripped, it does.
         expect(buildDerivationBaseMap(text).get("mine")).toBeUndefined();
         expect(
            buildDerivationBaseMap(stripMalloyCommentsAndLiterals(text)).get(
               "mine",
            ),
         ).toEqual(new Set(["X"]));
      });

      it("does not read a forged declaration out of a string literal", () => {
         const text = "run: mine -> { where: s = 'source: mine is open' }";
         expect(
            buildDerivationBaseMap(stripMalloyCommentsAndLiterals(text)).size,
         ).toBe(0);
      });
   });
});
