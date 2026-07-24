import { describe, expect, test } from "bun:test";
import { MalloyTranslator } from "@malloydata/malloy";
import { ScaffoldError } from "./errors";
import {
   preview,
   printable,
   toMalloyIdentifier,
   validatePackageName,
} from "./names";

/**
 * The keyword table and the standard function table are the machine-readable
 * statement of what a source cannot be named, but neither is on the package's
 * exports map, so reach them through the resolved package root. Re-deriving them
 * by hand would drift in exactly the way these tests exist to catch.
 */
const malloyDist = new URL(
   "./dist/",
   import.meta.resolve("@malloydata/malloy/package.json"),
);

const { MALLOY_STANDARD_FUNCTIONS } = (await import(
   new URL("dialect/functions/malloy_standard_functions.js", malloyDist).href
)) as { MALLOY_STANDARD_FUNCTIONS: Record<string, unknown> };

const { MalloyLexer } = (await import(
   new URL("lang/lib/Malloy/MalloyLexer.js", malloyDist).href
)) as { MalloyLexer: { ruleNames: string[] } };

/** Whether `name` survives the parser as a bare source name. */
function parsesAsSourceName(name: string): boolean {
   const url = "file://drift.malloy";
   const translator = new MalloyTranslator(url, null, {
      urls: { [url]: `source: ${name} is _db_.table("t")` },
   });
   const response = translator.translate();
   return !(response.problems ?? []).some(
      (problem) => problem.severity === "error",
   );
}

describe("validatePackageName", () => {
   test("accepts ordinary names", () => {
      for (const name of ["sales", "retail-sales", "my_data", "a.b", "pkg1"]) {
         expect(() => validatePackageName(name)).not.toThrow();
      }
   });

   test("rejects names that start with a dot", () => {
      expect(() => validatePackageName(".hidden")).toThrow(ScaffoldError);
   });

   test('rejects "." and ".."', () => {
      expect(() => validatePackageName(".")).toThrow(ScaffoldError);
      expect(() => validatePackageName("..")).toThrow(ScaffoldError);
   });

   test("rejects path separators and spaces", () => {
      expect(() => validatePackageName("a/b")).toThrow(ScaffoldError);
      expect(() => validatePackageName("a b")).toThrow(ScaffoldError);
   });

   test("rejects the empty string", () => {
      expect(() => validatePackageName("")).toThrow(ScaffoldError);
   });

   test("accepts a 248-char name but rejects 249", () => {
      // 248 + ".malloy" is exactly the 255-character filename limit. A longer
      // name used to pass here and then die on ENAMETOOLONG from writeFileSync,
      // leaving the package directory behind.
      expect(() => validatePackageName("a".repeat(248))).not.toThrow();
      expect(() => validatePackageName("a".repeat(249))).toThrow(ScaffoldError);
   });

   test("counts the characters the derived source name adds", () => {
      // A leading digit gains an "_" prefix, so 248 digits needs a 256-character
      // model file: rejected for the file it would have to write, not its length.
      expect(() => validatePackageName("1".repeat(248))).toThrow(ScaffoldError);
      expect(() => validatePackageName("1".repeat(247))).not.toThrow();
      // Every name the validator accepts must yield a writable model file.
      for (const name of ["a".repeat(248), "1".repeat(247), "count", "sales"]) {
         expect(
            `${toMalloyIdentifier(name)}.malloy`.length,
         ).toBeLessThanOrEqual(255);
      }
   });
});

describe("toMalloyIdentifier", () => {
   test("leaves a plain name unchanged", () => {
      expect(toMalloyIdentifier("sales")).toBe("sales");
   });

   test("replaces hyphens and dots with underscores", () => {
      expect(toMalloyIdentifier("retail-sales")).toBe("retail_sales");
      expect(toMalloyIdentifier("a.b")).toBe("a_b");
   });

   test("prefixes an underscore when it would start with a digit", () => {
      expect(toMalloyIdentifier("2024-data")).toBe("_2024_data");
   });

   test("always yields a valid Malloy identifier", () => {
      for (const name of ["sales", "retail-sales", "2024", "a.b.c", "x-1"]) {
         expect(toMalloyIdentifier(name)).toMatch(/^[A-Za-z_][A-Za-z0-9_]*$/);
      }
   });

   test("suffixes a Malloy reserved word so the source name is usable", () => {
      expect(toMalloyIdentifier("count")).toBe("count_source");
      expect(toMalloyIdentifier("table")).toBe("table_source");
      expect(toMalloyIdentifier("date")).toBe("date_source");
      // The suffix is applied case-insensitively.
      expect(toMalloyIdentifier("Sum")).toBe("Sum_source");
   });

   test("suffixes a Malloy standard function name", () => {
      // These fail only once a dialect is attached ("Cannot redefine 'lead'"),
      // so the scaffold used to emit them and the server refused the package.
      expect(toMalloyIdentifier("lead")).toBe("lead_source");
      expect(toMalloyIdentifier("log")).toBe("log_source");
      expect(toMalloyIdentifier("rank")).toBe("rank_source");
      expect(toMalloyIdentifier("replace")).toBe("replace_source");
   });

   test("suffixes a keyword the parser rejects outright", () => {
      expect(toMalloyIdentifier("public")).toBe("public_source");
      expect(toMalloyIdentifier("internal")).toBe("internal_source");
      expect(toMalloyIdentifier("export")).toBe("export_source");
      expect(toMalloyIdentifier("cast")).toBe("cast_source");
   });

   test("leaves non-reserved contextual keywords alone", () => {
      // These compile fine as source names, so they should not be suffixed.
      for (const name of ["orders", "order", "view", "select", "group"]) {
         expect(toMalloyIdentifier(name)).toBe(name);
      }
   });
});

/**
 * The reserved list is a snapshot of two tables inside @malloydata/malloy, so a
 * Malloy upgrade can silently re-break it. These re-derive both tables from the
 * installed package and fail with the words to add, rather than letting the
 * scaffolder ship a package the server cannot compile.
 */
describe("MALLOY_RESERVED drift against the installed @malloydata/malloy", () => {
   test("covers every Malloy standard function name", () => {
      const functionNames = Object.keys(MALLOY_STANDARD_FUNCTIONS);
      // A sweep that found nothing would pass vacuously.
      expect(functionNames.length).toBeGreaterThan(50);

      const uncovered = functionNames.filter(
         (name) => toMalloyIdentifier(name) !== `${name}_source`,
      );
      expect(uncovered).toEqual([]);
   });

   test("covers every lexer word the parser refuses as a source name", () => {
      const candidates = [
         ...new Set(MalloyLexer.ruleNames.map((rule) => rule.toLowerCase())),
      ].filter((word) => /^[a-z_][a-z0-9_]*$/.test(word));

      const rejected = candidates.filter((word) => !parsesAsSourceName(word));
      // Guards the sweep itself: if the probe stopped detecting errors this
      // would collapse to zero and the test would pass having checked nothing.
      expect(rejected.length).toBeGreaterThan(50);

      const uncovered = rejected.filter(
         (word) => toMalloyIdentifier(word) !== `${word}_source`,
      );
      expect(uncovered).toEqual([]);
   });

   test("does not suffix lexer words that are legal source names", () => {
      // False positives cost the user a confusing rename, so keep the list tight.
      const candidates = [
         ...new Set(MalloyLexer.ruleNames.map((rule) => rule.toLowerCase())),
      ].filter((word) => /^[a-z_][a-z0-9_]*$/.test(word));

      const overReached = candidates.filter(
         (word) =>
            parsesAsSourceName(word) &&
            !(word in MALLOY_STANDARD_FUNCTIONS) &&
            toMalloyIdentifier(word) !== word,
      );
      expect(overReached).toEqual([]);
   });
});

/**
 * printable() is the one thing standing between a string read off the user's
 * disk and their terminal, and nothing exercised it directly.
 *
 * Everything here is a character that changes what the terminal shows without
 * adding a character the reader can see, which is why they are shown rather than
 * sent: an ESC repaints the line above, a lone CR overwrites the line in place,
 * and the bidi overrides reorder the run that follows. All three turn a sentence
 * this tool composed into a different sentence the reader believes it composed.
 */
describe("printable", () => {
   const cases: { label: string; raw: string; expected: string }[] = [
      { label: "ESC", raw: "a\u001bb", expected: "a\\u001Bb" },
      { label: "CR", raw: "a\rb", expected: "a\\rb" },
      { label: "LF", raw: "a\nb", expected: "a\\nb" },
      { label: "TAB", raw: "a\tb", expected: "a\\tb" },
      { label: "DEL", raw: "a\u007fb", expected: "a\\u007Fb" },
      { label: "C1 (U+0085)", raw: "a\u0085b", expected: "a\\u0085b" },
      { label: "LINE SEPARATOR", raw: "a\u2028b", expected: "a\\u2028b" },
      // The bidi set. Invisible on their own, so an unescaped one is a
      // reordering the reader has no way to notice.
      { label: "RLO (U+202E)", raw: "a\u202eb", expected: "a\\u202Eb" },
      { label: "LRO (U+202D)", raw: "a\u202db", expected: "a\\u202Db" },
      { label: "RLE (U+202B)", raw: "a\u202bb", expected: "a\\u202Bb" },
      { label: "PDF (U+202C)", raw: "a\u202cb", expected: "a\\u202Cb" },
      { label: "RLI (U+2067)", raw: "a\u2067b", expected: "a\\u2067b" },
      { label: "PDI (U+2069)", raw: "a\u2069b", expected: "a\\u2069b" },
      { label: "RLM (U+200F)", raw: "a\u200fb", expected: "a\\u200Fb" },
      { label: "ALM (U+061C)", raw: "a\u061cb", expected: "a\\u061Cb" },
   ];

   for (const { label, raw, expected } of cases) {
      test(`escapes ${label}`, () => {
         expect(printable(raw)).toBe(expected);
      });
   }

   test("leaves ordinary text, including non-Latin scripts, alone", () => {
      // Only the characters that steer the terminal are escaped. Arabic and
      // Hebrew are right-to-left by their own character properties, which needs
      // no override and is not what this guards against.
      for (const text of [
         "plain",
         "café",
         "日本語",
         "مرحبا",
         "שלום",
         "1.2-3_4",
      ]) {
         expect(printable(text)).toBe(text);
      }
   });

   /**
    * The output holds only characters printable() leaves alone, so running it
    * twice cannot double-escape. The property the doc comment claims.
    */
   test("is idempotent", () => {
      for (const { raw } of cases) {
         expect(printable(printable(raw))).toBe(printable(raw));
      }
   });

   /**
    * preview() is what the CLI reaches for on strings under no length rule at
    * all (a scripts.start entry out of somebody else's package.json), so it has
    * to cap before it escapes: escaping first would let 60 ESCs become a 360
    * character line that is still "within" the limit.
    */
   test("preview caps length and escapes what survives", () => {
      expect(preview("x".repeat(100))).toBe(`${"x".repeat(60)}...`);
      // 60 characters kept, then escaped: the cap counts what was read off
      // disk, not the longer text the escaping produces. Written as an escape,
      // never as the literal character: a raw RLO in a source file reorders the
      // line for everyone reading it afterwards, which is the whole point.
      expect(preview("\u202e".repeat(100))).toBe(`${"\\u202E".repeat(60)}...`);
   });
});
