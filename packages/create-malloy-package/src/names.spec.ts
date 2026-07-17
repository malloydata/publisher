import { describe, expect, test } from "bun:test";
import { MalloyTranslator } from "@malloydata/malloy";
import { ScaffoldError } from "./errors";
import { toMalloyIdentifier, validatePackageName } from "./names";

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
