import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import path from "path";
import { extractPreamble, extractPreambleFromSource } from "./project";

describe("extractPreambleFromSource", () => {
   it("should extract pragmas and imports before a source definition", () => {
      const content = [
         "##! experimental.parameters",
         'import "utils.malloy"',
         'import { revenue } from "metrics.malloy"',
         "",
         'source: my_source is duckdb.table("data.parquet")',
         '  dimension: name is "test"',
      ].join("\n");

      const result = extractPreambleFromSource(content);
      expect(result).toBe(
         [
            "##! experimental.parameters",
            'import "utils.malloy"',
            'import { revenue } from "metrics.malloy"',
         ].join("\n"),
      );
   });

   it("should extract pragmas and imports before a run statement", () => {
      const content = [
         'import { my_source } from "model.malloy"',
         "",
         "run: my_source -> { aggregate: count() }",
      ].join("\n");

      const result = extractPreambleFromSource(content);
      expect(result).toBe('import { my_source } from "model.malloy"');
   });

   it("should extract pragmas and imports before a query definition", () => {
      const content = [
         'import { my_source } from "model.malloy"',
         "",
         "query: top_items is my_source -> { limit: 10 }",
      ].join("\n");

      const result = extractPreambleFromSource(content);
      expect(result).toBe('import { my_source } from "model.malloy"');
   });

   it("should return empty string when file starts with source:", () => {
      const content = 'source: my_source is duckdb.table("data.parquet")';

      const result = extractPreambleFromSource(content);
      expect(result).toBe("");
   });

   it("should return empty string when file starts with run:", () => {
      const content = 'run: duckdb.sql("SELECT 1")';

      const result = extractPreambleFromSource(content);
      expect(result).toBe("");
   });

   it("should return empty string when file starts with query:", () => {
      const content = 'query: q is duckdb.sql("SELECT 1")';

      const result = extractPreambleFromSource(content);
      expect(result).toBe("");
   });

   it("should preserve comments in the preamble", () => {
      const content = [
         "##! experimental.parameters",
         "// This model defines revenue metrics",
         'import { revenue } from "metrics.malloy"',
         "// Main source below",
         'source: my_source is duckdb.table("data.parquet")',
      ].join("\n");

      const result = extractPreambleFromSource(content);
      expect(result).toBe(
         [
            "##! experimental.parameters",
            "// This model defines revenue metrics",
            'import { revenue } from "metrics.malloy"',
            "// Main source below",
         ].join("\n"),
      );
   });

   it("should handle multiple import blocks", () => {
      const content = [
         'import { a } from "file_a.malloy"',
         'import { b } from "file_b.malloy"',
         'import { c, d } from "file_c.malloy"',
         "",
         "source: combined is a {",
         "  join_one: b on b.id = a.b_id",
         "}",
      ].join("\n");

      const result = extractPreambleFromSource(content);
      expect(result).toBe(
         [
            'import { a } from "file_a.malloy"',
            'import { b } from "file_b.malloy"',
            'import { c, d } from "file_c.malloy"',
         ].join("\n"),
      );
   });

   it("should return all content when there are no source/query/run definitions", () => {
      const content = [
         "##! experimental.parameters",
         'import { revenue } from "metrics.malloy"',
         "",
         "// no definitions yet",
      ].join("\n");

      const result = extractPreambleFromSource(content);
      expect(result).toBe(
         [
            "##! experimental.parameters",
            'import { revenue } from "metrics.malloy"',
            "",
            "// no definitions yet",
         ].join("\n"),
      );
   });

   it("should return empty string for empty input", () => {
      expect(extractPreambleFromSource("")).toBe("");
   });

   it("should handle indented source/query/run definitions", () => {
      const content = [
         'import "model.malloy"',
         "",
         '  source: indented is duckdb.table("data.parquet")',
      ].join("\n");

      // Indented definitions should still be detected (trimmed before check)
      const result = extractPreambleFromSource(content);
      expect(result).toBe('import "model.malloy"');
   });

   it("should not stop on 'source' appearing in comments or strings", () => {
      const content = [
         "// This file defines the source of truth",
         'import "model.malloy"',
         'source: my_source is duckdb.table("data.parquet")',
      ].join("\n");

      const result = extractPreambleFromSource(content);
      expect(result).toBe(
         [
            "// This file defines the source of truth",
            'import "model.malloy"',
         ].join("\n"),
      );
   });
});

describe("extractPreamble (file-based)", () => {
   const testDir = path.join(process.cwd(), "test-temp-preamble");
   const testModelPath = path.join(testDir, "test_model.malloy");

   beforeEach(() => {
      if (!fs.existsSync(testDir)) {
         fs.mkdirSync(testDir, { recursive: true });
      }
   });

   afterEach(() => {
      if (fs.existsSync(testDir)) {
         fs.rmSync(testDir, { recursive: true, force: true });
      }
   });

   it("should read and extract preamble from a file", async () => {
      const content = [
         'import { revenue } from "metrics.malloy"',
         "",
         'source: my_source is duckdb.table("data.parquet")',
      ].join("\n");
      fs.writeFileSync(testModelPath, content);

      const result = await extractPreamble(testModelPath);
      expect(result).toBe('import { revenue } from "metrics.malloy"');
   });

   it("should return empty string when file does not exist", async () => {
      const result = await extractPreamble(
         path.join(testDir, "nonexistent.malloy"),
      );
      expect(result).toBe("");
   });
});
