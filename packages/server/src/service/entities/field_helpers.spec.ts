// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * @fileoverview Tests for field_helpers entity classification.
 */

import { describe, test } from "bun:test";
import assert from "node:assert";
import type { Field } from "@malloydata/malloy";
import { Runtime, InMemoryURLReader } from "@malloydata/malloy";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { AccessModifierEnum, EntityTypeEnum } from "./internal_types";
import {
   getAccessModifier,
   getDescriptionFromTags,
   getEntityFromMalloyField,
   inferType,
   isFilePath,
   isMalloyDefined,
   parseDocAnnotation,
} from "./field_helpers";

// Build a minimal Field whose location.range spans the whole docString.
// Using an `internal.malloy` URL means extractCodeFromEntity reads from
// the provided docString instead of the filesystem.
function mockField(name: string, code: string): { field: Field; doc: string } {
   const lines = code.split("\n");
   const lastLine = lines.length - 1;
   const lastChar = lines[lastLine]!.length;
   const field = {
      name,
      location: {
         url: "file:///internal.malloy",
         range: {
            start: { line: 0, character: 0 },
            end: { line: lastLine, character: lastChar },
         },
      },
   } as unknown as Field;
   return { field, doc: code };
}

// Minimal Field for inferType: plain atomic non-hidden, non-aggregate,
// non-explore dimension with no hidden tags and a boolean type.
function mockDimensionField(
   name: string,
   code: string,
): { field: Field; doc: string } {
   const { field: base, doc } = mockField(name, code);
   const field = Object.assign(base as unknown as Record<string, unknown>, {
      isAtomicField: () => true,
      isExploreField: () => false,
      isCalculation: () => false,
      type: { toString: () => "boolean" },
      getTaglines: () => [],
      parentExplore: {
         tagParse: () => ({
            tag: {
               array: () => undefined,
               text: () => undefined,
            },
         }),
      },
   }) as unknown as Field;
   return { field, doc };
}

describe("isMalloyDefined", () => {
   test("recognizes a plain-identifier dimension definition", () => {
      const { field, doc } = mockField(
         "recall_url",
         "recall_url is concat('https://example.com/', x)",
      );
      assert.strictEqual(isMalloyDefined(field, doc), true);
   });

   test("recognizes a backtick-quoted dimension definition", () => {
      // Regression: before stripping backticks from the code, the regex
      // `/Major Recall\s+is/` failed to match "`Major Recall` is …" because
      // \s+ does not consume the trailing backtick, causing the field to
      // be misclassified as a column by inferType.
      const { field, doc } = mockField(
         "Major Recall",
         "`Major Recall` is ifnull(`Potentially Affected`, 0) > 100000",
      );
      assert.strictEqual(isMalloyDefined(field, doc), true);
   });

   test("returns false when the name appears only as a bare reference", () => {
      const { field, doc } = mockField(
         "Potentially Affected",
         "`Potentially Affected`",
      );
      assert.strictEqual(isMalloyDefined(field, doc), false);
   });
});

describe("inferType", () => {
   test("classifies a backtick-quoted dimension as Dimension (not Column)", () => {
      // End-to-end path from the bug: `Major Recall` is defined as a
      // dimension but was being classified as a Column in CI because the
      // backtick-aware check in isMalloyDefined missed it.
      const { field, doc } = mockDimensionField(
         "Major Recall",
         "`Major Recall` is ifnull(`Potentially Affected`, 0) > 100000",
      );
      assert.strictEqual(inferType(field, doc), EntityTypeEnum.Dimension);
   });

   test("classifies a plain-identifier dimension as Dimension", () => {
      const { field, doc } = mockDimensionField(
         "recall_url",
         "recall_url is concat('https://example.com/', x)",
      );
      assert.strictEqual(inferType(field, doc), EntityTypeEnum.Dimension);
   });

   test("classifies a bare column reference as Column", () => {
      const { field, doc } = mockDimensionField(
         "Potentially Affected",
         "`Potentially Affected`",
      );
      assert.strictEqual(inferType(field, doc), EntityTypeEnum.Column);
   });
});

// Minimal Field stand-ins exercising the three Malloy subclasses:
//   - AtomicField  carries the modifier on `fieldTypeDef`
//   - QueryField   (view / turtle) carries it on `turtleDef`
//   - ExploreField (join) carries it on `_structDef` (inherited from Explore)
const atomicField = (accessModifier?: string): Field =>
   ({
      fieldTypeDef: accessModifier === undefined ? {} : { accessModifier },
   }) as unknown as Field;

const queryField = (accessModifier?: string): Field =>
   ({
      turtleDef: accessModifier === undefined ? {} : { accessModifier },
   }) as unknown as Field;

const exploreField = (accessModifier?: string): Field =>
   ({
      _structDef: accessModifier === undefined ? {} : { accessModifier },
   }) as unknown as Field;

describe("getAccessModifier", () => {
   test("atomic field with no modifier is Public", () => {
      assert.strictEqual(
         getAccessModifier(atomicField()),
         AccessModifierEnum.Public,
      );
   });

   test("atomic field marked public is Public", () => {
      assert.strictEqual(
         getAccessModifier(atomicField("public")),
         AccessModifierEnum.Public,
      );
   });

   test("atomic field marked private is Private", () => {
      assert.strictEqual(
         getAccessModifier(atomicField("private")),
         AccessModifierEnum.Private,
      );
   });

   test("atomic field marked internal is Internal", () => {
      assert.strictEqual(
         getAccessModifier(atomicField("internal")),
         AccessModifierEnum.Internal,
      );
   });

   test("query (view) field with no modifier is Public", () => {
      assert.strictEqual(
         getAccessModifier(queryField()),
         AccessModifierEnum.Public,
      );
   });

   // Guards that view access modifiers are actually read (not defaulted
   // to Public), so private/internal views don't leak into summaries or
   // the entity index.
   test("query (view) field marked private is Private", () => {
      assert.strictEqual(
         getAccessModifier(queryField("private")),
         AccessModifierEnum.Private,
      );
   });

   test("query (view) field marked internal is Internal", () => {
      assert.strictEqual(
         getAccessModifier(queryField("internal")),
         AccessModifierEnum.Internal,
      );
   });

   test("explore (join) field with no modifier is Public", () => {
      assert.strictEqual(
         getAccessModifier(exploreField()),
         AccessModifierEnum.Public,
      );
   });

   // Guards that join access modifiers are actually read; for
   // inline-defined joins a missed modifier would leak the entire
   // nested source.
   test("explore (join) field marked private is Private", () => {
      assert.strictEqual(
         getAccessModifier(exploreField("private")),
         AccessModifierEnum.Private,
      );
   });

   test("explore (join) field marked internal is Internal", () => {
      assert.strictEqual(
         getAccessModifier(exploreField("internal")),
         AccessModifierEnum.Internal,
      );
   });

   test("unknown modifier value falls back to Public", () => {
      assert.strictEqual(
         getAccessModifier(atomicField("something-new")),
         AccessModifierEnum.Public,
      );
   });
});

describe("parseDocAnnotation", () => {
   test("line form #(doc) strips prefix", () => {
      assert.strictEqual(parseDocAnnotation("#(doc) A name."), "A name.");
   });

   test("line form tolerates a trailing newline", () => {
      assert.strictEqual(parseDocAnnotation("#(doc) A name.\n"), "A name.");
   });

   test("block form #|(doc) preserves internal newlines", () => {
      assert.strictEqual(
         parseDocAnnotation("#|(doc)\nFirst line.\nSecond line."),
         "First line.\nSecond line.",
      );
   });

   test("bare block prefix yields empty body", () => {
      assert.strictEqual(parseDocAnnotation("#|(doc)"), "");
   });

   test("text on the block prefix line is kept as the first body line", () => {
      assert.strictEqual(
         parseDocAnnotation("#|(doc) First line\nSecond line"),
         "First line\nSecond line",
      );
   });

   test("prefix-line-only block text works without following lines", () => {
      assert.strictEqual(parseDocAnnotation("#|(doc) Only line"), "Only line");
   });

   test("CRLF line endings are normalized to \\n", () => {
      assert.strictEqual(
         parseDocAnnotation("#|(doc)\r\nFirst.\r\nSecond."),
         "First.\nSecond.",
      );
   });

   test("non-doc annotations return null", () => {
      assert.strictEqual(parseDocAnnotation("#(filter) name=X"), null);
      assert.strictEqual(parseDocAnnotation("#(agent-hidden)"), null);
   });

   test("empty / undefined returns null", () => {
      assert.strictEqual(parseDocAnnotation(undefined), null);
      assert.strictEqual(parseDocAnnotation(""), null);
   });
});

describe("getDescriptionFromTags", () => {
   test("single doc tag, skipping non-doc tags", () => {
      assert.strictEqual(
         getDescriptionFromTags(["#(agent-hidden)", "#(doc) Real doc."]),
         "Real doc.",
      );
   });

   test("joins repeated legacy #(doc) lines with \\n (back-compat)", () => {
      assert.strictEqual(
         getDescriptionFromTags([
            "#(doc) Line one.",
            "#(doc) Line two.",
            "#(doc) Line three.",
         ]),
         "Line one.\nLine two.\nLine three.",
      );
   });

   test("ignores interleaved non-doc tags when joining", () => {
      assert.strictEqual(
         getDescriptionFromTags([
            "#(doc) Line one.",
            "# bar_chart",
            "#(doc) Line two.",
         ]),
         "Line one.\nLine two.",
      );
   });

   test("returns undefined when no doc tag present", () => {
      assert.strictEqual(getDescriptionFromTags(["#(filter) x"]), undefined);
   });
});

// Compile-backed: feed the block-annotation form through the REAL Malloy
// compiler (not a mocked tagline) and assert the multi-line body survives with
// its `\n` intact. This is the oracle the string-only mocks couldn't be.
describe("getDescriptionFromTags (compile-backed)", () => {
   async function describeField(model: string, fieldName: string) {
      const conn = new DuckDBConnection("duckdb");
      try {
         const runtime = new Runtime({
            urlReader: new InMemoryURLReader(new Map()),
            connection: conn,
         });
         await conn.runRawSQL(
            "CREATE OR REPLACE TABLE t AS SELECT 'a' as label",
         );
         const compiled = await runtime.getModel(model);
         const explore = compiled.getExploreByName("nums");
         const field = explore.allFields.find((f) => f.name === fieldName)!;
         return getDescriptionFromTags(field.getTaglines());
      } finally {
         await conn.close();
      }
   }

   test("block #|(doc) on a dimension yields a multi-line description", async () => {
      const model = `
source: nums is duckdb.table('t') extend {
  #|(doc)
  The customer's first name.
  Used as a join key.
  |#
  dimension: pub_label is label
}`;
      assert.strictEqual(
         await describeField(model, "pub_label"),
         "The customer's first name.\nUsed as a join key.",
      );
   });

   test("legacy single-line #(doc) still works", async () => {
      const model = `
source: nums is duckdb.table('t') extend {
  #(doc) Single line.
  dimension: pub_label is label
}`;
      assert.strictEqual(
         await describeField(model, "pub_label"),
         "Single line.",
      );
   });

   test("block #|(doc) with text on the prefix line keeps that first line", async () => {
      // The compiler emits `#|(doc) First line\nSecond line` as ONE tagline —
      // the prefix-line text is part of the body, not a stray remainder.
      const model = `
source: nums is duckdb.table('t') extend {
  #|(doc) First line
  Second line
  |#
  dimension: pub_label is label
}`;
      assert.strictEqual(
         await describeField(model, "pub_label"),
         "First line\nSecond line",
      );
   });

   test("legacy #(doc) repeated per line joins into a multi-line description", async () => {
      const model = `
source: nums is duckdb.table('t') extend {
  #(doc) Line one.
  #(doc) Line two.
  #(doc) Line three.
  dimension: pub_label is label
}`;
      assert.strictEqual(
         await describeField(model, "pub_label"),
         "Line one.\nLine two.\nLine three.",
      );
   });
});

describe("getEntityFromMalloyField table", () => {
   const tableOf = (tablePath: string, dialect: string) =>
      getEntityFromMalloyField(
         mockDimensionField("d", "dimension: d is true").field,
         "s",
         "dimension: d is true",
         "conn",
         tablePath,
         null,
         null,
         dialect,
      ).table_info;

   test("a DuckDB file is reported whole", () => {
      assert.deepStrictEqual(tableOf("orders.csv", "duckdb"), {
         connection_name: "conn",
         catalog_name: null,
         schema_name: null,
         table_name: "orders.csv",
      });
   });

   test("a warehouse name splits, a BigQuery wildcard table included", () => {
      assert.deepStrictEqual(tableOf("proj.ds.events_*", "standardsql"), {
         connection_name: "conn",
         catalog_name: "proj",
         schema_name: "ds",
         table_name: "events_*",
      });
      assert.strictEqual(
         tableOf("warehouse.data.csv", "snowflake")?.table_name,
         "csv",
      );
   });
});

describe("isFilePath", () => {
   test("a file, URL or glob is a file; a dotted name is not", () => {
      for (const path of [
         "orders.csv",
         "data/orders.parquet",
         "s3://bucket/orders.json",
         "logs/*.jsonl",
      ]) {
         assert.strictEqual(isFilePath(path), true, path);
      }
      for (const path of ["orders", "sales.orders", "proj.sales.orders"]) {
         assert.strictEqual(isFilePath(path), false, path);
      }
   });
});
