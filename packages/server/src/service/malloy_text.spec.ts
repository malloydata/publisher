// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, describe, expect, it } from "bun:test";
import * as path from "path";
import {
   annotationValue,
   escapeString,
   identifier,
   MALLOY_BARE_KEYWORDS,
   quotedIdentifier,
   quotedPath,
   stringLiteral,
} from "./malloy_text";

// Names that each break a naive quoter: a keyword, a space, a hyphen, a
// leading digit, an embedded backtick, a trailing backslash, a dot.
const AWKWARD = ["date", "count", "my col", "a-b", "1st", "a`b", "a\\", "x.y"];

const duckdb = new DuckDBConnection("duckdb");
afterAll(async () => {
   await duckdb.close();
});

/** Compile and run `model + query`, returning the first row. */
async function runOne(
   model: string,
   query: string,
): Promise<Record<string, unknown>> {
   const url = "file:///m.malloy";
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([[url, model]])),
      connections: new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   const result = await runtime.loadModel(new URL(url)).loadQuery(query).run();
   return result.data.toObject()[0] as Record<string, unknown>;
}

/** DuckDB SQL naming a column exactly `name`. */
function sqlColumn(name: string): string {
   return `"${name.replace(/"/g, '""')}"`;
}

describe("identifier and quotedIdentifier", () => {
   it("emit plain names bare and quote everything else", () => {
      expect(identifier("orders")).toBe("orders");
      expect(identifier("order_items_2")).toBe("order_items_2");
      expect(identifier("date")).toBe("`date`");
      expect(identifier("Date")).toBe("`Date`");
      expect(identifier("my col")).toBe("`my col`");
      expect(identifier("a`b")).toBe("`a\\`b`");
      expect(identifier("a\\")).toBe("`a\\\\`");
      expect(quotedIdentifier("orders")).toBe("`orders`");
   });

   for (const name of AWKWARD) {
      it(`round-trips ${JSON.stringify(name)} through a compile`, async () => {
         const model = `source: s is duckdb.sql(${stringLiteral(`select 1 as ${sqlColumn(name)}`)})`;
         for (const emit of [identifier, quotedIdentifier]) {
            const row = await runOne(
               model,
               `run: s -> { select: v is ${emit(name)} }`,
            );
            expect(row.v).toBe(1);
         }
      });
   }

   // The helpers this module replaced disagreed. The serve shape doubled an
   // embedded backtick (Malloy escapes with a backslash) and emitted a keyword
   // bare; the rollup companion did not escape at all. Each old spelling fails
   // to compile where the new one succeeds.
   it("fixes the spellings the old helpers produced", async () => {
      const model = `source: s is duckdb.sql(${stringLiteral(`select 1 as ${sqlColumn("a`b")}, 2 as ${sqlColumn("date")}`)})`;
      await expect(
         runOne(model, "run: s -> { select: v is `a``b` }"),
      ).rejects.toThrow();
      await expect(
         runOne(model, "run: s -> { select: v is date }"),
      ).rejects.toThrow();
      expect(
         (
            await runOne(
               model,
               `run: s -> { select: v is ${identifier("a`b")} }`,
            )
         ).v,
      ).toBe(1);
      expect(
         (
            await runOne(
               model,
               `run: s -> { select: v is ${identifier("date")} }`,
            )
         ).v,
      ).toBe(2);
   });
});

describe("quotedPath", () => {
   it("quotes each segment, from a dotted string or a list", () => {
      expect(quotedPath("orders.label")).toBe("`orders`.`label`");
      expect(quotedPath(["orders", "a`b"])).toBe("`orders`.`a\\`b`");
      expect(quotedPath("plain")).toBe("`plain`");
   });

   it("reads a join path back through a compile", async () => {
      const model = `
source: c is duckdb.sql(${stringLiteral(`select 1 as id, 'east' as ${sqlColumn("my region")}`)}) extend { primary_key: id }
source: o is duckdb.sql('select 1 as cid') extend { join_one: \`my join\` is c on cid = \`my join\`.id }
`;
      const row = await runOne(
         model,
         `run: o -> { select: v is ${quotedPath(["my join", "my region"])} }`,
      );
      expect(row.v).toBe("east");
   });
});

describe("string literals", () => {
   for (const value of ["o'brien", "back\\slash", "both\\'", "plain"]) {
      it(`round-trips ${JSON.stringify(value)}`, async () => {
         const row = await runOne(
            `source: s is duckdb.sql('select 1 as one')`,
            `run: s -> { select: v is ${stringLiteral(value)} }`,
         );
         expect(row.v).toBe(value);
      });
   }

   it("escapes with a backslash, never by doubling", () => {
      expect(escapeString("o'brien")).toBe("o\\'brien");
      expect(stringLiteral("a\\b")).toBe("'a\\\\b'");
   });
});

describe("annotationValue", () => {
   it("always double-quotes, so a hyphen does not split the tag", () => {
      expect(annotationValue("my-lake")).toBe('"my-lake"');
      expect(annotationValue('a"b')).toBe('"a\\"b"');
   });
});

describe("MALLOY_BARE_KEYWORDS", () => {
   it("covers every bare-word keyword Malloy's lexer defines", () => {
      // Read from Malloy's generated keyword table, a test-only dependency on
      // a non-public module: a keyword Malloy adds fails here, not in a
      // generated model.
      const file = path.join(
         path.dirname(require.resolve("@malloydata/malloy")),
         "lang/lib/Malloy/keyword-display-names.js",
      );
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const { KEYWORD_DISPLAY_NAMES } = require(file) as {
         KEYWORD_DISPLAY_NAMES: Record<string, string>;
      };
      const lexer = Object.values(KEYWORD_DISPLAY_NAMES).filter((s) =>
         /^[A-Za-z_][A-Za-z0-9_]*$/.test(s),
      );
      expect(lexer.length).toBeGreaterThan(0);
      for (const word of lexer) {
         expect(MALLOY_BARE_KEYWORDS.has(word.toLowerCase())).toBe(true);
      }
   });
});
