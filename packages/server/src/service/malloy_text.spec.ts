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
   doubleQuotedString,
   escapeString,
   identifier,
   MALLOY_BARE_KEYWORDS,
   MALLOY_STATEMENT_KEYWORDS,
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

/** Compile `model + query` to SQL without running it. */
async function compileOne(model: string, query: string): Promise<string> {
   const url = "file:///m.malloy";
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([[url, model]])),
      connections: new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   return runtime.loadModel(new URL(url)).loadQuery(query).getSQL();
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

describe("doubleQuotedString", () => {
   it("reads exactly as JSON.stringify wrote an ordinary path", () => {
      for (const p of [
         "./model.malloy",
         "../shared/orders.malloy",
         "file:///p/a b.malloy",
      ]) {
         expect(doubleQuotedString(p)).toBe(JSON.stringify(p));
      }
   });

   // Inside duckdb.sql("…"), the one place a double-quoted Malloy string can
   // be read back at run time.
   for (const value of ['a"b', "back\\slash", "line\nbreak"]) {
      it(`round-trips ${JSON.stringify(value)}`, async () => {
         const sql = `select '${value.replace(/'/g, "''")}' as v`;
         const row = await runOne(
            `source: s is duckdb.sql(${doubleQuotedString(sql)})`,
            "run: s -> { select: v }",
         );
         expect(row.v).toBe(value);
      });
   }
});

describe("control characters", () => {
   // Malloy's lexer refuses a raw line break or other control character
   // inside a quoted string or identifier; written as an escape it reads back.
   const VALUES = ["line1\nline2", "cr\rx", "bel\u0007", "tab\tstays"];

   it("escapes all but a tab, which compiles raw and stays unchanged", () => {
      expect(escapeString("a\nb")).toBe("a\\nb");
      expect(escapeString("a\rb")).toBe("a\\rb");
      expect(escapeString("a\u0001b")).toBe("a\\u0001b");
      expect(escapeString("a\tb")).toBe("a\tb");
      expect(quotedIdentifier("a\nb")).toBe("`a\\nb`");
   });

   for (const value of VALUES) {
      it(`round-trips the string ${JSON.stringify(value)}`, async () => {
         const row = await runOne(
            `source: s is duckdb.sql('select 1 as one')`,
            `run: s -> { select: v is ${stringLiteral(value)} }`,
         );
         expect(row.v).toBe(value);
      });
   }

   // DuckDB itself refuses a NUL in a SQL string and mishandles a column name
   // with a line break, so these two compile the Malloy rather than run it:
   // the claim is only that the text is valid Malloy.
   it("compiles a NUL in a string and a line break in a name", async () => {
      const name = "two\nlines";
      const model = `source: s is duckdb.sql(${stringLiteral(`select 1 as ${sqlColumn(name)}, ${stringLiteral("nul\u0000x")} as t`)})`;
      for (const emit of [identifier, quotedIdentifier]) {
         await expect(
            compileOne(
               model,
               `run: s -> { select: v is ${emit(name)}, w is ${stringLiteral("nul\u0000x")} }`,
            ),
         ).resolves.toContain("SELECT");
      }
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

   it("MALLOY_STATEMENT_KEYWORDS covers every statement keyword, without its colon", () => {
      const file = path.join(
         path.dirname(require.resolve("@malloydata/malloy")),
         "lang/lib/Malloy/keyword-display-names.js",
      );
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const { KEYWORD_DISPLAY_NAMES } = require(file) as {
         KEYWORD_DISPLAY_NAMES: Record<string, string>;
      };
      const statements = Object.values(KEYWORD_DISPLAY_NAMES)
         .filter((s) => /^[A-Za-z_][A-Za-z0-9_]*:$/.test(s))
         .map((s) => s.slice(0, -1).toLowerCase());
      expect(statements.length).toBeGreaterThan(0);
      expect([...MALLOY_STATEMENT_KEYWORDS].sort()).toEqual(
         [...new Set(statements)].sort(),
      );
   });

   it("quotes a statement keyword, which a following colon would lex as one", async () => {
      expect(identifier("select")).toBe("`select`");
      expect(identifier("order")).toBe("order");
      // `select::string` in a `type:` lexes as `select:`; quoted, it compiles.
      const typed = (name: (n: string) => string) =>
         "##! experimental { virtual_source }\n" +
         `type: t is { ${name("select")}::string, ${name("group_by")}::number }\n`;
      const load = (model: string) =>
         new Runtime({
            urlReader: new InMemoryURLReader(
               new Map([["file:///m.malloy", model]]),
            ),
            connections: new FixedConnectionMap(
               new Map([["duckdb", duckdb]]),
               "duckdb",
            ),
         })
            .loadModel(new URL("file:///m.malloy"))
            .getModel();
      await expect(load(typed(identifier))).resolves.toBeDefined();
      await expect(load(typed((n) => n))).rejects.toThrow();
   });
});
