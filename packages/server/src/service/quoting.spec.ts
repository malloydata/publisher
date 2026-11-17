// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   bareTableName,
   isQuotedIdentifierPath,
   quoteIdentifier,
   quoteManifestTablePath,
   quoteRenameTarget,
   quoteTablePath,
   sqlTablePathParts,
   UnquotableIdentifierError,
} from "./quoting";
import { getDialect } from "@malloydata/malloy";
import { decodeTablePathSegments } from "./materialization/run/incremental_apply";

describe("bareTableName", () => {
   it("returns the segment after the last dot for a qualified name", () => {
      expect(bareTableName("my_schema.my_table")).toBe("my_table");
   });

   it("returns the name unchanged when it is unqualified", () => {
      expect(bareTableName("my_table")).toBe("my_table");
   });
});

describe("quoteIdentifier", () => {
   it("backticks for backtick dialects (BigQuery / MySQL / Databricks)", () => {
      expect(quoteIdentifier("my-table", "standardsql")).toBe("`my-table`");
      expect(quoteIdentifier("t", "mysql")).toBe("`t`");
      expect(quoteIdentifier("t", "databricks")).toBe("`t`");
   });

   it("double-quotes for everything else (Postgres / DuckDB / Snowflake)", () => {
      expect(quoteIdentifier("my-table", "postgres")).toBe('"my-table"');
      expect(quoteIdentifier("t", "duckdb")).toBe('"t"');
      expect(quoteIdentifier("t", "snowflake")).toBe('"t"');
   });

   it("escapes a backtick and a backslash with a backslash on BigQuery", () => {
      expect(quoteIdentifier("a`b", "standardsql")).toBe("`a\\`b`");
      expect(quoteIdentifier("a\\b", "standardsql")).toBe("`a\\\\b`");
      expect(quoteIdentifier("a\\", "standardsql")).toBe("`a\\\\`");
   });

   it("doubles an embedded quote char on every other dialect", () => {
      expect(quoteIdentifier("a`b", "mysql")).toBe("`a``b`");
      expect(quoteIdentifier("a`b", "databricks")).toBe("`a``b`");
      expect(quoteIdentifier('a"b', "postgres")).toBe('"a""b"');
   });
});

// ---------------------------------------------------------------------------
// Hostile name parts, decoded by each dialect's DOCUMENTED rule rather than by
// the encoder's inverse, so a quoting that a warehouse reads differently fails
// here.
// ---------------------------------------------------------------------------

/** What a reader made of a quoted identifier at the start of `sql`. */
type Read = { name: string; end: number } | { error: string };

/**
 * BigQuery's lexer on a quoted identifier: backticks, the escape sequences of a
 * string literal, and no raw newline
 * (https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical).
 * Octal, hex and Unicode escapes are not decoded: nothing here writes them.
 */
function readBigQuery(sql: string): Read {
   const simple: Record<string, string> = {
      a: "\x07",
      b: "\b",
      f: "\f",
      n: "\n",
      r: "\r",
      t: "\t",
      v: "\v",
      "\\": "\\",
      "?": "?",
      '"': '"',
      "'": "'",
      "`": "`",
   };
   if (sql[0] !== "`") return { error: "not quoted" };
   let name = "";
   for (let i = 1; i < sql.length; i++) {
      const c = sql[i];
      if (c === "\n" || c === "\r") return { error: "newline in identifier" };
      if (c === "`") {
         return name === "" ? { error: "empty" } : { name, end: i + 1 };
      }
      if (c === "\\") {
         const e = simple[sql[i + 1]];
         if (e === undefined) return { error: "illegal escape" };
         name += e;
         i++;
         continue;
      }
      name += c;
   }
   return { error: "unterminated" };
}

/**
 * A quoted identifier whose quote char is doubled to escape it and where a
 * backslash is an ordinary character: MySQL and Databricks with a backtick
 * (https://dev.mysql.com/doc/refman/8.4/en/identifiers.html,
 * https://docs.databricks.com/sql/language-manual/sql-ref-identifiers.html),
 * Postgres, DuckDB, Snowflake, Trino and SQL Server with a double quote.
 */
function readDoubled(sql: string, q: string): Read {
   if (sql[0] !== q) return { error: "not quoted" };
   let name = "";
   for (let i = 1; i < sql.length; i++) {
      if (sql[i] === q) {
         if (sql[i + 1] === q) {
            name += q;
            i++;
            continue;
         }
         return { name, end: i + 1 };
      }
      name += sql[i];
   }
   return { error: "unterminated" };
}

const READERS: Record<string, (sql: string) => Read> = {
   standardsql: readBigQuery,
   mysql: (sql) => readDoubled(sql, "`"),
   databricks: (sql) => readDoubled(sql, "`"),
   postgres: (sql) => readDoubled(sql, '"'),
   duckdb: (sql) => readDoubled(sql, '"'),
   snowflake: (sql) => readDoubled(sql, '"'),
   trino: (sql) => readDoubled(sql, '"'),
   presto: (sql) => readDoubled(sql, '"'),
   sqlserver: (sql) => readDoubled(sql, '"'),
};

/** Name parts that would end an identifier early under a wrong escape. */
const HOSTILE = [
   "a`b",
   "a``b",
   "trailing`",
   "`",
   "a\\b",
   "trailing\\",
   "\\`",
   "\\\\`",
   'a"b',
   "a'b",
   '"',
   "`;\\\\\"'",
   "dots.stay.one.part",
];

describe("quoteIdentifier on hostile name parts", () => {
   for (const [dialect, read] of Object.entries(READERS)) {
      for (const part of HOSTILE) {
         it(`${dialect} reads ${JSON.stringify(part)} back whole`, () => {
            const quoted = quoteIdentifier(part, dialect);
            // Followed by more SQL, the identifier still ends where the
            // quoting ends: no early close, no swallowed closing quote.
            const read1 = read(quoted + ".x");
            expect(read1).toEqual({ name: part, end: quoted.length });
         });
      }
   }

   it("refuses a BigQuery part with a newline or other control character", () => {
      for (const part of ["a\nb", "a\rb", "a\tb", "\u007f", "\u0001"]) {
         expect(() => quoteIdentifier(part, "standardsql")).toThrow(
            UnquotableIdentifierError,
         );
      }
   });

   it("refuses an empty BigQuery part, which BigQuery cannot quote", () => {
      expect(() => quoteIdentifier("", "standardsql")).toThrow(
         UnquotableIdentifierError,
      );
      expect(() => quoteTablePath("ds..t", "standardsql")).toThrow(
         UnquotableIdentifierError,
      );
   });

   it("keeps a newline literally on dialects whose quoted identifiers hold one", () => {
      for (const [dialect, read] of Object.entries(READERS)) {
         if (dialect === "standardsql") continue;
         const quoted = quoteIdentifier("a\nb", dialect);
         expect(read(quoted)).toEqual({ name: "a\nb", end: quoted.length });
      }
   });

   it("refuses NUL on every dialect", () => {
      for (const dialect of Object.keys(READERS)) {
         expect(() => quoteIdentifier("a\u0000b", dialect)).toThrow(
            UnquotableIdentifierError,
         );
      }
   });

   it("matches Malloy's own BigQuery quoting wherever Malloy quotes the name", () => {
      // Malloy's standardsql dialect escapes with backslashes too, but refuses
      // a backtick outright and encodes a newline that this module refuses.
      const malloy = getDialect("standardsql");
      for (const part of HOSTILE.filter((p) => !p.includes("`"))) {
         expect(quoteIdentifier(part, "standardsql")).toBe(
            malloy.sqlQuoteIdentifier(part),
         );
      }
   });
});

describe("quoteTablePath on hostile name parts", () => {
   const parts = ["my-proj", "d`s\\", "t\"'x"];

   it("quotes project.dataset.table so it decodes to the intended parts", () => {
      const path = quoteTablePath(parts.join("."), "standardsql");
      expect(path).toBe("`my-proj`.`d\\`s\\\\`.`t\"'x`");
      expect(decodeTablePathSegments("standardsql", path)).toEqual(parts);
      let rest = path;
      const read: string[] = [];
      for (;;) {
         const r = readBigQuery(rest);
         if ("error" in r) throw new Error(r.error);
         read.push(r.name);
         if (r.end === rest.length) break;
         expect(rest[r.end]).toBe(".");
         rest = rest.slice(r.end + 1);
      }
      expect(read).toEqual(parts);
   });

   it("decodes the doubled form on the doubling dialects", () => {
      expect(
         decodeTablePathSegments(
            "postgres",
            quoteTablePath('s"x.t\\', "postgres"),
         ),
      ).toEqual(['s"x', "t\\"]);
   });
});

// Cross-repo conformance (PR ms2data/service#5272 review item 1). The publisher
// keys identifier quoting off the Malloy `dialectName`; the control plane keys
// the SAME fact off the publisher *connection type* in
// `PhysicalTableName.BACKTICK_TYPES` ({bigquery, mysql, databricks}). The two
// must stay byte-compatible — a name the publisher CREATEs with backticks the CP
// must DROP with backticks — but they live in different repos under different key
// vocabularies (`standardsql` the dialect vs `bigquery` the connection type), so
// drift produces malformed DDL with no compile error. This table pins the full
// connection-type → dialect → quote-char correspondence; if it changes, update
// `PhysicalTableName.BACKTICK_TYPES` in the control plane in lockstep.
describe("dialect/connection-type quoting conformance", () => {
   const QUOTING = [
      { connectionType: "bigquery", dialect: "standardsql", quote: "`" },
      { connectionType: "mysql", dialect: "mysql", quote: "`" },
      { connectionType: "databricks", dialect: "databricks", quote: "`" },
      { connectionType: "postgres", dialect: "postgres", quote: '"' },
      { connectionType: "snowflake", dialect: "snowflake", quote: '"' },
      { connectionType: "trino", dialect: "trino", quote: '"' },
      { connectionType: "sqlserver", dialect: "sqlserver", quote: '"' },
      { connectionType: "duckdb", dialect: "duckdb", quote: '"' },
      { connectionType: "motherduck", dialect: "duckdb", quote: '"' },
   ] as const;

   for (const { connectionType, dialect, quote } of QUOTING) {
      it(`${connectionType} (${dialect}) quotes with ${quote}`, () => {
         expect(quoteIdentifier("x", dialect)).toBe(`${quote}x${quote}`);
      });
   }

   // The escapes, per connection type. The control plane admits only
   // `[A-Za-z0-9_-]` names, so it never quotes one of these; bigquery's row is
   // where it would differ (it doubles a backtick) if it ever did.
   const ESCAPES = [
      {
         connectionType: "bigquery",
         dialect: "standardsql",
         tick: "`a\\`b`",
         slash: "`a\\\\b`",
      },
      {
         connectionType: "mysql",
         dialect: "mysql",
         tick: "`a``b`",
         slash: "`a\\b`",
      },
      {
         connectionType: "databricks",
         dialect: "databricks",
         tick: "`a``b`",
         slash: "`a\\b`",
      },
      {
         connectionType: "postgres",
         dialect: "postgres",
         tick: '"a`b"',
         slash: '"a\\b"',
      },
      {
         connectionType: "snowflake",
         dialect: "snowflake",
         tick: '"a`b"',
         slash: '"a\\b"',
      },
      {
         connectionType: "trino",
         dialect: "trino",
         tick: '"a`b"',
         slash: '"a\\b"',
      },
      {
         connectionType: "sqlserver",
         dialect: "sqlserver",
         tick: '"a`b"',
         slash: '"a\\b"',
      },
      {
         connectionType: "duckdb",
         dialect: "duckdb",
         tick: '"a`b"',
         slash: '"a\\b"',
      },
      {
         connectionType: "motherduck",
         dialect: "duckdb",
         tick: '"a`b"',
         slash: '"a\\b"',
      },
   ] as const;

   for (const { connectionType, dialect, tick, slash } of ESCAPES) {
      it(`${connectionType} (${dialect}) escapes a backtick and a backslash`, () => {
         expect(quoteIdentifier("a`b", dialect)).toBe(tick);
         expect(quoteIdentifier("a\\b", dialect)).toBe(slash);
      });
   }
});

describe("quoteTablePath", () => {
   it("quotes each segment of a container path independently", () => {
      expect(
         quoteTablePath(
            "my-proj.mydataset.engaged_events_ab12_v0",
            "standardsql",
         ),
      ).toBe("`my-proj`.`mydataset`.`engaged_events_ab12_v0`");
      expect(quoteTablePath("mydataset.t_ab12_v0", "postgres")).toBe(
         '"mydataset"."t_ab12_v0"',
      );
   });

   it("quotes a bare (unqualified) name", () => {
      expect(quoteTablePath("t_ab12_v0", "standardsql")).toBe("`t_ab12_v0`");
      expect(quoteTablePath("t_ab12_v0", "postgres")).toBe('"t_ab12_v0"');
   });
});

describe("isQuotedIdentifierPath", () => {
   it("is false for a logical, unquoted path (the control-plane form)", () => {
      expect(isQuotedIdentifierPath("schema.order_summary__g0")).toBe(false);
      expect(isQuotedIdentifierPath("order_summary")).toBe(false);
   });

   it("is true once any segment carries a dialect quote char", () => {
      expect(isQuotedIdentifierPath('"schema"."order_summary"')).toBe(true);
      expect(isQuotedIdentifierPath("`ds`.`events`")).toBe(true);
   });
});

// quoteManifestTablePath is the single quoting authority both the serve-side
// bind and the build-side manifest route through. It runs the real
// quoteTablePath (not a re-implementation), so this pins the exact per-dialect
// contract that keeps read (a Malloy FROM) byte-identical to write (the CREATE
// DDL, which also quotes via quoteTablePath) on every dialect — case-folding or
// not.
describe("quoteManifestTablePath", () => {
   it("double-quotes each segment on a case-folding double-quote dialect (Snowflake)", () => {
      expect(
         quoteManifestTablePath("schema.order_summary__g0", "snowflake"),
      ).toBe('"schema"."order_summary__g0"');
   });

   it("double-quotes on Postgres and DuckDB (unchanged for lowercase names, correct for mixed case)", () => {
      expect(quoteManifestTablePath("public.daily", "postgres")).toBe(
         '"public"."daily"',
      );
      expect(quoteManifestTablePath("main.orders_mz", "duckdb")).toBe(
         '"main"."orders_mz"',
      );
   });

   it("backticks each segment on BigQuery (standardsql) — required for hyphenated ids", () => {
      expect(quoteManifestTablePath("my-proj.ds.events", "standardsql")).toBe(
         "`my-proj`.`ds`.`events`",
      );
   });

   it("passes an already-quoted name through unchanged (any dialect)", () => {
      expect(quoteManifestTablePath('"schema"."already"', "snowflake")).toBe(
         '"schema"."already"',
      );
      expect(quoteManifestTablePath("`ds`.`events`", "standardsql")).toBe(
         "`ds`.`events`",
      );
   });
});

describe("sqlTablePathParts", () => {
   // Hostile parts, minus a dot: BigQuery reads a dot inside backticks as a
   // path separator, so a part holding one does not survive a round trip.
   const PARTS = HOSTILE.filter((p) => !p.includes("."));

   for (const dialect of Object.keys(READERS)) {
      it(`${dialect} reads back every hostile part quoteTablePath writes`, () => {
         for (const a of PARTS) {
            for (const b of ["t", ...PARTS]) {
               const quoted = [a, b]
                  .map((p) => quoteIdentifier(p, dialect))
                  .join(".");
               expect(sqlTablePathParts(quoted, dialect)).toEqual([
                  { value: a, quoted: true },
                  { value: b, quoted: true },
               ]);
            }
         }
      });
   }

   it("reads bare and partly quoted paths, keeping bare parts unfolded", () => {
      expect(sqlTablePathParts('"S".t', "snowflake")).toEqual([
         { value: "S", quoted: true },
         { value: "t", quoted: false },
      ]);
      expect(sqlTablePathParts("my-proj.ds.t", "standardsql")).toEqual([
         { value: "my-proj", quoted: false },
         { value: "ds", quoted: false },
         { value: "t", quoted: false },
      ]);
   });

   it("reads one BigQuery backtick pair around a dotted path as the whole path", () => {
      expect(sqlTablePathParts("`p-1.ds.t`", "standardsql")).toEqual([
         { value: "p-1", quoted: true },
         { value: "ds", quoted: true },
         { value: "t", quoted: true },
      ]);
   });

   it("refuses a path that does not parse, or that holds a part quoteIdentifier refuses", () => {
      const refused: [string, string][] = [
         ["`ds`.`t", "standardsql"],
         ['"s"."t', "postgres"],
         ['"s"x.t', "postgres"],
         ['s."t"."', "postgres"],
         ["s..t", "postgres"],
         ['"s".', "postgres"],
         ['"".t', "postgres"],
         ['s"x.t', "postgres"],
         ["`ds`.`a\\nb`", "standardsql"],
         ["`ds`.`a\nb`", "standardsql"],
         ['"s"."a\u0000b"', "postgres"],
         ["`ds`.`t\\`", "standardsql"],
         // A doubled backtick is two quotes on BigQuery: the part ends early.
         ["`ds`.`a``b`", "standardsql"],
      ];
      for (const [path, dialect] of refused) {
         expect(sqlTablePathParts(path, dialect)).toBeUndefined();
      }
   });
});

describe("quoteManifestTablePath on a path written as SQL", () => {
   it("re-quotes each part, folding a bare one as the dialect does", () => {
      expect(quoteManifestTablePath('"S".t', "snowflake")).toBe('"S"."T"');
      expect(quoteManifestTablePath('"S".T', "postgres")).toBe('"S"."t"');
      expect(quoteManifestTablePath("`p-1.ds.t`", "standardsql")).toBe(
         "`p-1`.`ds`.`t`",
      );
      expect(quoteManifestTablePath("`ds`.`a\\`b`", "standardsql")).toBe(
         "`ds`.`a\\`b`",
      );
   });

   it("never inlines a path as given: one that does not parse is refused", () => {
      expect(() =>
         quoteManifestTablePath("`ds`.`a``b`", "standardsql"),
      ).toThrow(UnquotableIdentifierError);
      expect(() => quoteManifestTablePath('"s"."t', "postgres")).toThrow(
         UnquotableIdentifierError,
      );
   });

   it("keeps a plain dotted path's case, as quoteTablePath does", () => {
      expect(quoteManifestTablePath("Sch.Orders", "snowflake")).toBe(
         '"Sch"."Orders"',
      );
   });
});

describe("quoteRenameTarget", () => {
   it("qualifies the target on Snowflake, which resolves a bare one against the session", () => {
      expect(quoteRenameTarget("MY_SCHEMA.orders_v3", "snowflake")).toBe(
         '"MY_SCHEMA"."orders_v3"',
      );
   });

   it("qualifies every segment of a catalog-qualified Snowflake path", () => {
      expect(
         quoteRenameTarget("WAREHOUSE.MY_SCHEMA.orders_v3", "snowflake"),
      ).toBe('"WAREHOUSE"."MY_SCHEMA"."orders_v3"');
   });

   it("qualifies the target on MySQL, which also resolves a bare one against the session", () => {
      expect(quoteRenameTarget("mydb.orders_v3", "mysql")).toBe(
         "`mydb`.`orders_v3`",
      );
   });

   it("qualifies the target on Trino, whose connectors differ on a bare one", () => {
      expect(quoteRenameTarget("s1.orders_v3", "trino")).toBe(
         '"s1"."orders_v3"',
      );
   });

   it("keeps the target bare on dialects that reject a qualified one", () => {
      // Postgres, BigQuery and DuckDB all name the table within its existing
      // container and refuse a qualified rename target.
      expect(quoteRenameTarget("public.orders_v3", "postgres")).toBe(
         '"orders_v3"',
      );
      expect(quoteRenameTarget("ds.orders_v3", "standardsql")).toBe(
         "`orders_v3`",
      );
      expect(quoteRenameTarget("main.orders_v3", "duckdb")).toBe('"orders_v3"');
   });

   it("is a no-op on an already-unqualified name", () => {
      expect(quoteRenameTarget("orders_v3", "snowflake")).toBe('"orders_v3"');
      expect(quoteRenameTarget("orders_v3", "postgres")).toBe('"orders_v3"');
   });
});
