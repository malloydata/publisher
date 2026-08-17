import { describe, expect, it } from "bun:test";

import {
   bigQueryQueryLabelValue,
   snowflakeQueryTagValue,
   snowflakeSetQueryTagSQL,
} from "./build_query_tag";

describe("snowflakeQueryTagValue", () => {
   it("renders the bag as JSON so the properties stay queryable", () => {
      expect(
         snowflakeQueryTagValue({ team: "finance", cred_org: "acme" }),
      ).toBe('{"team":"finance","cred_org":"acme"}');
   });

   it("preserves case", () => {
      // QUERY_HISTORY is read by humans and by joins against other systems'
      // identifiers; folding case here would silently rename both.
      expect(snowflakeQueryTagValue({ Team: "Finance" })).toBe(
         '{"Team":"Finance"}',
      );
   });

   it("has no tag for an empty or absent bag", () => {
      expect(snowflakeQueryTagValue({})).toBeUndefined();
      expect(snowflakeQueryTagValue(undefined)).toBeUndefined();
   });

   it("drops an over-long tag rather than truncating it into invalid JSON", () => {
      // Truncated JSON is unparseable, and a consumer cannot tell that from a
      // bug. Absent reads as absent.
      const huge = { k: "x".repeat(2100) };
      expect(snowflakeQueryTagValue(huge)).toBeUndefined();
   });
});

describe("snowflakeSetQueryTagSQL", () => {
   it("tags the session, through the passthrough, carrying the secret", () => {
      const sql = snowflakeSetQueryTagSQL({ team: "finance" }, "sf_secret");
      expect(sql).toStartWith("SELECT * FROM snowflake_query(");
      expect(sql).toContain("ALTER SESSION SET QUERY_TAG");
      expect(sql).toContain("'sf_secret'");
   });

   it("emits nothing when there is nothing to tag", () => {
      expect(snowflakeSetQueryTagSQL({}, "s")).toBeUndefined();
      expect(snowflakeSetQueryTagSQL(undefined, "s")).toBeUndefined();
   });

   it("escapes a backslash for BOTH nesting levels, so the stored tag still parses", () => {
      // The failure this pins is silent and not a syntax error. A property value
      // may contain a backslash (the contract admits printable ASCII except `"`),
      // JSON renders it `\\`, and Snowflake treats backslash as an escape inside
      // a single-quoted literal. Doubling quotes only would deliver `\` where the
      // JSON needed `\\`: the statement succeeds, and the tag it stores is
      // unparseable in QUERY_HISTORY.
      const sql = snowflakeSetQueryTagSQL({ path: "c:\\src" }, "s")!;

      // Recover the Snowflake-level statement, then what Snowflake reads from it.
      const snowflakeSees = unwrapDuckDBLiteral(sql);
      const tag = snowflakeReadsLiteral(
         snowflakeSees.slice(snowflakeSees.indexOf("'")),
      );

      expect(JSON.parse(tag)).toEqual({ path: "c:\\src" });
   });

   it("escapes a quote in a value without terminating the literal early", () => {
      const sql = snowflakeSetQueryTagSQL({ team: "o'brien" }, "s")!;
      const snowflakeSees = unwrapDuckDBLiteral(sql);
      const tag = snowflakeReadsLiteral(
         snowflakeSees.slice(snowflakeSees.indexOf("'")),
      );
      expect(JSON.parse(tag)).toEqual({ team: "o'brien" });
   });
});

describe("bigQueryQueryLabelValue", () => {
   it("renders key:value pairs joined by commas", () => {
      expect(
         bigQueryQueryLabelValue({ cred_run: "abc", cred_class: "ops" }),
      ).toBe("cred_run:abc,cred_class:ops");
   });

   it("lowercases and substitutes anything outside BigQuery's grammar", () => {
      expect(bigQueryQueryLabelValue({ Team: "Acme Corp" })).toBe(
         "team:acme_corp",
      );
   });

   it("removes the separators a value could otherwise inject", () => {
      // `,` and `:` are outside [a-z0-9_-], so the join cannot be confused by a
      // property value. This is what makes the joined form safe by construction
      // rather than by the caller being careful.
      expect(bigQueryQueryLabelValue({ k: "a,b:c" })).toBe("k:a_b_c");
   });

   it("removes quote and backslash, so the value cannot disturb its SQL literal", () => {
      expect(bigQueryQueryLabelValue({ k: "o'brien\\x" })).toBe("k:o_brien_x");
   });

   it("dedupes keys that collide once sanitized, rather than failing the build", () => {
      // Property names are validated case-PRESERVING, so `Team` and `team` are
      // two legal properties that both render as `team`. BigQuery refuses a label
      // list with a duplicate key outright — verified against a live project:
      // "Invalid query label list 'team:a,team:b'. Duplicate..." — which fails the
      // script and kills the build. Last wins.
      expect(bigQueryQueryLabelValue({ Team: "a", team: "b" })).toBe("team:b");
   });

   it("drops a key that cannot start with a lowercase letter", () => {
      // BigQuery rejects it outright, so sending it would fail the whole job.
      expect(bigQueryQueryLabelValue({ "1st": "x", ok: "y" })).toBe("ok:y");
   });

   it("has no label when every key was dropped, rather than an empty string", () => {
      expect(bigQueryQueryLabelValue({ "1st": "x" })).toBeUndefined();
      expect(bigQueryQueryLabelValue({})).toBeUndefined();
      expect(bigQueryQueryLabelValue(undefined)).toBeUndefined();
   });

   it("truncates to BigQuery's 63-character ceiling", () => {
      expect(bigQueryQueryLabelValue({ k: "x".repeat(80) })).toBe(
         `k:${"x".repeat(63)}`,
      );
   });
});

/** Undo one DuckDB string literal (quote-doubling only — no backslash escape). */
function unwrapDuckDBLiteral(sql: string): string {
   const start = sql.indexOf("'");
   const body = sql.slice(start + 1);
   return body.slice(0, body.lastIndexOf("', '")).replace(/''/g, "'");
}

/** What Snowflake reads from a single-quoted literal: `''` -> `'`, `\X` -> `X`. */
function snowflakeReadsLiteral(literal: string): string {
   let out = "";
   let i = 1;
   while (i < literal.length) {
      if (literal[i] === "\\") {
         out += literal[i + 1];
         i += 2;
         continue;
      }
      if (literal[i] === "'" && literal[i + 1] === "'") {
         out += "'";
         i += 2;
         continue;
      }
      if (literal[i] === "'") return out;
      out += literal[i++];
   }
   return out;
}
