// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The dialect/connector contract for ad hoc SQL sent to the connection
 * `sqlQuery` endpoints.
 *
 * Some dialects finalize a query by collapsing its result into a single JSON
 * column named `row` (`Dialect.hasFinalStage`; Postgres is the only one today,
 * pinned by the TRIPWIRE in incremental_compiler_contract.spec.ts). Their
 * connectors unwrap that column on the way back out -- `PostgresConnection`
 * does `rows[i] = rows[i].row` in `runSQL` and yields `row.row` from
 * `runSQLStream` -- so a statement that does NOT produce the column comes back
 * as nullish rows rather than data. `service/db_utils.ts` and
 * `incremental_apply.ts`'s `probeSelect` both honor this for the SQL they
 * author themselves; this module is the same contract for SQL the CALLER
 * authors, where the statement's shape is not known in advance.
 *
 * The hard part is not the wrapper, it is deciding when to apply it. The
 * endpoint carries two contracts at once:
 *
 *   - Raw SQL from a human, an agent, or the hammer harness, which wants the
 *     wrapper applied so `SELECT 1` returns a row.
 *   - `@malloydata/db-publisher`'s transport: a `publisher` connection reports
 *     the REMOTE's dialectName, so the local Malloy compiler has ALREADY
 *     finalized the statement before it is sent. Wrapping again adds a level
 *     the connector does not strip, and the caller reads fields off
 *     `{"row": {...}}` -- nulls, not an error. That path is used by the Malloy
 *     CLI and the VS Code extensions, whose versions this server does not
 *     control, so silently corrupting it is not an available trade.
 *
 * Nothing in the request distinguishes them, so eligibility is decided from
 * the statement itself, and every test below fails toward passing the
 * statement through unchanged. A statement wrongly passed through returns the
 * actionable 400 raised in stream_helpers; a statement wrongly wrapped returns
 * wrong data.
 */

import {
   DatabricksDialect,
   type Dialect,
   DuckDBDialect,
   MySQLDialect,
   PostgresDialect,
   SnowflakeDialect,
   StandardSQLDialect,
   TrinoDialect,
} from "@malloydata/malloy";

/**
 * Dialect instances by `dialectName`, built from the classes the SDK exports
 * (`getDialect` is not on the package's public surface). Same construction as
 * the compiler-contract tripwire, so the two agree on which dialects exist.
 */
let dialectsByName: Map<string, Dialect> | undefined;

function dialectFor(dialectName: string): Dialect | undefined {
   if (!dialectsByName) {
      dialectsByName = new Map(
         [
            new PostgresDialect(),
            new StandardSQLDialect(),
            new DuckDBDialect(),
            new SnowflakeDialect(),
            new TrinoDialect(),
            new MySQLDialect(),
            new DatabricksDialect(),
         ].map((d) => [d.name, d] as const),
      );
   }
   return dialectsByName.get(dialectName);
}

/**
 * The distinctive call in a dialect's final stage -- `row_to_json` for
 * Postgres -- read off the dialect rather than hardcoded, so a dialect that
 * gains a differently-named final stage is covered without an edit here.
 * Undefined when the final stage has no leading call to key on, which makes
 * the statement ineligible and so passes it through.
 */
function finalStageMarker(dialect: Dialect): string | undefined {
   return /(\w+)\s*\(/.exec(dialect.sqlFinalStage("__probe", []))?.[1];
}

/** Leading whitespace and comments removed, so the first keyword is readable. */
function statementHead(sql: string): string {
   let rest = sql;
   for (;;) {
      const trimmed = rest
         .replace(/^\s+/, "")
         .replace(/^--[^\n]*(?:\n|$)/, "")
         .replace(/^\/\*[\s\S]*?\*\//, "");
      if (trimmed === rest) return trimmed;
      rest = trimmed;
   }
}

/**
 * Statements that can legally sit in the subquery position the wrapper puts
 * them in. Everything else -- DDL, `SET`, `EXPLAIN`, `SHOW`, `CALL` -- is
 * passed through, which is also what those statements need: they return no
 * rows for the connector to unwrap, so they already work.
 */
const WRAPPABLE_HEAD = /^(?:select|with|values|\()/i;

/**
 * Keywords that make a statement illegal in a subquery position even when it
 * opens with a wrappable head: a data-modifying CTE
 * (`WITH x AS (INSERT ... RETURNING ...) SELECT ...`) and Postgres's
 * `SELECT ... INTO`, which is table creation rather than projection. Both work
 * through this endpoint today, so wrapping them would turn a working statement
 * into a syntax error. Matched as bare words anywhere in the statement: a hit
 * inside a string literal or an identifier costs a passthrough, which is the
 * safe direction.
 */
const NOT_SUBQUERY_SAFE = /\b(?:insert|update|delete|merge|into)\b/i;

/** True when this dialect's connector unwraps a `row` column from every row. */
export function dialectHasFinalStage(dialectName: string): boolean {
   return dialectFor(dialectName)?.hasFinalStage ?? false;
}

/**
 * Whether `sql` should be finalized by {@link wrapFinalStage} before it is
 * handed to the connector.
 *
 * The already-finalized test is "mentions the dialect's final-stage call at
 * all" rather than a match on the shape Malloy emits. It is deliberately the
 * blunter of the two: a compiled query always mentions it, so the transport
 * path is safe by construction and stays safe if Malloy changes how the final
 * stage is composed. The cost is that a caller's own SQL mentioning
 * `row_to_json` is passed through and answered with the 400, rather than being
 * wrapped -- a loud failure for a rare hand-written statement, traded against
 * a silent one for every proxied query.
 */
export function shouldWrapFinalStage(
   dialectName: string,
   sql: string,
): boolean {
   const dialect = dialectFor(dialectName);
   if (!dialect?.hasFinalStage) return false;

   const marker = finalStageMarker(dialect);
   if (marker === undefined) return false;
   if (new RegExp(`\\b${marker}\\b`, "i").test(sql)) return false;

   if (!WRAPPABLE_HEAD.test(statementHead(sql))) return false;
   if (NOT_SUBQUERY_SAFE.test(sql)) return false;
   return true;
}

/**
 * `sql` finalized the way the dialect finalizes a compiled query, so the
 * connector's unwrap finds the column it expects and the caller gets plain row
 * objects. The trailing semicolon a caller may include is dropped: it is legal
 * on its own and illegal inside the CTE.
 */
export function wrapFinalStage(dialectName: string, sql: string): string {
   const dialect = dialectFor(dialectName);
   if (!dialect?.hasFinalStage) return sql;
   const stage = "__publisher_raw_sql";
   const body = sql.replace(/;\s*$/, "");
   return `WITH ${stage} AS (\n${body}\n) ${dialect.sqlFinalStage(stage, [])}`;
}

/**
 * The message for a statement that reached the connector without the column
 * its unwrap expects. Names the wrapper in the dialect's own terms so the
 * caller can apply it by hand.
 */
export function finalStageContractMessage(dialectName: string): string {
   const dialect = dialectFor(dialectName);
   const example = dialect?.hasFinalStage
      ? ` Wrap it as: WITH t AS (<your statement>) ${dialect.sqlFinalStage("t", [])}`
      : "";
   return `The statement returned rows the ${dialectName} driver could not read. This connection's driver unwraps a single column named "row" from every row, which the statement did not produce.${example}`;
}
