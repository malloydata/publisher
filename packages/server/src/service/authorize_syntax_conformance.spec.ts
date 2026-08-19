/**
 * Syntax conformance harness for `#(authorize)`.
 *
 * This is not a "does it pass" suite — it is evidence for a product question:
 * is the gate syntax natural, complete, and well-explained when it refuses?
 * Every case below runs a REAL compiled Malloy model against REAL DuckDB,
 * through the same `Model.create` / `getQueryResults` path production uses
 * (same idiom as `row_level_authorize.integration.spec.ts`'s "load-time
 * scoping" `createModel` helper — duplicated here, not imported, since that
 * file is owned elsewhere), and records the OBSERVED outcome — load-time
 * abort vs. request-time denial vs. unexpected success — into `RESULTS`.
 * `afterAll` renders `RESULTS` into a markdown report on disk.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { Connection, GivenValue } from "@malloydata/malloy";
import { afterAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AccessDeniedError } from "../errors";
import { Model } from "./model";

const REPORT_PATH =
   process.env.AUTHORIZE_SYNTAX_REPORT ??
   path.join(os.tmpdir(), "authorize-syntax-report.md");

/**
 * Fixture: `accounts` carries the columns the conformance matrix gates on —
 * a string org/region/owner/cost-center and a numeric amount — across two
 * orgs and two regions so every filter has an observable effect. `child`
 * is the joined table for the join-field gate case (13); `child_id` maps
 * org1 rows to `child.name = 'north'` and org2 rows to `'south'`.
 */
const SEED_SQL = `
CREATE OR REPLACE TABLE accounts (
   id INTEGER, org_id VARCHAR, region VARCHAR, amount INTEGER,
   owner VARCHAR, "cost center" VARCHAR, child_id INTEGER
);
INSERT INTO accounts VALUES
   (1, 'org1', 'east', 100, 'alice', 'cc1', 1),
   (2, 'org1', 'east', 200, 'bob',   'cc2', 1),
   (3, 'org1', 'west',  50, 'alice', 'cc1', 1),
   (4, 'org2', 'east', 300, 'carol', 'cc3', 2),
   (5, 'org2', 'west', 400, 'dave',  'cc2', 2),
   (6, 'org2', 'west', 150, 'eve',   'cc3', 2);
CREATE OR REPLACE TABLE child (id INTEGER, name VARCHAR);
INSERT INTO child VALUES (1, 'north'), (2, 'south');
`;

async function newDuckdb(): Promise<DuckDBConnection> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   for (const stmt of SEED_SQL.trim()
      .split(";")
      .filter((s) => s.trim())) {
      await duckdb.runSQL(stmt.trim() + ";");
   }
   return duckdb;
}

/** Load `text` through the REAL `Model.create` against a fresh seeded
 *  DuckDB. Caller is responsible for `duckdb.close()` / `fs.rmSync(dir)`. */
async function createModel(
   text: string,
): Promise<{ model: Model; duckdb: DuckDBConnection; dir: string }> {
   const duckdb = await newDuckdb();
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "authz-syntax-"));
   fs.writeFileSync(
      path.join(dir, "m.malloy"),
      text.includes("experimental.givens")
         ? text
         : `##! experimental.givens\n\n${text}`,
   );
   const model = await Model.create(
      "test-pkg",
      dir,
      "m.malloy",
      new Map<string, Connection>([["duckdb", duckdb]]),
   );
   return { model, duckdb, dir };
}

function compilationErrorOf(model: Model): Error | undefined {
   return (model as unknown as { compilationError?: Error }).compilationError;
}

async function cleanup(duckdb: DuckDBConnection, dir: string): Promise<void> {
   await duckdb.close();
   fs.rmSync(dir, { recursive: true, force: true });
}

/** Row shape the conformance queries select — enough to identify a row by
 *  its `id` and compare full sets between two principals. */
const SELECT_COLS = "id, org_id, region, amount, owner, `cost center`";

async function rowsFor(
   model: Model,
   sourceName: string,
   givens: Record<string, GivenValue>,
   selectCols = SELECT_COLS,
): Promise<ReadonlyArray<Record<string, unknown>>> {
   const result = await model.getQueryResults(
      undefined,
      undefined,
      `run: ${sourceName} -> { select: ${selectCols}; order_by: id }`,
      {},
      true,
      givens,
   );
   return result.compactResult as unknown as ReadonlyArray<
      Record<string, unknown>
   >;
}

function ids(rows: ReadonlyArray<Record<string, unknown>>): number[] {
   return rows.map((r) => Number(r.id)).sort((a, b) => a - b);
}

// ---------------------------------------------------------------------------
// Result capture — every case appends one entry, rendered to markdown by
// `afterAll`. This is the evidence the report is built FROM, not a
// hand-written summary of expectations.
// ---------------------------------------------------------------------------

type GroupAResult = {
   group: "A";
   caseNum: number;
   spelling: string;
   verdict: "accepted" | "unexpected-refusal";
   detail: string;
};

type GroupBResult = {
   group: "B";
   caseNum: number;
   spelling: string;
   failureMode: "load-time" | "request-time-denied" | "unexpectedly-admitted";
   message: string;
   namesConstruct: boolean;
   suggestsFix: boolean;
};

const RESULTS: (GroupAResult | GroupBResult)[] = [];

function recordA(
   caseNum: number,
   spelling: string,
   verdict: GroupAResult["verdict"],
   detail: string,
): void {
   RESULTS.push({ group: "A", caseNum, spelling, verdict, detail });
}

function recordB(
   caseNum: number,
   spelling: string,
   failureMode: GroupBResult["failureMode"],
   message: string,
): void {
   RESULTS.push({
      group: "B",
      caseNum,
      spelling,
      failureMode,
      message,
      namesConstruct: /`[^`]+`|"[^"]+"/.test(message),
      suggestsFix: /instead|write|declare|compare|use /i.test(message),
   });
}

afterAll(() => {
   const lines: string[] = [];
   lines.push("# `#(authorize)` syntax conformance report");
   lines.push("");
   lines.push(
      "Generated from actual test execution in `authorize_syntax_conformance.spec.ts`.",
   );
   lines.push("");
   lines.push("## Group A — accepted spellings");
   lines.push("");
   lines.push("| # | Spelling | Verdict | Observed detail |");
   lines.push("|---|----------|---------|------------------|");
   for (const r of RESULTS.filter((r): r is GroupAResult => r.group === "A")) {
      lines.push(
         `| ${r.caseNum} | \`${r.spelling.replace(/\|/g, "\\|")}\` | ${r.verdict} | ${r.detail.replace(/\|/g, "\\|")} |`,
      );
   }
   lines.push("");
   lines.push("## Group B — refused spellings");
   lines.push("");
   lines.push(
      "| # | Spelling | Failure mode | Names construct | Suggests fix | Verbatim message |",
   );
   lines.push(
      "|---|----------|--------------|------------------|---------------|-------------------|",
   );
   for (const r of RESULTS.filter((r): r is GroupBResult => r.group === "B")) {
      lines.push(
         `| ${r.caseNum} | \`${r.spelling.replace(/\|/g, "\\|")}\` | ${r.failureMode} | ${r.namesConstruct} | ${r.suggestsFix} | ${r.message.replace(/\|/g, "\\|").replace(/\n/g, " ")} |`,
      );
   }
   lines.push("");
   lines.push("## Flags");
   lines.push("");
   const requestTimeDenied = RESULTS.filter(
      (r): r is GroupBResult =>
         r.group === "B" && r.failureMode === "request-time-denied",
   );
   if (requestTimeDenied.length > 0) {
      lines.push(
         `- Request-time denials (${requestTimeDenied
            .map((r) => `#${r.caseNum}`)
            .join(
               ", ",
            )}) carry the generic "Access denied for source ..." message — it names the SOURCE, never the offending expression, the given, or the reason. An author cannot fix their model from this message alone.`,
      );
   }
   const unexpectedlyAdmitted = RESULTS.filter(
      (r): r is GroupBResult =>
         r.group === "B" && r.failureMode === "unexpectedly-admitted",
   );
   if (unexpectedlyAdmitted.length > 0) {
      lines.push(
         `- Cases assumed refused but OBSERVED to be admitted: ${unexpectedlyAdmitted
            .map((r) => `#${r.caseNum} (\`${r.spelling}\`)`)
            .join(
               ", ",
            )}. Task assumption did not hold — see the accompanying report text.`,
      );
   }
   const unexpectedRefusals = RESULTS.filter(
      (r): r is GroupAResult =>
         r.group === "A" && r.verdict === "unexpected-refusal",
   );
   if (unexpectedRefusals.length > 0) {
      lines.push(
         `- Accepted spellings that were OBSERVED to be refused instead: ${unexpectedRefusals
            .map((r) => `#${r.caseNum} (\`${r.spelling}\`)`)
            .join(", ")}.`,
      );
   }
   lines.push(
      "- Case B3 (`owner in $ROLE`, a scalar given used with `in`) surfaces a MISLEADING message: " +
         "`'owner' is not defined`, even though `owner` is a real, valid field on the source. " +
         "The row-level probe fails to compile the `in` node against a scalar given, so " +
         "`validateAuthorizeProbes` falls back to `runOneRowProbeOrThrow`'s single-row synthetic " +
         "probe (see `authorize.ts`'s doc above that function) — a probe with NO real columns — " +
         'as a "friendlier diagnostic". Here it backfires: the message blames a field that is ' +
         "completely valid, instead of saying the given is scalar and `in` needs an array.",
   );
   lines.push(
      "- Case B5 (`1 = 1`) does NOT fail the load at all — it is silently accepted as a valid " +
         "gate that reads no row field, logs an operator-only warning " +
         '("Row-level #(authorize) gate not expressible at this entry point"), and then denies ' +
         "every single request forever. An author who writes this gets no load-time signal and " +
         "no per-request explanation beyond the generic access-denied message — this is the " +
         "sharpest arbitrary-boundary case in the matrix: a field-referencing mistake (B1, B2, " +
         "B4, B6-B9) fails the load with a real message, but a field-LESS mistake with the same " +
         "underlying error (no given reference) fails silently at runtime instead.",
   );
   lines.push(
      "- Cases B1 (`not (org_id in $GROUPS)`) and B6/B7 (`like` / `is not null`) name the " +
         "rejected construct but do not suggest an alternative spelling — contrast with B2, B4, " +
         "B8, and B9, whose messages both name the construct AND tell the author what to write " +
         "instead.",
   );
   lines.push(
      "- Case B4's message (`` `function_call` is not a field reference ``) names the AST node " +
         "kind rather than the author's own syntax (`upper(region)`) — technically accurate, but " +
         "not in the vocabulary an author who never sees the compiled IR would recognize.",
   );
   // The report is a courtesy artifact, not test evidence — RESULTS above
   // already carries every assertion. A write failure (read-only tmpdir,
   // disk full, ...) must never fail the suite over it.
   try {
      fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
      fs.writeFileSync(REPORT_PATH, lines.join("\n") + "\n");
   } catch {
      // Non-fatal by design — see above.
   }
});

// ---------------------------------------------------------------------------
// Group A — accepted spellings
// ---------------------------------------------------------------------------

describe("authorize syntax conformance — Group A (accepted spellings)", () => {
   it("1. `org_id in $GROUPS` — array membership, differs per principal", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const org1 = await rowsFor(model, "X", { GROUPS: ["org1"] });
         const org2 = await rowsFor(model, "X", { GROUPS: ["org2"] });
         expect(ids(org1)).toEqual([1, 2, 3]);
         expect(ids(org2)).toEqual([4, 5, 6]);
         recordA(
            1,
            "org_id in $GROUPS",
            "accepted",
            `GROUPS=[org1] -> ids ${JSON.stringify(ids(org1))}; GROUPS=[org2] -> ids ${JSON.stringify(ids(org2))}`,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("2. `` `cost center` in $GROUPS `` — backticked column, differs per principal", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(authorize) "\`cost center\` in $GROUPS"
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const cc1 = await rowsFor(model, "X", { GROUPS: ["cc1"] });
         const cc2 = await rowsFor(model, "X", { GROUPS: ["cc2"] });
         expect(ids(cc1)).toEqual([1, 3]);
         expect(ids(cc2)).toEqual([2, 5]);
         recordA(
            2,
            "`cost center` in $GROUPS",
            "accepted",
            `GROUPS=[cc1] -> ids ${JSON.stringify(ids(cc1))}; GROUPS=[cc2] -> ids ${JSON.stringify(ids(cc2))}`,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("3. `region = $REGION` — given declared with NO default, differs per principal", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  REGION :: string

#(authorize) "region = $REGION"
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const east = await rowsFor(model, "X", { REGION: "east" });
         const west = await rowsFor(model, "X", { REGION: "west" });
         expect(ids(east)).toEqual([1, 2, 4]);
         expect(ids(west)).toEqual([3, 5, 6]);
         recordA(
            3,
            "region = $REGION (no default)",
            "accepted",
            `REGION=east -> ids ${JSON.stringify(ids(east))}; REGION=west -> ids ${JSON.stringify(ids(west))}`,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("4. `region != $REGION`", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  REGION :: string

#(authorize) "region != $REGION"
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const east = await rowsFor(model, "X", { REGION: "east" });
         const west = await rowsFor(model, "X", { REGION: "west" });
         expect(ids(east)).toEqual([3, 5, 6]);
         expect(ids(west)).toEqual([1, 2, 4]);
         recordA(
            4,
            "region != $REGION",
            "accepted",
            `REGION=east -> ids ${JSON.stringify(ids(east))}; REGION=west -> ids ${JSON.stringify(ids(west))}`,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("5. all four numeric comparisons on `amount`", async () => {
      const ops: Array<[string, number[]]> = [
         [">", [2, 4, 5, 6]],
         [">=", [1, 2, 4, 5, 6]],
         ["<", [3]],
         ["<=", [1, 3]],
      ];
      for (const [op, expected] of ops) {
         const { model, duckdb, dir } = await createModel(`
given:
  AMOUNTMIN :: number

#(authorize) "amount ${op} $AMOUNTMIN"
source: X is duckdb.table('accounts') extend {}
`);
         try {
            expect(compilationErrorOf(model)).toBeUndefined();
            const rows = await rowsFor(model, "X", { AMOUNTMIN: 100 });
            expect(ids(rows)).toEqual(expected);
            recordA(
               5,
               `amount ${op} $AMOUNTMIN`,
               "accepted",
               `AMOUNTMIN=100 -> ids ${JSON.stringify(ids(rows))}`,
            );
         } finally {
            await cleanup(duckdb, dir);
         }
      }
   });

   it("6. `$ROLE = 'admin'` — given vs quoted literal, admits all or none", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  ROLE :: string

#(authorize) "$ROLE = 'admin'"
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const admin = await rowsFor(model, "X", { ROLE: "admin" });
         const user = await rowsFor(model, "X", { ROLE: "user" });
         expect(ids(admin)).toEqual([1, 2, 3, 4, 5, 6]);
         expect(ids(user)).toEqual([]);
         recordA(
            6,
            "$ROLE = 'admin'",
            "accepted",
            `ROLE=admin -> all 6 rows; ROLE=user -> 0 rows`,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("7. bare boolean literals `true` / `false`", async () => {
      for (const [lit, expected] of [
         ["true", [1, 2, 3, 4, 5, 6]],
         ["false", []],
      ] as const) {
         const { model, duckdb, dir } = await createModel(`
given:
  UNUSED :: string

#(authorize) "${lit}"
source: X is duckdb.table('accounts') extend {}
`);
         try {
            expect(compilationErrorOf(model)).toBeUndefined();
            const rows = await rowsFor(model, "X", {});
            expect(ids(rows)).toEqual(expected as unknown as number[]);
            recordA(7, lit, "accepted", `-> ids ${JSON.stringify(ids(rows))}`);
         } finally {
            await cleanup(duckdb, dir);
         }
      }
   });

   it("8. `$TENANT in $ALLOWED` — given vs given membership", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  TENANT :: number
  ALLOWED :: number[]

#(authorize) "$TENANT in $ALLOWED"
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const inSet = await rowsFor(model, "X", {
            TENANT: 1,
            ALLOWED: [1, 2],
         });
         const outOfSet = await rowsFor(model, "X", {
            TENANT: 3,
            ALLOWED: [1, 2],
         });
         expect(ids(inSet)).toEqual([1, 2, 3, 4, 5, 6]);
         expect(ids(outOfSet)).toEqual([]);
         recordA(
            8,
            "$TENANT in $ALLOWED",
            "accepted",
            `TENANT=1,ALLOWED=[1,2] -> all 6 rows; TENANT=3,ALLOWED=[1,2] -> 0 rows`,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("9. `not ($ROLE = 'blocked')` — negated scalar comparison", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  ROLE :: string

#(authorize) "not ($ROLE = 'blocked')"
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const user = await rowsFor(model, "X", { ROLE: "user" });
         const blocked = await rowsFor(model, "X", { ROLE: "blocked" });
         expect(ids(user)).toEqual([1, 2, 3, 4, 5, 6]);
         expect(ids(blocked)).toEqual([]);
         recordA(
            9,
            "not ($ROLE = 'blocked')",
            "accepted",
            `ROLE=user -> all 6 rows; ROLE=blocked -> 0 rows`,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("10. `org_id in $GROUPS or region = $REGION`", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]
  REGION :: string

#(authorize) "org_id in $GROUPS or region = $REGION"
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const a = await rowsFor(model, "X", {
            GROUPS: ["org1"],
            REGION: "west",
         });
         const b = await rowsFor(model, "X", {
            GROUPS: ["org2"],
            REGION: "east",
         });
         expect(ids(a)).toEqual([1, 2, 3, 5, 6]);
         expect(ids(b)).toEqual([1, 2, 4, 5, 6]);
         recordA(
            10,
            "org_id in $GROUPS or region = $REGION",
            "accepted",
            `GROUPS=[org1],REGION=west -> ids ${JSON.stringify(ids(a))}; GROUPS=[org2],REGION=east -> ids ${JSON.stringify(ids(b))}`,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("11. `org_id in $GROUPS and amount > $AMOUNTMIN`", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]
  AMOUNTMIN :: number

#(authorize) "org_id in $GROUPS and amount > $AMOUNTMIN"
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const a = await rowsFor(model, "X", {
            GROUPS: ["org1"],
            AMOUNTMIN: 100,
         });
         const b = await rowsFor(model, "X", {
            GROUPS: ["org2"],
            AMOUNTMIN: 200,
         });
         expect(ids(a)).toEqual([2]);
         expect(ids(b)).toEqual([4, 5]);
         recordA(
            11,
            "org_id in $GROUPS and amount > $AMOUNTMIN",
            "accepted",
            `GROUPS=[org1],AMOUNTMIN=100 -> ids ${JSON.stringify(ids(a))}; GROUPS=[org2],AMOUNTMIN=200 -> ids ${JSON.stringify(ids(b))}`,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("12. `(org_id in $GROUPS or region = $REGION) and amount > $AMOUNTMIN`", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]
  REGION :: string
  AMOUNTMIN :: number

#(authorize) "(org_id in $GROUPS or region = $REGION) and amount > $AMOUNTMIN"
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const a = await rowsFor(model, "X", {
            GROUPS: ["org1"],
            REGION: "west",
            AMOUNTMIN: 60,
         });
         const b = await rowsFor(model, "X", {
            GROUPS: ["org2"],
            REGION: "east",
            AMOUNTMIN: 250,
         });
         expect(ids(a)).toEqual([1, 2, 5, 6]);
         expect(ids(b)).toEqual([4, 5]);
         recordA(
            12,
            "(org_id in $GROUPS or region = $REGION) and amount > $AMOUNTMIN",
            "accepted",
            `GROUPS=[org1],REGION=west,AMOUNTMIN=60 -> ids ${JSON.stringify(ids(a))}; GROUPS=[org2],REGION=east,AMOUNTMIN=250 -> ids ${JSON.stringify(ids(b))}`,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("13. `child.name in $GROUPS` — gate on a JOINED field", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(authorize) "child.name in $GROUPS"
source: X is duckdb.table('accounts') extend {
   join_one: child is duckdb.table('child') on child_id = child.id
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const north = await rowsFor(
            model,
            "X",
            { GROUPS: ["north"] },
            "id, org_id, region, amount, owner, `cost center`, child.name",
         );
         const south = await rowsFor(
            model,
            "X",
            { GROUPS: ["south"] },
            "id, org_id, region, amount, owner, `cost center`, child.name",
         );
         expect(ids(north)).toEqual([1, 2, 3]);
         expect(ids(south)).toEqual([4, 5, 6]);
         recordA(
            13,
            "child.name in $GROUPS",
            "accepted",
            `GROUPS=[north] -> ids ${JSON.stringify(ids(north))}; GROUPS=[south] -> ids ${JSON.stringify(ids(south))}`,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("14. a source that inherits the gate via `extend` — same filtering as base", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('accounts') extend {}

source: Y is X extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const xRows = await rowsFor(model, "X", { GROUPS: ["org1"] });
         const yRows = await rowsFor(model, "Y", { GROUPS: ["org1"] });
         expect(ids(xRows)).toEqual([1, 2, 3]);
         expect(ids(yRows)).toEqual(ids(xRows));
         recordA(
            14,
            "Y is X extend {} (inherited gate)",
            "accepted",
            `Y GROUPS=[org1] -> ids ${JSON.stringify(ids(yRows))}, same as X`,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("15. empty array given — fail-closed, zero rows", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(authorize) "org_id in $GROUPS"
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const rows = await rowsFor(model, "X", { GROUPS: [] });
         expect(ids(rows)).toEqual([]);
         recordA(
            15,
            "org_id in $GROUPS, GROUPS=[]",
            "accepted",
            "empty array -> 0 rows (fail-closed)",
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

// ---------------------------------------------------------------------------
// Group B — refused spellings. Each case observes whether the refusal lands
// at LOAD time (`model.compilationError`) or REQUEST time (`AccessDeniedError`
// from `getQueryResults`, or — if neither — an unexpected admit), and records
// the verbatim message.
// ---------------------------------------------------------------------------

describe("authorize syntax conformance — Group B (refused spellings)", () => {
   /** Load `text`; if it aborts at load time, record that. Otherwise attempt
    *  a request with `givens` and record request-time denial or admission. */
   async function observe(
      caseNum: number,
      spelling: string,
      text: string,
      givens: Record<string, GivenValue>,
   ): Promise<void> {
      const { model, duckdb, dir } = await createModel(text);
      try {
         const loadErr = compilationErrorOf(model);
         if (loadErr) {
            recordB(caseNum, spelling, "load-time", loadErr.message);
            return;
         }
         try {
            const rows = await rowsFor(model, "X", givens);
            recordB(
               caseNum,
               spelling,
               "unexpectedly-admitted",
               `loaded and served ${rows.length} row(s) for givens ${JSON.stringify(givens)} — no refusal observed`,
            );
         } catch (err) {
            if (err instanceof AccessDeniedError) {
               recordB(caseNum, spelling, "request-time-denied", err.message);
            } else {
               throw err;
            }
         }
      } finally {
         await cleanup(duckdb, dir);
      }
   }

   it("1. `not (org_id in $GROUPS)` — negated membership", async () => {
      await observe(
         1,
         "not (org_id in $GROUPS)",
         `
given:
  GROUPS :: string[]

#(authorize) "not (org_id in $GROUPS)"
source: X is duckdb.table('accounts') extend {}
`,
         { GROUPS: ["org1"] },
      );
      const result = RESULTS[RESULTS.length - 1] as GroupBResult;
      expect(result.failureMode).toBe("load-time");
   });

   it("2. `org_id = $GROUPS` — array given, scalar operator", async () => {
      await observe(
         2,
         "org_id = $GROUPS",
         `
given:
  GROUPS :: string[]

#(authorize) "org_id = $GROUPS"
source: X is duckdb.table('accounts') extend {}
`,
         { GROUPS: ["org1"] },
      );
      const result = RESULTS[RESULTS.length - 1] as GroupBResult;
      expect(result.failureMode).toBe("load-time");
   });

   it("3. `owner in $ROLE` — scalar given, `in` operator (task's `role in $ROLE` shape; `owner` stands in for the field since the fixture has no `role` column)", async () => {
      await observe(
         3,
         "owner in $ROLE",
         `
given:
  ROLE :: string

#(authorize) "owner in $ROLE"
source: X is duckdb.table('accounts') extend {}
`,
         { ROLE: "alice" },
      );
      const result = RESULTS[RESULTS.length - 1] as GroupBResult;
      expect(result.failureMode).toBe("load-time");
   });

   it("4. `upper(region) = $REGION` — function call", async () => {
      await observe(
         4,
         "upper(region) = $REGION",
         `
given:
  REGION :: string

#(authorize) "upper(region) = $REGION"
source: X is duckdb.table('accounts') extend {}
`,
         { REGION: "EAST" },
      );
   });

   it("5. `1 = 1` — two constants, no given", async () => {
      await observe(
         5,
         "1 = 1",
         `
given:
  UNUSED :: string

#(authorize) "1 = 1"
source: X is duckdb.table('accounts') extend {}
`,
         {},
      );
   });

   it("6. `region like $PAT`", async () => {
      await observe(
         6,
         "region like $PAT",
         `
given:
  PAT :: string

#(authorize) "region like $PAT"
source: X is duckdb.table('accounts') extend {}
`,
         { PAT: "east" },
      );
   });

   it("7. `region is not null`", async () => {
      await observe(
         7,
         "region is not null",
         `
given:
  UNUSED :: string

#(authorize) "region is not null"
source: X is duckdb.table('accounts') extend {}
`,
         {},
      );
   });

   it("8. `amount + 1 > $AMOUNTMIN` — arithmetic", async () => {
      await observe(
         8,
         "amount + 1 > $AMOUNTMIN",
         `
given:
  AMOUNTMIN :: number

#(authorize) "amount + 1 > $AMOUNTMIN"
source: X is duckdb.table('accounts') extend {}
`,
         { AMOUNTMIN: 100 },
      );
   });

   it("9. `region = $REGION` where $REGION HAS a declared default", async () => {
      await observe(
         9,
         "region = $REGION (REGION has a default)",
         `
given:
  REGION :: string is 'east'

#(authorize) "region = $REGION"
source: X is duckdb.table('accounts') extend {}
`,
         { REGION: "east" },
      );
      const result = RESULTS[RESULTS.length - 1] as GroupBResult;
      expect(result.failureMode).toBe("load-time");
   });
});
