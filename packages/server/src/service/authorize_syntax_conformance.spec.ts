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
   failureMode:
      | "load-time"
      | "request-time-denied"
      | "unexpectedly-admitted"
      // The DIMENSION form does not classify a gate's expression shape at
      // load time the way the string form's `classifyAuthorizeGate` did —
      // see `./gate_dimension`'s doc. A shape the string form refused
      // outright at load can now compile and graft, only to crash Malloy's
      // own SQL generation at REQUEST time (a genuine Malloy limitation for
      // that shape, orthogonal to authorize). Neither a clean admit nor a
      // graceful `AccessDeniedError` — recorded distinctly so this gap is
      // visible in the report rather than silently swallowed as either.
      | "request-time-execution-error";
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
      "- Case B3 (`owner in $ROLE`, a scalar given used with `in`) fails the load with a CLEAR " +
         "message under the dimension form: Malloy's own compiler rejects `in $ROLE` outright " +
         "(`` `in $ROLE` requires `ROLE` to be an array, but it is `string` ``) before any " +
         "row-level probe runs — the dimension form's gate is a real compiled dimension, so a " +
         "type mismatch in its own expression is an ordinary compile error, not something a " +
         "probe has to diagnose after the fact. (Under the now-deleted string form this same " +
         "shape surfaced a misleading `'owner' is not defined` instead, from a fallback " +
         "single-row synthetic probe with no real columns — see task-4-report.md for the trail; " +
         "that fallback no longer exists.)",
   );
   lines.push(
      "- Case B5 (`1 = 1`, a gate dimension that reads no row field and no given) is now " +
         "OBSERVED-ADMITTED, not a silent runtime deny: it loads cleanly with an operator-only " +
         "warning (`gate_dimension_no_given_reference` — a fixed predicate, not an access rule " +
         "keyed on the caller), and then serves every row to every caller, since `authorized is " +
         "1 = 1` is `decidable` with no given to withhold on (Task 4's routed finding (a) fix to " +
         "`Model.authorizeAndBindRunnable`'s `decidable` check, pinned in " +
         "`compile_authorize.spec.ts`). This reverses the shape this case used to name: a bare " +
         "`1 = 1` no longer denies every request forever — it is the KILL-SWITCH's admit-everyone " +
         "counterpart (`authorized is false` is the deny-everyone one) working as designed, not " +
         "an arbitrary-boundary trap.",
   );
   lines.push(
      "- Of the three cases still genuinely refused with a message (B2, B3, B9), only B9's " +
         "(`region = $REGION` with a defaulted given) tells the author what to write instead " +
         "(`Declare $REGION with no default`). B2's DuckDB conversion error and B3's Malloy " +
         "compile diagnostic both name the offending construct but stop short of suggesting an " +
         "alternative spelling.",
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

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is org_id in $GROUPS
}
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

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is \`cost center\` in $GROUPS
}
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

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is region = $REGION
}
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

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is region != $REGION
}
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

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is amount ${op} $AMOUNTMIN
}
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

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is $ROLE = 'admin'
}
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

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is ${lit}
}
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

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is $TENANT in $ALLOWED
}
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

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is not ($ROLE = 'blocked')
}
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

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is org_id in $GROUPS or region = $REGION
}
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

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is org_id in $GROUPS and amount > $AMOUNTMIN
}
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

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is (org_id in $GROUPS or region = $REGION) and amount > $AMOUNTMIN
}
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

source: X is duckdb.table('accounts') extend {
   join_one: child is duckdb.table('child') on child_id = child.id
   #(authorize)
   internal dimension: authorized is child.name in $GROUPS
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

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is org_id in $GROUPS
}

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

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is org_id in $GROUPS
}
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
    *  a request with `givens` and record request-time denial or admission.
    *  Returns the served rows when the request is admitted (load succeeded
    *  and no error was thrown), so a caller can assert the actual row set
    *  rather than only the failure-mode label — a regression that returned
    *  ALL rows instead of filtering would still be "admitted" but must not
    *  pass a case titled "and it actually filters". */
   async function observe(
      caseNum: number,
      spelling: string,
      text: string,
      givens: Record<string, GivenValue>,
   ): Promise<ReadonlyArray<Record<string, unknown>> | undefined> {
      const { model, duckdb, dir } = await createModel(text);
      try {
         const loadErr = compilationErrorOf(model);
         if (loadErr) {
            recordB(caseNum, spelling, "load-time", loadErr.message);
            return undefined;
         }
         try {
            const rows = await rowsFor(model, "X", givens);
            recordB(
               caseNum,
               spelling,
               "unexpectedly-admitted",
               `loaded and served ${rows.length} row(s) for givens ${JSON.stringify(givens)} — no refusal observed`,
            );
            return rows;
         } catch (err) {
            if (err instanceof AccessDeniedError) {
               recordB(caseNum, spelling, "request-time-denied", err.message);
            } else if (err instanceof Error) {
               recordB(
                  caseNum,
                  spelling,
                  "request-time-execution-error",
                  err.message,
               );
            } else {
               throw err;
            }
         }
      } finally {
         await cleanup(duckdb, dir);
      }
   }

   it("1. `not (org_id in $GROUPS)` — negated membership — no longer refused at load under the DIMENSION form, and it actually filters (guarantee changed — see task-3b-report.md)", async () => {
      // The STRING form's `classifyAuthorizeGate` refused this shape outright
      // at load. The DIMENSION form does not classify the expression's shape
      // at all (`./gate_dimension`'s doc: validation reads the compiled
      // `FieldDef`, not a parsed comparison/`inGiven` node) — G1 only checks
      // "scalar boolean dimension", which `not (a in b)` satisfies, so this
      // loads and correctly filters (W2-warned: negated membership is a
      // KNOWN GAP for the EMPTY-given case specifically — an empty array
      // then matches every row instead of none — see
      // `gate_dimension_integration.spec.ts` — but a non-empty given, as
      // exercised here, filters correctly).
      const rows = await observe(
         1,
         "not (org_id in $GROUPS)",
         `
given:
  GROUPS :: string[]

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is not (org_id in $GROUPS)
}
`,
         { GROUPS: ["org1"] },
      );
      const result = RESULTS[RESULTS.length - 1] as GroupBResult;
      expect(result.failureMode).toBe("unexpectedly-admitted");
      // Assert the actual row set, not just the failure-mode label — a
      // regression that returned ALL rows (a bare admit, not a filter)
      // would still be "unexpectedly-admitted" but must not pass a case
      // titled "and it actually filters". GROUPS=[org1] excludes org1 rows.
      expect(ids(rows ?? [])).toEqual([4, 5, 6]);
   });

   it("2. `org_id = $GROUPS` — array given, scalar operator — no longer refused at load under the DIMENSION form (guarantee changed — see task-3b-report.md)", async () => {
      // Same root cause as case 1: the DIMENSION form does not statically
      // check that a scalar comparison's given operand is scalar-typed (the
      // string form's classification refused this on the given's DECLARED
      // type). G1 only asks "is this a scalar boolean dimension" — Malloy
      // itself accepts `org_id = $GROUPS` at compile time, and only crashes
      // generating SQL for it at REQUEST time.
      await observe(
         2,
         "org_id = $GROUPS",
         `
given:
  GROUPS :: string[]

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is org_id = $GROUPS
}
`,
         { GROUPS: ["org1"] },
      );
      const result = RESULTS[RESULTS.length - 1] as GroupBResult;
      expect(result.failureMode).toBe("request-time-execution-error");
      // Not the old publisher-synthesized "Access denied for source ..."
      // message, and it does name the author's construct (the SQL DuckDB
      // rejects quotes `org_id`) — but this is DuckDB failing to generate
      // SQL at REQUEST time, not "Malloy's own compile error, on the
      // author's line" the task brief's table expected. See task-5-report.md.
      expect(result.message).not.toMatch(/^Access denied for source/);
      expect(result.message).toMatch(/org_id/);
   });

   it("3. `owner in $ROLE` — scalar given, `in` operator (task's `role in $ROLE` shape; `owner` stands in for the field since the fixture has no `role` column)", async () => {
      await observe(
         3,
         "owner in $ROLE",
         `
given:
  ROLE :: string

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is owner in $ROLE
}
`,
         { ROLE: "alice" },
      );
      const result = RESULTS[RESULTS.length - 1] as GroupBResult;
      expect(result.failureMode).toBe("load-time");
      // Malloy's own compile diagnostic, naming the given — not the old
      // publisher-synthesized "'owner' is not defined" from the deleted
      // string form's fallback single-row probe.
      expect(result.message).toMatch(/ROLE/);
      expect(result.message).not.toMatch(/is not defined/);
   });

   it("4. `upper(region) = $REGION` — function call — accepted and filters correctly (C2 fix; previously wrongly refused the WHOLE model load)", async () => {
      // Before the C2 fix, Malloy's synthetic empty-`path` `fieldUsage` entry
      // for the `upper(...)` call resolved to `undefined` in
      // `expandGivenIds`, which G3 then treated as an unresolvable
      // reference — aborting the entire model load, not just this case. This
      // case previously carried NO assertion at all (see task-3-fix-brief.md
      // I5), which is why that over-refusal slipped past a docs commit
      // claiming this exact spelling was verified legal.
      const rows = await observe(
         4,
         "upper(region) = $REGION",
         `
given:
  REGION :: string

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is upper(region) = $REGION
}
`,
         { REGION: "EAST" },
      );
      const result = RESULTS[RESULTS.length - 1] as GroupBResult;
      expect(result.failureMode).toBe("unexpectedly-admitted");
      expect(ids(rows ?? [])).toEqual([1, 2, 4]);
   });

   it("5. `1 = 1` — two constants, no given — accepted, admits all (W1-warned)", async () => {
      const rows = await observe(
         5,
         "1 = 1",
         `
given:
  UNUSED :: string

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is 1 = 1
}
`,
         {},
      );
      const result = RESULTS[RESULTS.length - 1] as GroupBResult;
      expect(result.failureMode).toBe("unexpectedly-admitted");
      expect(ids(rows ?? [])).toEqual([1, 2, 3, 4, 5, 6]);
   });

   it("6. `region like $PAT` — accepted and filters correctly", async () => {
      const rows = await observe(
         6,
         "region like $PAT",
         `
given:
  PAT :: string

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is region like $PAT
}
`,
         { PAT: "east" },
      );
      const result = RESULTS[RESULTS.length - 1] as GroupBResult;
      expect(result.failureMode).toBe("unexpectedly-admitted");
      expect(ids(rows ?? [])).toEqual([1, 2, 4]);
   });

   it("7. `region is not null` — accepted, admits all (no row has a null region)", async () => {
      const rows = await observe(
         7,
         "region is not null",
         `
given:
  UNUSED :: string

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is region is not null
}
`,
         {},
      );
      const result = RESULTS[RESULTS.length - 1] as GroupBResult;
      expect(result.failureMode).toBe("unexpectedly-admitted");
      expect(ids(rows ?? [])).toEqual([1, 2, 3, 4, 5, 6]);
   });

   it("8. `amount + 1 > $AMOUNTMIN` — arithmetic — accepted and filters correctly", async () => {
      const rows = await observe(
         8,
         "amount + 1 > $AMOUNTMIN",
         `
given:
  AMOUNTMIN :: number

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is amount + 1 > $AMOUNTMIN
}
`,
         { AMOUNTMIN: 100 },
      );
      const result = RESULTS[RESULTS.length - 1] as GroupBResult;
      expect(result.failureMode).toBe("unexpectedly-admitted");
      expect(ids(rows ?? [])).toEqual([1, 2, 4, 5, 6]);
   });

   it("9. `region = $REGION` where $REGION HAS a declared default", async () => {
      await observe(
         9,
         "region = $REGION (REGION has a default)",
         `
given:
  REGION :: string is 'east'

source: X is duckdb.table('accounts') extend {
  #(authorize)
  internal dimension: authorized is region = $REGION
}
`,
         { REGION: "east" },
      );
      const result = RESULTS[RESULTS.length - 1] as GroupBResult;
      expect(result.failureMode).toBe("load-time");
      // G4 specifically (a declared default), not some other load-time abort.
      expect(result.message).toMatch(/declared with a default/);
   });
});
