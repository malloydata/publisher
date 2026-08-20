/**
 * Verification suite for the DIMENSION form of `#(authorize)` — see
 * `./gate_dimension`'s doc for what this form is and why it is validated
 * separately from the string form. Every case compiles REAL Malloy against a
 * REAL DuckDB connection (never hand-typed IR — same convention
 * `row_level_authorize.integration.spec.ts`'s `buildGatedModel` documents),
 * through `Model.create` / `getQueryResults`, matching production — with ONE
 * deliberate exception, the G1 raw-boolean-column test below, which hand
 * -builds a `FieldDef` because no real Malloy syntax reaches that shape.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type Connection,
   type FieldDef,
   type GivenValue,
   type ModelDef,
   type SourceDef,
} from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AccessDeniedError, ModelCompilationError } from "../errors";
import { assertNoLegacyStringGate } from "./authorize";
import {
   findGateDimensionCandidates,
   gateFieldName,
   validateGateDimension,
} from "./gate_dimension";
import { Model } from "./model";

const ROOT = "file:///gate-dimension-tests/";

/** `accounts`: two orgs, two rows each, so a `$GROUPS`-keyed gate has an
 *  observable effect and an empty array is distinguishable from "no gate".
 *  `region` (added for the C2 function-call cases below) is pre-uppercased
 *  so `upper(region)`, `region` alone, and `upper($REGION)` all compare
 *  case-consistently across the different spellings those cases exercise. */
const SEED_SQL = `
CREATE OR REPLACE TABLE accounts (id INTEGER, org_id VARCHAR, amount INTEGER, region VARCHAR);
INSERT INTO accounts VALUES
   (1, 'org1', 100, 'EAST'), (2, 'org1', 200, 'WEST'),
   (3, 'org2', 300, 'EAST'), (4, 'org2', 400, 'WEST');
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
 *  DuckDB — exercises the full load-time validation path (both
 *  `validateAuthorizeProbes` and `validateGateDimensionsForModel`). */
async function createModel(
   text: string,
): Promise<{ model: Model; duckdb: DuckDBConnection; dir: string }> {
   const duckdb = await newDuckdb();
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gate-dim-"));
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

async function cleanup(duckdb: DuckDBConnection, dir: string): Promise<void> {
   await duckdb.close();
   fs.rmSync(dir, { recursive: true, force: true });
}

function compilationErrorOf(model: Model): Error | undefined {
   return (model as unknown as { compilationError?: Error }).compilationError;
}

async function ids(
   model: Model,
   sourceName: string,
   givens: Record<string, GivenValue>,
): Promise<number[]> {
   const result = await model.getQueryResults(
      undefined,
      undefined,
      `run: ${sourceName} -> { select: id; order_by: id }`,
      {},
      true,
      givens,
   );
   return (
      result.compactResult as unknown as ReadonlyArray<Record<string, unknown>>
   ).map((r) => Number(r.id));
}

/** Compile `text` (no execution) for direct `validateGateDimension` /
 *  `modelDef` inspection — the pure-logic rules (G1/G3/G4/private/
 *  multiplicity/W1/W2) don't need a live query, just the compiled IR. */
async function compileModelDef(
   text: string,
   duckdb: DuckDBConnection,
   modelPath = "m.malloy",
): Promise<{ modelDef: ModelDef; declaredGivenNames: ReadonlySet<string> }> {
   const fullText = text.includes("experimental.givens")
      ? text
      : `##! experimental.givens\n\n${text}`;
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}${modelPath}`, fullText]]),
   );
   const runtime = new Runtime({
      urlReader,
      connections: new FixedConnectionMap(
         new Map<string, Connection>([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   const mm = runtime.loadModel(new URL(`${ROOT}${modelPath}`), {
      importBaseURL: new URL(ROOT),
   });
   const compiled = await mm.getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const modelDef = (compiled as any)._modelDef as ModelDef;
   // The model's OWN given surface — see `validateGateDimension`'s doc for
   // why this (not `modelDef.givens`) is G3's reachability signal.
   const declaredGivenNames = new Set(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      Array.from((compiled as any).givens.values()).map(
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         (g: any) => g.name as string,
      ),
   );
   return { modelDef, declaredGivenNames };
}

function sourceOf(modelDef: ModelDef, name: string) {
   const obj = modelDef.contents[name];
   if (!obj) throw new Error(`no such source: ${name}`);
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   return obj as any;
}

/** Compile `text` and validate `accounts`'s gate dimension in one call —
 *  shared by every "pure rules" case below. */
async function validateAccounts(
   text: string,
   duckdb: DuckDBConnection,
   onWarning?: (cause: string, detail: string) => void,
) {
   const { modelDef, declaredGivenNames } = await compileModelDef(text, duckdb);
   return validateGateDimension(
      "accounts",
      sourceOf(modelDef, "accounts"),
      modelDef,
      declaredGivenNames,
      onWarning,
   );
}

describe("validateGateDimension — pure rules", () => {
   it("resolves a legal gate dimension: internal scalar boolean, given with no default", async () => {
      const duckdb = await newDuckdb();
      try {
         const resolved = await validateAccounts(
            `given:\n  GROUPS :: string[]\n\nsource: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $GROUPS\n}\n`,
            duckdb,
         );
         expect(resolved?.name).toBe("authorized");
      } finally {
         await duckdb.close();
      }
   });

   it("G1 — refuses a non-boolean annotated dimension", async () => {
      const duckdb = await newDuckdb();
      try {
         await expect(
            validateAccounts(
               `given:\n  GROUPS :: string[]\n\nsource: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id\n}\n`,
               duckdb,
            ),
         ).rejects.toThrow(/scalar boolean dimension/);
      } finally {
         await duckdb.close();
      }
   });

   it("G1 — refuses an annotated MEASURE (aggregate, not scalar)", async () => {
      const duckdb = await newDuckdb();
      try {
         await expect(
            validateAccounts(
               `given:\n  GROUPS :: string[]\n\nsource: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   measure: authorized is count()\n}\n`,
               duckdb,
            ),
         ).rejects.toThrow(/scalar boolean dimension/);
      } finally {
         await duckdb.close();
      }
   });

   it("G1 — refuses a raw boolean column (type boolean, but no expression) — hand-built, since real Malloy syntax has no way to attach #(authorize) to an auto-detected schema column with no dimension: declaration of its own", () => {
      // Every other case in this file compiles REAL Malloy (this file's own
      // header states that convention) because an annotation always lands on
      // a field the AUTHOR wrote a `dimension:`/`measure:`/`view:` line for —
      // there is no Malloy syntax to write `#(authorize)` directly on a raw,
      // schema-auto-detected column with no declaration of its own to hang
      // the annotation off. `asExpr.e === undefined` in `validateGateDimension`
      // is a defensive check for exactly that shape (a `type: "boolean"`
      // field with no compiled expression) should one ever reach it — pinned
      // here with a hand-built `FieldDef`, the one deliberate exception to
      // this file's real-Malloy convention.
      const field = {
         name: "authorized",
         type: "boolean",
         expressionType: "scalar",
         e: undefined,
         annotations: {
            blockNotes: [{ text: "#(authorize)\n" }],
         },
      } as unknown as FieldDef;
      const struct = {
         name: "accounts",
         fields: [field],
      } as unknown as SourceDef;
      const modelDef = { givens: {} } as unknown as ModelDef;
      expect(() =>
         validateGateDimension("accounts", struct, modelDef, new Set()),
      ).toThrow(/scalar boolean dimension/);
   });

   it("more than one annotated dimension on one source names both", async () => {
      const duckdb = await newDuckdb();
      try {
         await expect(
            validateAccounts(
               `given:\n  GROUPS :: string[]\n\nsource: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $GROUPS\n   #(authorize)\n   internal dimension: also_gate is org_id in $GROUPS\n}\n`,
               duckdb,
            ),
         ).rejects.toThrow(/authorized.*also_gate|also_gate.*authorized/);
      } finally {
         await duckdb.close();
      }
   });

   it("mixed forms (I3) — a source declaring BOTH the string form (on itself) and the dimension form (on its own field) is refused", async () => {
      const { model, duckdb, dir } = await createModel(
         `given:\n  GROUPS :: string[]\n\n#(authorize) "true"\nsource: entry is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $GROUPS\n}\n`,
      );
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/string form.*DIMENSION form/i);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("mixed forms (I3), REVERSE order — a source that INHERITS the dimension form from an ancestor and adds its OWN string form is also refused", async () => {
      // The "order" this reverses relative to the test above: there, both
      // forms are born together on the SAME source (string tag necessarily
      // precedes the field tag textually, since it sits above `source:`).
      // Here the dimension form is established FIRST, upstream, on X — Y
      // only inherits it unchanged (Malloy flattens X's still-annotated
      // field into Y's own `fields`, same as every other inheritance case
      // in this file) — and Y's OWN string form is added downstream. Refusal
      // must not depend on which form was declared first in the model.
      const { model, duckdb, dir } = await createModel(
         `given:\n  GROUPS :: string[]\n\nsource: X is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $GROUPS\n}\n#(authorize) "true"\nsource: Y is X extend {}\n`,
      );
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/string form.*DIMENSION form/i);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("private refusal — a private gate dimension is an active load-time error, not merely inert", async () => {
      const duckdb = await newDuckdb();
      try {
         await expect(
            validateAccounts(
               `given:\n  GROUPS :: string[]\n\nsource: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   private dimension: authorized is org_id in $GROUPS\n}\n`,
               duckdb,
            ),
         ).rejects.toThrow(/private/);
      } finally {
         await duckdb.close();
      }
   });

   it("G4 — refuses a given declared with a default", async () => {
      const duckdb = await newDuckdb();
      try {
         await expect(
            validateAccounts(
               `given:\n  ROLE :: string is 'user'\n\nsource: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id = $ROLE\n}\n`,
               duckdb,
            ),
         ).rejects.toThrow(/declared with a default/);
      } finally {
         await duckdb.close();
      }
   });

   it("G4 transitive pin — a one-hop wrapper over a DEFAULTED given is still refused (givenUsage is not transitive; G4 must expand it)", async () => {
      // `authorized` carries the annotation and no `givenUsage` of its own
      // (a bare `is` re-export) — only `base_authorized`, one hop away,
      // actually references `$ROLE`. A G4 that reads only `authorized`'s own
      // `refSummary.givenUsage` would see nothing and wrongly pass.
      const duckdb = await newDuckdb();
      try {
         await expect(
            validateAccounts(
               `given:\n  ROLE :: string is 'user'\n\nsource: accounts is duckdb.table('accounts') extend {\n   dimension: base_authorized is org_id = $ROLE\n   #(authorize)\n   internal dimension: authorized is base_authorized\n}\n`,
               duckdb,
            ),
         ).rejects.toThrow(/declared with a default/);
      } finally {
         await duckdb.close();
      }
   });

   it("W1 — no given referenced warns and still resolves (package loads)", async () => {
      const duckdb = await newDuckdb();
      try {
         const warnings: [string, string][] = [];
         const resolved = await validateAccounts(
            `source: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id = 'org1'\n}\n`,
            duckdb,
            (cause, detail) => warnings.push([cause, detail]),
         );
         expect(resolved?.name).toBe("authorized");
         expect(warnings.map((w) => w[0])).toContain(
            "gate_dimension_no_given_reference",
         );
      } finally {
         await duckdb.close();
      }
      // The pure `validateGateDimension` call above proves the RULE; this
      // proves the claim in the title — that a real package actually LOADS
      // (via `Model.create`, the same path `Model.create`/the package-load
      // worker use) rather than merely that one direct function call didn't
      // throw.
      const {
         model,
         duckdb: duckdb2,
         dir,
      } = await createModel(
         `source: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id = 'org1'\n}\n`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
      } finally {
         await cleanup(duckdb2, dir);
      }
   });

   it("W2 — a negated membership test warns and still resolves (package loads)", async () => {
      const duckdb = await newDuckdb();
      try {
         const warnings: [string, string][] = [];
         const resolved = await validateAccounts(
            `given:\n  GROUPS :: string[]\n\nsource: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is not (org_id in $GROUPS)\n}\n`,
            duckdb,
            (cause, detail) => warnings.push([cause, detail]),
         );
         expect(resolved?.name).toBe("authorized");
         expect(warnings.map((w) => w[0])).toContain(
            "gate_dimension_negated_membership",
         );
      } finally {
         await duckdb.close();
      }
      // Same distinction as W1 above: prove a real package actually loads.
      const {
         model,
         duckdb: duckdb2,
         dir,
      } = await createModel(
         `given:\n  GROUPS :: string[]\n\nsource: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is not (org_id in $GROUPS)\n}\n`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
      } finally {
         await cleanup(duckdb2, dir);
      }
   });

   it("G3 — a given 2 import hops away (beyond this model's own surface) fails the LOAD, not just the request", async () => {
      // Mirrors `row_level_authorize.integration.spec.ts`'s identical 2-hop
      // shape for the string form's `unreachable_given` — but the dimension
      // form's G3 is a LOAD-time check (`modelDef.givens` doesn't surface
      // `FAR`), so this aborts `Model.create` outright rather than loading
      // and denying only at request time.
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gate-dim-farhop-"));
      try {
         fs.writeFileSync(
            path.join(dir, "deep.malloy"),
            `##! experimental.givens\n\ngiven:\n  FAR :: string[]\n\nsource: Deep is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $FAR\n}\n`,
         );
         fs.writeFileSync(
            path.join(dir, "mid.malloy"),
            `import "deep.malloy"\nsource: Mid is Deep extend {}\n`,
         );
         fs.writeFileSync(
            path.join(dir, "entry.malloy"),
            `import "mid.malloy"\nsource: Entry is Mid extend {}\n`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "entry.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(
            /references a given this model cannot resolve/,
         );
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

describe("C2 — a function call in the gate expression must not abort the whole model load", () => {
   // Malloy emits a synthetic `refSummary.fieldUsage` entry with an EMPTY
   // `path` for a function call. Before the fix, `expandGivenIds` resolved
   // that empty path to `undefined` and returned `{ok: false}`, which G3
   // then treated as an unresolvable reference — refusing the ENTIRE model
   // load for a gate that is actually perfectly legal. See
   // task-3-fix-brief.md C2 for the five spellings pinned here.

   it("`upper(region) = $REGION` — function wraps the field — loads and filters", async () => {
      const { model, duckdb, dir } = await createModel(
         `given:\n  REGION :: string\n\nsource: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is upper(region) = $REGION\n}\n`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         expect(await ids(model, "accounts", { REGION: "EAST" })).toEqual([
            1, 3,
         ]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("`region = upper($REGION)` — function wraps the given — loads and filters", async () => {
      const { model, duckdb, dir } = await createModel(
         `given:\n  REGION :: string\n\nsource: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is region = upper($REGION)\n}\n`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         expect(await ids(model, "accounts", { REGION: "east" })).toEqual([
            1, 3,
         ]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("`concat(region,'') = $REGION` — a different function, same empty-path shape — loads and filters", async () => {
      const { model, duckdb, dir } = await createModel(
         `given:\n  REGION :: string\n\nsource: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is concat(region,'') = $REGION\n}\n`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         expect(await ids(model, "accounts", { REGION: "EAST" })).toEqual([
            1, 3,
         ]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("`upper(region) = 'EAST'` — no given at all — loads with a W1 warning, not a refusal", async () => {
      const duckdb = await newDuckdb();
      try {
         const warnings: [string, string][] = [];
         const resolved = await validateAccounts(
            `source: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is upper(region) = 'EAST'\n}\n`,
            duckdb,
            (cause, detail) => warnings.push([cause, detail]),
         );
         expect(resolved?.name).toBe("authorized");
         expect(warnings.map((w) => w[0])).toContain(
            "gate_dimension_no_given_reference",
         );
      } finally {
         await duckdb.close();
      }
      // Prove the real package-load path (not just the pure rule) accepts
      // it too.
      const {
         model,
         duckdb: duckdb2,
         dir,
      } = await createModel(
         `source: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is upper(region) = 'EAST'\n}\n`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         expect(await ids(model, "accounts", {})).toEqual([1, 3]);
      } finally {
         await cleanup(duckdb2, dir);
      }
   });

   it("`org_id in $G or upper(region) = 'EAST'` — a function call ORed with a real given reference — loads and filters on both", async () => {
      const { model, duckdb, dir } = await createModel(
         `given:\n  G :: string[]\n\nsource: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $G or upper(region) = 'EAST'\n}\n`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         expect(await ids(model, "accounts", { G: ["org2"] })).toEqual([
            1, 3, 4,
         ]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

describe("C1 — the legacy-string-gate refusal message must itself load-and-gate (round trip)", () => {
   it("the exact remediation text emitted by assertNoLegacyStringGate compiles into a working gate — regression guard for the one-line message bug", async () => {
      let message: string | undefined;
      try {
         assertNoLegacyStringGate([
            { sourceName: "accounts", exprs: ["org_id in $GROUPS"] },
         ]);
      } catch (err) {
         message = (err as Error).message;
      }
      if (!message)
         throw new Error("expected assertNoLegacyStringGate to throw");

      // Extract the remediation block VERBATIM from the thrown message —
      // this is what makes the test a regression guard: it feeds back
      // whatever text authorize.ts actually emits, not a hand-typed guess
      // of it. A one-line remediation (the bug this test exists to catch)
      // would make this regex fail to match at all, since the dimension
      // declaration would be on the SAME line as `#(authorize)`.
      const match = message.match(/- source "accounts":\n(( {6}.+\n?)+)/);
      if (!match) {
         throw new Error(
            `could not extract a two-line remediation block from: ${message}`,
         );
      }
      const remediation = match[1];
      expect(remediation).toMatch(
         /^ {6}#\(authorize\)\n {6}internal dimension:/,
      );

      const { model, duckdb, dir } = await createModel(
         `given:\n  GROUPS :: string[]\n\nsource: accounts is duckdb.table('accounts') extend {\n${remediation}}\n`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         // A gate candidate exists...
         const candidates = findGateDimensionCandidates(
            (model as unknown as { modelDef: ModelDef }).modelDef.contents[
               "accounts"
            ] as unknown as SourceDef,
         );
         expect(candidates.map((f) => gateFieldName(f))).toEqual([
            "authorized",
         ]);
         // ...and it actually filters.
         expect(await ids(model, "accounts", { GROUPS: ["org1"] })).toEqual([
            1, 2,
         ]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

describe("join-qualified given references (C1 regression — see task-2-report.md)", () => {
   it("a join-qualified reference (`h.ok`) to a DEFAULTED given is followed, not silently dropped: G4 refuses", async () => {
      const duckdb = await newDuckdb();
      try {
         const text =
            `given:\n  ROLE :: string[] is ['org1','org2']\n\n` +
            `source: helper is duckdb.table('accounts') extend {\n   dimension: ok is org_id in $ROLE\n   primary_key: id\n}\n` +
            `source: entry is duckdb.table('accounts') extend {\n   join_one: h is helper on id = h.id\n   #(authorize)\n   internal dimension: authorized is h.ok\n}\n`;
         const { modelDef, declaredGivenNames } = await compileModelDef(
            text,
            duckdb,
         );
         expect(() =>
            validateGateDimension(
               "entry",
               sourceOf(modelDef, "entry"),
               modelDef,
               declaredGivenNames,
            ),
         ).toThrow(/declared with a default/);
      } finally {
         await duckdb.close();
      }
   });

   it("the same join-qualified gate, given UNDEFAULTED: filters per caller through the join, and denies opaquely when unbound", async () => {
      const { model, duckdb, dir } = await createModel(
         `given:\n  ROLE :: string[]\n\n` +
            `source: helper is duckdb.table('accounts') extend {\n   dimension: ok is org_id in $ROLE\n   primary_key: id\n}\n` +
            `source: entry is duckdb.table('accounts') extend {\n   join_one: h is helper on id = h.id\n   #(authorize)\n   internal dimension: authorized is h.ok\n}\n`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         expect(await ids(model, "entry", { ROLE: ["org1"] })).toEqual([1, 2]);
         await expect(ids(model, "entry", {})).rejects.toBeInstanceOf(
            AccessDeniedError,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a nested join chain (`b.c.flag`) resolves through BOTH joins, not just one hop", async () => {
      const { model, duckdb, dir } = await createModel(
         `given:\n  ROLE :: string[]\n\n` +
            `source: innerSrc is duckdb.table('accounts') extend {\n   dimension: flag is org_id in $ROLE\n   primary_key: id\n}\n` +
            `source: mid is duckdb.table('accounts') extend {\n   join_one: c is innerSrc on id = c.id\n   primary_key: id\n}\n` +
            `source: entry is duckdb.table('accounts') extend {\n   join_one: b is mid on id = b.id\n   #(authorize)\n   internal dimension: authorized is b.c.flag\n}\n`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         expect(await ids(model, "entry", { ROLE: ["org1"] })).toEqual([1, 2]);
         // The filtering assertion above alone does NOT distinguish correct
         // 2-hop expansion from the bug: the graft is by NAME regardless of
         // whether `expandGivenIds` tracked the given correctly, so a bound
         // query filters correctly either way. What the bug actually breaks
         // is the DECLARED given-tracking used for defense-in-depth — an
         // unbound `$ROLE` must deny opaquely (`AccessDeniedError`), not
         // leak Malloy's raw "has no value and no default" error naming the
         // given. Confirmed failing-first: under the pre-fix code, this
         // request throws a raw `MalloyError` instead.
         await expect(ids(model, "entry", {})).rejects.toBeInstanceOf(
            AccessDeniedError,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a LOCAL field sharing the join-qualified reference's LEAF name must not substitute its givens (the reverse-direction bug)", async () => {
      // `entry` declares its OWN "ok" dimension (keyed on a DIFFERENT,
      // DEFAULTED given) purely to create a leaf-name collision with the
      // join-qualified reference "h.ok" (keyed on an UNDEFAULTED given). If
      // resolution matched by leaf name alone rather than following the
      // join segment first, it would wrongly resolve to entry's OWN "ok"
      // and see the DEFAULTED given — G4 would incorrectly refuse this
      // legal model. Fixed, resolution follows the "h" segment first and
      // correctly reaches helper's "ok" (the undefaulted ROLE), so this
      // loads clean.
      const duckdb = await newDuckdb();
      try {
         const text =
            `given:\n  ROLE :: string[]\n  OTHER :: string[] is ['x']\n\n` +
            `source: helper is duckdb.table('accounts') extend {\n   dimension: ok is org_id in $ROLE\n   primary_key: id\n}\n` +
            `source: entry is duckdb.table('accounts') extend {\n   join_one: h is helper on id = h.id\n   dimension: ok is org_id in $OTHER\n   #(authorize)\n   internal dimension: authorized is h.ok\n}\n`;
         const { modelDef, declaredGivenNames } = await compileModelDef(
            text,
            duckdb,
         );
         expect(() =>
            validateGateDimension(
               "entry",
               sourceOf(modelDef, "entry"),
               modelDef,
               declaredGivenNames,
            ),
         ).not.toThrow();
      } finally {
         await duckdb.close();
      }
   });
});

describe("dimension-form gate — end to end", () => {
   it("grafts by name and filters rows; empty array + `in` denies zero rows; a missing given is a 400", async () => {
      const { model, duckdb, dir } = await createModel(
         `given:\n  GROUPS :: string[]\n\nsource: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $GROUPS\n}\n`,
      );
      try {
         expect(await ids(model, "accounts", { GROUPS: ["org1"] })).toEqual([
            1, 2,
         ]);
         expect(await ids(model, "accounts", { GROUPS: ["org2"] })).toEqual([
            3, 4,
         ]);
         // Constraint 2, re-pinned against the dimension form: empty array +
         // `in` is fail-CLOSED (zero rows), not an accidental admit-all.
         expect(await ids(model, "accounts", { GROUPS: [] })).toEqual([]);
         // Constraint 2, re-pinned: a missing given fails closed BEFORE
         // execution. Empirically (see task-2-report.md) this is an opaque
         // 403 (`AccessDeniedError`), matching the STRING form exactly — the
         // gate given unbound at bind time is denied rather than leaking
         // Malloy's raw "has no value and no default" compile error, which
         // would name the gate's given to the caller.
         await expect(ids(model, "accounts", {})).rejects.toBeInstanceOf(
            AccessDeniedError,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("what `internal` actually does for the gate dimension: blocks an ordinary query's OWN reference, but not the graft's extend-based where:", async () => {
      // `validateGateDimension` refuses `private` because it hides the field
      // from the graft too, which would load fine but fail every query at
      // request time. `internal` is required instead — but empirically
      // `internal` is NOT simply "visible everywhere but across an import
      // boundary", which a plausible but wrong reading of this task's report
      // draft once assumed. It is narrower and more precise than that: a
      // caller's OWN reference to the field — a direct `select:`, or even a
      // `where:` written INSIDE the query's own pipeline stage
      // (`-> {where: authorized}`) — is refused with `'authorized' is
      // internal`, exactly like `private` would be. What `internal`
      // specifically permits is the ONE shape the graft itself compiles:
      // a `where:` attached via `extend {}` BEFORE the pipeline
      // (`` `accounts` extend { where: (authorized) } -> {...} ``,
      // `buildRowLevelProbe`'s shape). `internal` is not a general
      // visibility relaxation of the gate dimension's value; it is exactly
      // permeable to the graft mechanism and nothing more permissive than
      // that within this model.
      const { model, duckdb, dir } = await createModel(
         `given:\n  GROUPS :: string[]\n\nsource: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $GROUPS\n}\n`,
      );
      const givens = { GROUPS: ["org1"] };
      try {
         // A direct SELECT of the gate dimension is refused, same as
         // `private` would be.
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: accounts -> { select: id, authorized }",
               {},
               true,
               givens,
            ),
         ).rejects.toThrow(/'authorized' is internal/);
         // A `where:` INSIDE the query's own pipeline stage is refused the
         // same way — `internal` does not carve out `where:` in general,
         // only the graft's specific extend-based shape (proven below).
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: accounts -> { where: authorized; select: id }",
               {},
               true,
               givens,
            ),
         ).rejects.toThrow(/'authorized' is internal/);
         // The graft's OWN shape — an extend-based where: attached BEFORE
         // the pipeline — succeeds and filters correctly. This is the exact
         // mechanism `buildRowLevelProbe`/the request-time graft compile.
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: accounts extend { where: (authorized) } -> { select: id; order_by: id }",
            {},
            true,
            givens,
         );
         const ids = (
            result.compactResult as unknown as ReadonlyArray<
               Record<string, unknown>
            >
         ).map((r) => Number(r.id));
         expect(ids).toEqual([1, 2]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("the gate dimension's OWN expression can reference a renamed field and still graft correctly", async () => {
      // NOT a proof that the graft is by-name-not-by-code — the graft target
      // itself (`quoteMalloyIdentifier(fieldName)`) is exercised identically
      // by every other test in this file; that property is Constraint 9's
      // design intent (never re-derive `filterText` from `.code`) and is not
      // independently distinguishable from the outside for this or any other
      // single model. What THIS test actually pins: the gate dimension's own
      // expression can reference a field the author renamed (`org` for
      // `org_id`), and the graft still resolves and filters correctly —
      // a case a naive re-parse of `.code` (unresolved field paths) could get
      // wrong, even though this test does not itself prove the graft avoids
      // that path.
      const { model, duckdb, dir } = await createModel(
         `given:\n  GROUPS :: string[]\n\nsource: accounts is duckdb.table('accounts') extend {\n   rename: org is org_id\n   #(authorize)\n   internal dimension: authorized is org in $GROUPS\n}\n`,
      );
      try {
         expect(await ids(model, "accounts", { GROUPS: ["org1"] })).toEqual([
            1, 2,
         ]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("getAuthorize() surfaces the gate dimension's own `code`, same shape the string form returned", async () => {
      // Task 3a-bis: `getAuthorize` used to return `[]` for every
      // dimension-form gate (only the retired string form populated it).
      // This is introspection ONLY — `code` here is the author's expression
      // text, never re-parsed or re-derived from for enforcement.
      const { model, duckdb, dir } = await createModel(
         `given:\n  GROUPS :: string[]\n\nsource: accounts is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $GROUPS\n}\n`,
      );
      try {
         expect(model.getAuthorize("accounts")).toEqual(["org_id in $GROUPS"]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a field literally named `authorized` that carries no annotation does not collide with a gate dimension declared under a DIFFERENT name", async () => {
      // The gate attaches to the ANNOTATED field, never by the name
      // "authorized" — that name is only a convention. `authorized` here is
      // an ordinary, unannotated dimension that would admit every row if it
      // (wrongly) drove the graft; `gate_ok` is the real, annotated gate.
      const { model, duckdb, dir } = await createModel(
         `given:\n  GROUPS :: string[]\n\nsource: accounts is duckdb.table('accounts') extend {\n   dimension: authorized is true\n   #(authorize)\n   internal dimension: gate_ok is org_id in $GROUPS\n}\n`,
      );
      try {
         expect(await ids(model, "accounts", { GROUPS: ["org1"] })).toEqual([
            1, 2,
         ]);
         expect(model.getAuthorize("accounts")).toEqual(["org_id in $GROUPS"]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

describe("inheritance matrix", () => {
   async function loadOk(text: string): Promise<Model> {
      const { model, duckdb, dir } = await createModel(text);
      await cleanup(duckdb, dir);
      return model;
   }

   it("plain extend inherits the gate unchanged (loads cleanly, enforces)", async () => {
      const { model, duckdb, dir } = await createModel(
         `given:\n  GROUPS :: string[]\n\nsource: X is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $GROUPS\n}\nsource: Y is X extend {}\n`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         expect(await ids(model, "Y", { GROUPS: ["org1"] })).toEqual([1, 2]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("own-only: a source's own annotated dimension needs no base at all", async () => {
      const model = await loadOk(
         `given:\n  GROUPS :: string[]\n\nsource: X is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $GROUPS\n}\n`,
      );
      expect(compilationErrorOf(model)).toBeUndefined();
   });

   it("both: an inherited gate plus a NEW own gate on the extension fails the load (two annotated dimensions)", async () => {
      const { model, duckdb, dir } = await createModel(
         `given:\n  GROUPS :: string[]\n\nsource: X is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $GROUPS\n}\nsource: Y is X extend {\n   #(authorize)\n   internal dimension: other_gate is org_id = 'org1'\n}\n`,
      );
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(
            /authorized.*other_gate|other_gate.*authorized/,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("KNOWN GAP: except: + unannotated redefinition is NOT detected — Malloy leaves no IR link to the base for this shape", async () => {
      // Intent: a same-named gate dimension redefined in an extend WITHOUT
      // re-annotating should fail the load (task-2-brief.md's inheritance
      // rule). A bare `dimension: authorized is …` redefinition can't even
      // reach that rule — Malloy refuses it outright at compile time
      // ("Cannot redefine", the same mechanism PIN 1 below pins for a
      // caller) — so the only way to redeclare the name at all is
      // `except: authorized` first, which REMOVES the field before the new
      // one is added.
      //
      // Empirically (see task-2-report.md), `except:` leaves NO discoverable
      // link from `Y` back to `X`: `Y.referenceID`/`Y.sourceID` don't name
      // `X`, `resolveDeclaredSource(Y, modelDef)` returns `{kind: "none"}`
      // (confirmed the same for a no-op `rename:` extend, so this isn't
      // specific to `except:`), and neither `Y`'s own struct annotations nor
      // the new field's carry anything — `undefined` on both, not merely
      // empty. `validateGateDimension`'s redefinition check
      // (`gate_dimension.ts`) is built on exactly that link and therefore
      // CANNOT fire for this shape: there is nothing in the compiled IR
      // connecting the two structs to compare identity against. This is a
      // genuine, reported gap (see the report's Concerns), not a weakened
      // test — the model loads CLEANLY and Y's own gate silently reads a
      // fixed predicate instead of X's `$GROUPS` gate.
      const { model, duckdb, dir } = await createModel(
         `given:\n  GROUPS :: string[]\n\nsource: X is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $GROUPS\n}\nsource: Y is X extend {\n   except: authorized\n   internal dimension: authorized is org_id = 'org1'\n}\n`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         // Y's redefinition of `authorized` carries no `#(authorize)` of its
         // own, so `findGateDimensionCandidates(Y)` finds nothing at all —
         // Y is completely UNGATED. Every row comes back regardless of
         // `GROUPS`, which is the silent-shedding hazard this rule exists
         // to close and, per the gap above, cannot close for this shape.
         expect(await ids(model, "Y", { GROUPS: ["org2"] })).toEqual([
            1, 2, 3, 4,
         ]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("KNOWN GAP — the canonical fail-open shape: `extend { except: authorized }` alone, with nothing re-added, drops the gate entirely and admits every row", async () => {
      // The fail-open/fail-closed rule (task-3-fix-brief.md): DROPPING the
      // annotated dimension fails OPEN, because `findGateDimensionCandidates`
      // simply finds nothing on Y to gate on — this is the same root cause
      // as the "KNOWN GAP" test above (no IR link from Y back to X for
      // `validateGateDimension`'s redefinition check to use), but pinned
      // here in its most minimal, most dangerous form: no redeclaration at
      // all, just a bare `except:`. This is a genuine, documented gap in
      // this repo, NOT correct behavior — Malloy keeps no IR link from a
      // derived source back to its base, so there is nothing to detect the
      // drop against.
      const { model, duckdb, dir } = await createModel(
         `given:\n  GROUPS :: string[]\n\nsource: X is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $GROUPS\n}\nsource: Y is X extend {\n   except: authorized\n}\n`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         expect(await ids(model, "Y", { GROUPS: ["org1"] })).toEqual([
            1, 2, 3, 4,
         ]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("renaming the gate dimension via `extend { rename: ... }` is a legal, correctly-enforced shape — the annotation survives the rename", async () => {
      // Contrast with the `except:` cases above: a rename does not drop the
      // field, it relabels it, so the annotation (attached to the field
      // object, not the name) travels with it and the gate keeps filtering
      // exactly as it did on X.
      const { model, duckdb, dir } = await createModel(
         `given:\n  GROUPS :: string[]\n\nsource: X is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $GROUPS\n}\nsource: Y is X extend {\n   rename: gate2 is authorized\n}\n`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         expect(await ids(model, "Y", { GROUPS: ["org1"] })).toEqual([1, 2]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a re-annotated redefinition is a legal override — own gate replaces inherited", async () => {
      const { model, duckdb, dir } = await createModel(
         `given:\n  GROUPS :: string[]\n\nsource: X is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $GROUPS\n}\nsource: Y is X extend {\n   except: authorized\n   #(authorize)\n   internal dimension: authorized is org_id = 'org1'\n}\n`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         // Y's OWN override — a fixed predicate, not keyed on GROUPS — wins.
         expect(await ids(model, "Y", { GROUPS: ["org2"] })).toEqual([1, 2]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

describe("pins — load-bearing inferences", () => {
   it("PIN 1 — caller-supplied redefinition does not shadow the gate; Malloy refuses it outright at compile time", async () => {
      const { model, duckdb, dir } = await createModel(
         `given:\n  GROUPS :: string[]\n\nsource: X is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $GROUPS\n}\n`,
      );
      try {
         // Failing-first evidence (see task-2-report.md for the captured
         // failure): the naive expectation is that the caller's redefinition
         // either shadows the gate (returns ALL rows) or loses a race to the
         // model's own definition (returns the gate-filtered rows) — i.e.
         // that the query SUCCEEDS one way or the other. It does neither:
         // Malloy refuses the redefinition at compile time, so the query
         // never runs. Assert the PROPERTIES (a rejection; the caller's
         // `true` never took effect via a successful all-rows read), not
         // Malloy's exact wording, so a compiler bump can't break this.
         let threw: unknown;
         try {
            await model.getQueryResults(
               undefined,
               undefined,
               "run: X extend { dimension: authorized is true } -> { select: id }",
               {},
               true,
               { GROUPS: ["org1"] },
            );
         } catch (e) {
            threw = e;
         }
         expect(threw).toBeDefined();
         expect(threw).not.toBeInstanceOf(AccessDeniedError);
         // Never an all-rows read — the caller's `true` redefinition would
         // have produced ids [1,2,3,4] had it taken effect. It's simplest to
         // assert the call didn't return at all (a compile error), which the
         // `threw` check above already does; this restates the properties
         // pin explicitly for the reader.
         expect(String((threw as Error).message).toLowerCase()).toContain(
            "redefine",
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("PIN 2 — a -> derivation that projects the gate dimension away denies", async () => {
      const { model, duckdb, dir } = await createModel(
         `given:\n  GROUPS :: string[]\n\nsource: X is duckdb.table('accounts') extend {\n   #(authorize)\n   internal dimension: authorized is org_id in $GROUPS\n}\nsource: Z is X -> { select: org_id, amount }\n`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         // Discovery recurses to X (the derivation base) where `authorized`
         // still exists, so Z is still recognized as gated; the graft then
         // attempts to compile `Z extend { where: (\`authorized\`) }`, and
         // fails — `authorized` is not in Z's own projected field space
         // (`select: org_id, amount` dropped it) — with "'authorized' is not
         // defined" / "Filter expression must have boolean value" (see
         // task-2-report.md for the captured message). That lift failure
         // denies closed via the SAME opaque-403 path a missing given uses,
         // never a 200 with zero rows: the query never runs at all.
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: Z -> { select: org_id }",
               {},
               true,
               { GROUPS: ["org1"] },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});
