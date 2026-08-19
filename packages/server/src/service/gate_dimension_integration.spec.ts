/**
 * Verification suite for the DIMENSION form of `#(authorize)` — see
 * `./gate_dimension`'s doc for what this form is and why it is validated
 * separately from the string form. Every case compiles REAL Malloy against a
 * REAL DuckDB connection (never hand-typed IR — same convention
 * `row_level_authorize.integration.spec.ts`'s `buildGatedModel` documents),
 * through `Model.create` / `getQueryResults`, matching production.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type Connection,
   type GivenValue,
   type ModelDef,
} from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AccessDeniedError, ModelCompilationError } from "../errors";
import { validateGateDimension } from "./gate_dimension";
import { Model } from "./model";

const ROOT = "file:///gate-dimension-tests/";

/** `accounts`: two orgs, two rows each, so a `$GROUPS`-keyed gate has an
 *  observable effect and an empty array is distinguishable from "no gate". */
const SEED_SQL = `
CREATE OR REPLACE TABLE accounts (id INTEGER, org_id VARCHAR, amount INTEGER);
INSERT INTO accounts VALUES
   (1, 'org1', 100), (2, 'org1', 200), (3, 'org2', 300), (4, 'org2', 400);
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

   it("rewritten dimension by NAME survives a rename around it (graft-by-name, not by re-parsed code)", async () => {
      // If the graft re-parsed `code` instead of referencing the dimension
      // by name, this would be indistinguishable from the by-name case for
      // this particular model — the real proof that it's by name, not code,
      // is Constraint 9's design intent (never re-derive `filterText` from
      // `.code`); this test at least pins that the graft still works when
      // the gate dimension's OWN expression references another renamed
      // field, which a naive re-parse of `code` (not re-resolved field
      // paths) could get wrong.
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
         let threw: unknown;
         let rows: unknown;
         try {
            rows = await model.getQueryResults(
               undefined,
               undefined,
               "run: Z -> { select: org_id }",
               {},
               true,
               { GROUPS: ["org1"] },
            );
         } catch (e) {
            threw = e;
         }
         // Discovery recurses to X (the derivation base) where `authorized`
         // still exists; the graft on Z (the entry point) then can't resolve
         // the name in Z's own projected field space and fails closed. Pin
         // whichever actually happens: a 403 (the graft never lands) or a
         // 200 with zero rows (the graft lands but nothing satisfies it).
         if (threw !== undefined) {
            expect(threw).toBeInstanceOf(AccessDeniedError);
         } else {
            const compact = (rows as { compactResult: unknown[] })
               .compactResult;
            expect(compact.length).toBe(0);
         }
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});
