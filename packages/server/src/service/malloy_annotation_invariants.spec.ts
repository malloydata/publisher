/**
 * Pins the undocumented `@malloydata/malloy` IR behaviors the row-level
 * `#(authorize)` enforcement design depends on.
 *
 * The whole design is reverse-engineered from Malloy's annotation-copy
 * behavior, and nothing currently fails when that behavior changes. The
 * discriminators throughout `model.ts`, `authorize.ts`, `source_extraction.ts`
 * and `gate_registry_walk.ts` — inherited-vs-authored,
 * `findSourceByOwnAnnotationIdentity`, `joinFieldNamesUnresolvableDeclaration`,
 * the composite merge in `effectiveAncestorGateExprs` — all depend on WHICH
 * struct Malloy copies WHICH note object onto, BY REFERENCE, in the pinned
 * version below. Those modules' comments repeatedly assert "confirmed
 * against 0.0.427", but in prose, not in an assertion that goes red on a
 * version bump. A real P0 leak on this branch (a composite parent's gate and
 * its member's gate being OR'd into one list — see
 * `row_level_authorize.integration.spec.ts`'s "composite gate grouping"
 * describe block) existed precisely because one of these behaviors was not
 * known. The failure direction is NOT uniformly fail-closed, so a silent
 * change here can open access rather than close it.
 *
 * This file's job is to make the next Malloy bump a RED TEST rather than a
 * silent authorization change. It asserts Malloy's own compiler behavior
 * directly — never the publisher's handling of it — and uses reference
 * identity (`toBe`, `Set`/array `includes`) wherever the claim is about
 * object identity, not deep equality: deep equality would keep passing even
 * if Malloy switched to copying notes by value, which is exactly the
 * regression this file exists to catch.
 *
 * Every invariant below was verified empirically against the installed
 * `@malloydata/malloy` before being written down (compile a small model,
 * inspect the real IR, THEN write the assertion) — see the version assertion
 * at the top of the describe block for how to find out what changed if this
 * file goes red.
 */
import {
   Annotations,
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type Connection,
   type ModelDef,
   type SourceDef,
   type StructDef,
} from "@malloydata/malloy";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { describe, expect, it } from "bun:test";
import * as fs from "fs";

const ROOT = "file:///malloy-annotation-invariants-tests/";

/** One raw annotation note, as carried on `annotations.blockNotes`/`.notes` —
 *  see `annotations.ts`'s `AnnotationNote` for why object identity (not
 *  text) is the only sound way to detect Malloy's by-reference copy. */
interface RawNote {
   text: string;
}
interface RawAnnotations {
   blockNotes?: RawNote[];
   notes?: RawNote[];
   inherits?: RawAnnotations;
}

/** The annotation notes declared directly on ONE node — not its `inherits`
 *  ancestors. Mirrors `annotations.ts`'s `ownLevelNotes`. */
function ownLevelNotes(annotations: RawAnnotations | undefined): RawNote[] {
   return [...(annotations?.blockNotes ?? []), ...(annotations?.notes ?? [])];
}

/** Compile `text` to a real `ModelDef` — same idiom as
 *  `row_level_authorize.integration.spec.ts`'s `createModel`, trimmed to just
 *  the compile step since these tests inspect IR shape and never run a
 *  query. Every model here uses `duckdb.sql(...)` literals, so an in-memory
 *  connection with no seeded tables is enough. */
async function compileModel(text: string): Promise<ModelDef> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   try {
      const urlReader = new InMemoryURLReader(
         new Map([[`${ROOT}m.malloy`, text]]),
      );
      const connMap = new Map<string, Connection>([["duckdb", duckdb]]);
      const runtime = new Runtime({
         urlReader,
         connections: new FixedConnectionMap(connMap, "duckdb"),
      });
      const mm = runtime.loadModel(new URL(`${ROOT}m.malloy`));
      const compiled = await mm.getModel();
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return (compiled as any)._modelDef as ModelDef;
   } finally {
      await duckdb.close();
   }
}

/** The compiled `Model`'s givens, keyed by name. `compileModel` above returns
 *  the `ModelDef`, which carries no given declarations — `Model.givens` is the
 *  only surface that does, and it is what `package_load_worker` reads. */
async function compileGivens(
   text: string,
): Promise<Map<string, { _internal?: { defaultText?: string } }>> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   try {
      const urlReader = new InMemoryURLReader(
         new Map([[`${ROOT}m.malloy`, text]]),
      );
      const connMap = new Map<string, Connection>([["duckdb", duckdb]]);
      const runtime = new Runtime({
         urlReader,
         connections: new FixedConnectionMap(connMap, "duckdb"),
      });
      const compiled = await runtime
         .loadModel(new URL(`${ROOT}m.malloy`))
         .getModel();
      return compiled.givens as unknown as Map<
         string,
         { _internal?: { defaultText?: string } }
      >;
   } finally {
      await duckdb.close();
   }
}

describe("Malloy IR annotation invariants (pins @malloydata/malloy behavior)", () => {
   it("resolves the installed @malloydata/malloy version this file was verified against", () => {
      // If every test below starts failing after a version bump, this is the
      // first thing to report: which version's behavior changed underneath
      // these invariants. Read from the installed package, not hardcoded, so
      // it always names the CURRENT version — including one that already
      // diverged from what these tests assert.
      const malloyPkg = JSON.parse(
         fs.readFileSync(
            require.resolve("@malloydata/malloy/package.json"),
            "utf-8",
         ),
      ) as { version: string };
      // Confirmed against 0.0.427 — see this file's header and each test's
      // comment for what was verified. Not itself an assertion that the
      // version IS 0.0.427; the point is only to name it in a failure report.
      // eslint-disable-next-line no-console
      console.log(
         `malloy_annotation_invariants.spec.ts verified against @malloydata/malloy ${malloyPkg.version}`,
      );
      expect(typeof malloyPkg.version).toBe("string");
   });

   // -------------------------------------------------------------------
   // Invariant 1 (P0): a composite-resolved member struct's blockNotes
   // contains BOTH the composite parent's own note object and the member's
   // own — by reference. This is the exact mechanism behind the P0 leak
   // fixed in `effectiveAncestorGateExprs` (`gate_registry_walk.ts`): reading
   // the resolved member's own notes without first identity-subtracting the
   // composite parent's (`parentOwnNotes`/`compositeOwnNotes` there) folds
   // TWO different sources' conditions into one OR'd list, silently turning
   // this file's own AND-across-sources rule into an OR. If Malloy stops
   // copying the composite parent's note onto the resolved member (or starts
   // copying a re-parsed COPY instead of the same object), the identity
   // subtraction in `effectiveAncestorGateExprs` stops matching anything,
   // `compositeOwnNotes` silently includes the parent's note again, and the
   // P0 leak reopens with no other signal.
   // -------------------------------------------------------------------
   it("composite: a resolved member's own notes include BOTH the composite parent's own note and the member's own, by reference", async () => {
      const modelDef = await compileModel(`##! experimental.composite_sources
##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "org_id in \\$GROUPS"
source: member_a is duckdb.sql("SELECT 7 as org_id") extend {}

source: member_b is duckdb.sql("SELECT 99 as org_id") extend {}

#(authorize) "region = 'us'"
source: combo is compose(member_a, member_b)

source: qs is combo -> { group_by: org_id }
`);
      const memberA = modelDef.contents["member_a"] as StructDef;
      const combo = modelDef.contents["combo"] as StructDef;
      const qs = modelDef.contents["qs"] as unknown as {
         type: string;
         query?: { compositeResolvedSourceDef?: StructDef };
      };
      expect(qs.type).toBe("query_source");
      const resolvedMember = qs.query?.compositeResolvedSourceDef;
      expect(resolvedMember).toBeDefined();

      const memberAOwnNotes = ownLevelNotes(
         memberA.annotations as RawAnnotations | undefined,
      );
      const comboOwnNotes = ownLevelNotes(
         combo.annotations as RawAnnotations | undefined,
      );
      const resolvedOwnNotes = ownLevelNotes(
         resolvedMember!.annotations as RawAnnotations | undefined,
      );
      expect(memberAOwnNotes.length).toBe(1);
      expect(comboOwnNotes.length).toBe(1);

      // THE canary: both ancestors' note OBJECTS, by reference, land on the
      // resolved member's own notes.
      expect(resolvedOwnNotes).toContain(memberAOwnNotes[0]);
      expect(resolvedOwnNotes).toContain(comboOwnNotes[0]);
   });

   // Proves the canary above can actually fail: if Malloy ever stopped
   // sharing the composite parent's note object with the resolved member
   // (copied by value instead), `toContain` (`Array.includes`, reference
   // equality) would no longer find it. Simulated here by asserting the
   // negation to show the assertion is not vacuously true.
   it("composite: reference-identity assertion is not vacuous — an independently-authored note with the SAME text is NOT found by reference", async () => {
      const modelDef = await compileModel(`##! experimental.composite_sources
##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) "org_id in \\$GROUPS"
source: member_a is duckdb.sql("SELECT 7 as org_id") extend {}

source: member_b is duckdb.sql("SELECT 99 as org_id") extend {}

#(authorize) "region = 'us'"
source: combo is compose(member_a, member_b)

source: qs is combo -> { group_by: org_id }
`);
      const qs = modelDef.contents["qs"] as unknown as {
         query?: { compositeResolvedSourceDef?: StructDef };
      };
      const resolvedOwnNotes = ownLevelNotes(
         qs.query?.compositeResolvedSourceDef?.annotations as
            | RawAnnotations
            | undefined,
      );
      // A freshly-constructed note object with byte-identical text is NOT
      // the same object Malloy copied — proving `toContain` discriminates on
      // reference, not text, i.e. that flipping invariant 1's assertion
      // direction really does fail (see this test's sibling for the actual
      // flip-and-fail evidence pasted in the report).
      const impostor = { text: resolvedOwnNotes[0]?.text };
      expect(resolvedOwnNotes).not.toContain(impostor);
   });

   // -------------------------------------------------------------------
   // Invariant 2: a trivial `extend {}` derivation shares the base's note
   // object by reference (no annotation of its own). This is what makes
   // `ownLevelNoteTexts` on the DERIVING struct itself already find the
   // base's gate for this shape — `gate_registry_walk.ts`'s module doc
   // explains why `ancestorGateExprs`/`resolveDeclaredSource` are never
   // reached for it in practice. If Malloy stops doing this (e.g. starts
   // leaving the derived struct's `annotations` empty, or moves the base's
   // note to `.inherits` even for a trivial `extend {}`), every "does this
   // entry point declare its own gate" check keyed off note-object identity
   // (`findSourceByOwnAnnotationIdentity`, `validateAuthorizeProbes`'s
   // own-vs-inherited discriminator) silently starts treating an inherited
   // gate as absent, which fails OPEN at that entry point.
   // -------------------------------------------------------------------
   it("extend {}: a trivial derivation shares the base's own note object by reference, at the TOP level (no .inherits demotion)", async () => {
      const modelDef = await compileModel(`
#(authorize) "org_id > 0"
source: base is duckdb.sql("SELECT 7 as org_id") extend {}

source: derived is base extend {}
`);
      const base = modelDef.contents["base"] as StructDef;
      const derived = modelDef.contents["derived"] as SourceDef;
      const baseOwnNotes = ownLevelNotes(
         base.annotations as RawAnnotations | undefined,
      );
      const derivedOwnNotes = ownLevelNotes(
         derived.annotations as RawAnnotations | undefined,
      );
      expect(baseOwnNotes.length).toBe(1);
      expect(derivedOwnNotes.length).toBe(1);
      // THE canary: same object, not a re-parsed equal one.
      expect(derivedOwnNotes[0]).toBe(baseOwnNotes[0]);
      // Also: Malloy elides the derivation link entirely for this shape —
      // `derived`'s sourceRegistry entry self-references rather than
      // pointing at `base` — which is exactly why the note-identity fallback
      // in `findSourceByOwnAnnotationIdentity` has to exist at all.
      expect(derived.referenceID).toBeUndefined();
   });

   // Proves invariant 2's canary can fail: flip `toBe` to `not.toBe` and it
   // must go red, since the two structs otherwise share nothing structurally
   // distinguishing them from independently-declared sources with equal
   // text. See the report for the pasted failing run.
   it("extend {}: two independently-declared sources with identical gate text are DISTINCT note objects (not shared)", async () => {
      const modelDef = await compileModel(`
#(authorize) "org_id > 0"
source: indepA is duckdb.sql("SELECT 7 as org_id") extend {}

#(authorize) "org_id > 0"
source: indepB is duckdb.sql("SELECT 8 as org_id") extend {}
`);
      const indepA = modelDef.contents["indepA"] as StructDef;
      const indepB = modelDef.contents["indepB"] as StructDef;
      const aNotes = ownLevelNotes(
         indepA.annotations as RawAnnotations | undefined,
      );
      const bNotes = ownLevelNotes(
         indepB.annotations as RawAnnotations | undefined,
      );
      expect(aNotes[0].text).toBe(bNotes[0].text);
      expect(aNotes[0]).not.toBe(bNotes[0]);
   });

   // -------------------------------------------------------------------
   // Invariant 3: an `extend` that declares its OWN `#(authorize)` does NOT
   // share the base's note object — it is a distinct object — and the
   // base's own note is demoted to `annotations.inherits` rather than
   // dropped. This is the "own wins over ancestor" rule `ancestorGateExprs`
   // (`gate_registry_walk.ts`) depends on: an extension's authored gate
   // REPLACES the base's for enforcement, but the base's gate must still be
   // reachable via `.inherits` for anything that walks ancestry (there is
   // none here, since own wins outright, but the same `.inherits` slot is
   // what an annotated `join_one:`/`join_many:` line does NOT get — see
   // that module's doc for the three-way split). If Malloy ever stopped
   // demoting the base's note to `.inherits` when the deriving statement
   // adds its own, `ancestorGateExprs`'s inherits-chain walk would silently
   // stop finding anything for this shape.
   // -------------------------------------------------------------------
   it("extend with its OWN #(authorize): the authored note is a distinct object, and the base's own note is demoted to .inherits", async () => {
      const modelDef = await compileModel(`
#(authorize) "org_id > 0"
source: base3 is duckdb.sql("SELECT 7 as org_id") extend {}

#(authorize) "org_id > 5"
source: derived3 is base3 extend {}
`);
      const base3 = modelDef.contents["base3"] as StructDef;
      const derived3 = modelDef.contents["derived3"] as StructDef;
      const base3OwnNotes = ownLevelNotes(
         base3.annotations as RawAnnotations | undefined,
      );
      const derived3Annotations = derived3.annotations as
         | RawAnnotations
         | undefined;
      const derived3OwnNotes = ownLevelNotes(derived3Annotations);

      expect(base3OwnNotes.length).toBe(1);
      expect(derived3OwnNotes.length).toBe(1);
      // THE canary: derived3's own note is NOT the same object as base3's.
      expect(derived3OwnNotes[0]).not.toBe(base3OwnNotes[0]);
      // base3's own note is still reachable, but only one level down, on
      // .inherits — not at derived3's own level, and not dropped.
      expect(derived3Annotations?.inherits).toBeDefined();
      const inheritedNotes = ownLevelNotes(derived3Annotations?.inherits);
      expect(inheritedNotes).toContain(base3OwnNotes[0]);
   });

   // -------------------------------------------------------------------
   // Invariant 4: a `query_source` (`Z is X -> {...}`) struct has no
   // `annotations` of its own — confirmed `undefined`, not an empty object.
   // `gate_registry_walk.ts`'s module doc calls this out explicitly: it is
   // why the `annotations.inherits` chain never runs for a query_source, and
   // why `Model.collectEntryPointGates`/`effectiveAncestorGateExprs` instead
   // follow `query.structRef` (`resolveQuerySourceBase`) as the ONLY
   // surviving link back to the base. If Malloy ever started attaching
   // annotations to a query_source struct, code reading
   // `ownLevelNoteTexts(struct.annotations)` for it would start seeing
   // whatever landed there — silently changing which gate wins between the
   // query_source's own (nonexistent, currently) and its base's.
   // -------------------------------------------------------------------
   it("query_source: the struct itself carries no annotations at all", async () => {
      const modelDef = await compileModel(`
#(authorize) "org_id > 0"
source: base4 is duckdb.sql("SELECT 7 as org_id") extend {}

source: z4 is base4 -> { group_by: org_id }
`);
      const z4 = modelDef.contents["z4"] as unknown as {
         type: string;
         annotations?: unknown;
      };
      expect(z4.type).toBe("query_source");
      expect(z4.annotations).toBeUndefined();
   });

   // -------------------------------------------------------------------
   // Invariant 5: a JOINED source's own gate note is copied onto the
   // joining struct's join FIELD by reference — same by-reference mechanism
   // as invariant 2's `extend {}`, just landing on a field instead of a
   // source. This is exactly what makes authored-vs-inherited undecidable by
   // TEXT alone on the join path: `source_extraction.ts`'s
   // `gatedSourceOwnAuthorizeNotes` set (built from every source's own
   // notes, model-wide) is what tells "this join field's note is Malloy's
   // copy of some source's own gate" from "an author wrote `#(authorize)`
   // directly on this join_one: line" — the two look byte-identical by text.
   // If Malloy stopped copying the joined source's note onto the join field
   // (or started copying a re-parsed equal one), `gatedSourceOwnAuthorize
   // Notes` would stop matching it, and EVERY unannotated join of a gated
   // source would start being misclassified as an author-written misplaced
   // annotation — failing the load for every such package
   // (`assertNoMisplacedAuthorizeAnnotations`'s refusal).
   //
   // Note: the join field's own `name` is Malloy's synthetic
   // `sql://duckdb/<uuid>` identifier for this inline `duckdb.sql(...)`
   // source; `as` carries the join alias (`salaries`) actually used in the
   // model text — confirmed by inspecting the compiled field list directly.
   // -------------------------------------------------------------------
   it("join_one: an unannotated join copies the joined source's own gate note onto the join field, by reference", async () => {
      const modelDef = await compileModel(`
#(authorize) "org_id > 0"
source: salaries is duckdb.sql("SELECT 7 as org_id, 1 as id") extend {}

source: emp is duckdb.sql("SELECT 1 as id") extend {
  join_one: salaries on id = salaries.id
}
`);
      const salaries = modelDef.contents["salaries"] as StructDef;
      const emp = modelDef.contents["emp"] as StructDef;
      const salariesOwnNotes = ownLevelNotes(
         salaries.annotations as RawAnnotations | undefined,
      );
      expect(salariesOwnNotes.length).toBe(1);

      const joinField = emp.fields.find(
         (f) => (f as unknown as { as?: string }).as === "salaries",
      ) as unknown as { annotations?: RawAnnotations } | undefined;
      expect(joinField).toBeDefined();
      const joinFieldOwnNotes = ownLevelNotes(joinField?.annotations);
      expect(joinFieldOwnNotes.length).toBe(1);

      // THE canary: the join field's note is the SAME object as `salaries`'
      // own note, not a re-parsed equal one.
      expect(joinFieldOwnNotes[0]).toBe(salariesOwnNotes[0]);
   });

   // -------------------------------------------------------------------
   // Invariants 5a-5c: the same by-reference note-copy mechanism invariants
   // 1-5 pin for the STRING form's source-level annotation, re-verified for
   // the DIMENSION form's field-level one — `findGateDimensionCandidates`
   // (`gate_dimension.ts`) discovers a gate by walking `struct.fields` for an
   // annotated `internal dimension:`, so what matters here is whether the
   // FIELD carrying `#(authorize)` (not the struct) survives a rename or an
   // unchanged `extend {}` by reference, and whether the near-miss spelling
   // from invariant 6 below also fails to route when it sits on a field
   // rather than a source. `gate_dimension_integration.spec.ts`'s "renaming
   // the gate dimension via extend { rename: ... }" and "KNOWN GAP" tests
   // already pin the END-TO-END enforcement behavior of these shapes; these
   // three assert the raw IR mechanism underneath, same as invariants 1-5.
   // -------------------------------------------------------------------
   it("rename: on a gate-annotated field carries the field's own annotation object onto the renamed field, by reference", async () => {
      const modelDef = await compileModel(`
##! experimental.givens

given:
  GROUPS :: number[]

source: base5a is duckdb.sql("SELECT 7 as org_id") extend {
   #(authorize)
   internal dimension: authorized is org_id in $GROUPS
}

source: renamed5a is base5a extend {
   rename: gate5a is authorized
}
`);
      const base = modelDef.contents["base5a"] as StructDef;
      const renamed = modelDef.contents["renamed5a"] as StructDef;
      const baseGateField = base.fields.find(
         (f) => (f as unknown as { name?: string }).name === "authorized",
      ) as unknown as { annotations?: RawAnnotations } | undefined;
      const renamedGateField = renamed.fields.find(
         (f) => (f as unknown as { as?: string }).as === "gate5a",
      ) as unknown as { annotations?: RawAnnotations } | undefined;
      expect(baseGateField).toBeDefined();
      expect(renamedGateField).toBeDefined();
      const baseNotes = ownLevelNotes(baseGateField?.annotations);
      const renamedNotes = ownLevelNotes(renamedGateField?.annotations);
      expect(baseNotes.length).toBe(1);
      expect(renamedNotes.length).toBe(1);
      // THE canary: rename carries the SAME annotation object, not a
      // re-parsed equal one.
      expect(renamedNotes[0]).toBe(baseNotes[0]);
   });

   it("extend {} (unchanged): the gate-annotated field's own annotation is the SAME object on the deriving struct's copy of the field, by reference", async () => {
      const modelDef = await compileModel(`
##! experimental.givens

given:
  GROUPS :: number[]

source: base5b is duckdb.sql("SELECT 7 as org_id") extend {
   #(authorize)
   internal dimension: authorized is org_id in $GROUPS
}

source: derived5b is base5b extend {}
`);
      const base = modelDef.contents["base5b"] as StructDef;
      const derived = modelDef.contents["derived5b"] as StructDef;
      const baseGateField = base.fields.find(
         (f) => (f as unknown as { name?: string }).name === "authorized",
      ) as unknown as { annotations?: RawAnnotations } | undefined;
      const derivedGateField = derived.fields.find(
         (f) => (f as unknown as { name?: string }).name === "authorized",
      ) as unknown as { annotations?: RawAnnotations } | undefined;
      expect(baseGateField).toBeDefined();
      expect(derivedGateField).toBeDefined();
      // Unlike invariant 2's STRUCT-level flattening, Malloy rebuilds a new
      // field object per struct here — `derivedGateField` is not `===
      // baseGateField` — so the canary is at the ANNOTATION level, same as
      // every other invariant in this file: the note object itself is
      // carried by reference onto the rebuilt field, which is exactly why
      // `findGateDimensionCandidates` finds an inherited gate dimension for
      // this shape at all (by walking `struct.fields` for the annotation,
      // never by identity-comparing the field object itself).
      const baseNotes = ownLevelNotes(baseGateField?.annotations);
      const derivedNotes = ownLevelNotes(derivedGateField?.annotations);
      expect(baseNotes.length).toBe(1);
      expect(derivedNotes.length).toBe(1);
      expect(derivedNotes[0]).toBe(baseNotes[0]);
   });

   it("`#(authorized)` (extra `d`) on a FIELD's annotation does not route to `authorize` either — the near miss is not source-position-specific", async () => {
      const modelDef = await compileModel(`
source: base5c is duckdb.sql("SELECT 7 as org_id") extend {
   #(authorized)
   internal dimension: notagate is org_id
}
`);
      const base = modelDef.contents["base5c"] as StructDef;
      const field = base.fields.find(
         (f) => (f as unknown as { name?: string }).name === "notagate",
      ) as unknown as { annotations?: RawAnnotations } | undefined;
      expect(field).toBeDefined();
      const notes = ownLevelNotes(field?.annotations);
      expect(notes.length).toBe(1);
      const at = {
         url: `${ROOT}m.malloy`,
         range: {
            start: { line: 0, character: 0 },
            end: { line: 0, character: 0 },
         },
      };
      expect(
         new Annotations({ notes: [{ text: notes[0].text, at }] }).forRoute(
            "authorize",
         ).length,
      ).toBe(0);
   });

   // -------------------------------------------------------------------
   // Invariant 6: which annotation SPELLINGS Malloy routes to `authorize`.
   //
   // This is the input to every authorize code path — `parseAuthorizeAnnotation`,
   // `containsAuthorizeAnnotationTag`, `materialization_eligibility.ts`'s
   // `isAuthorizeAnnotation` all classify by asking Malloy — so a change in the
   // prefix grammar silently changes what is a gate. Both directions are live
   // hazards. A spelling Malloy STOPS routing here (say `#[authorize]`) turns a
   // locked source into one that serves every row, load-clean. A spelling Malloy
   // STARTS routing here (say `# (authorize)`) makes publisher begin enforcing a
   // filter on packages that served every row yesterday — and would collide with
   // the near-miss refusal (`authorize.ts`'s `collectAuthorizeNearMisses`), which
   // currently refuses exactly the notes this test asserts are NOT gates.
   //
   // Asserted against Malloy's own `Annotations.forRoute` rather than publisher's
   // wrapper, per this file's rule: the claim is about the compiler.
   // -------------------------------------------------------------------
   it("annotation routing: exactly these spellings reach the `authorize` route", () => {
      const at = {
         url: `${ROOT}m.malloy`,
         range: {
            start: { line: 0, character: 0 },
            end: { line: 0, character: 0 },
         },
      };
      const routesToAuthorize = (text: string): boolean =>
         new Annotations({ notes: [{ text, at }] }).forRoute("authorize")
            .length > 0;

      // Sigil `#` or `##`, an optional block `|`, then `authorize` in any of
      // Malloy's four bracket pairs.
      for (const text of [
         '#(authorize) "x=1"',
         '##(authorize) "x=1"',
         '#|(authorize) "x=1"',
         '##|(authorize) "x=1"',
         '#[authorize] "x=1"',
         '#<authorize> "x=1"',
         '#{authorize} "x=1"',
      ]) {
         expect(routesToAuthorize(text)).toBe(true);
      }

      // The near misses. A space after the sigil is route `''` — Malloy's own
      // reserved MOTLY/render namespace, the same route as `# bar_chart`. The
      // rest are `malformed-route`, which `forRoute` excludes by contract.
      // `#(authorized)` is the boundary case: a legitimately different app
      // route, which is why the near-miss detector requires `authorize` to end
      // at a non-word character.
      for (const text of [
         '# (authorize) "x=1"',
         '## (authorize) "x=1"',
         '#( authorize ) "x=1"',
         '#(authorize ) "x=1"',
         '#(authorize)X "x=1"',
         '#authorize "x=1"',
         '#(AUTHORIZE) "x=1"',
         '#(authorized) "x=1"',
         '#(doc) "x=1"',
         "# bar_chart",
         '##(description) "see the #(authorize) tag"',
      ]) {
         expect(routesToAuthorize(text)).toBe(false);
      }
   });

   it("given: `_internal.defaultText` is the rendered default literal, and is absent when no default is declared", async () => {
      // This one does not pin a discriminator — it pins an INPUT to a security
      // refusal. `service/given.ts` reads `_internal.defaultText` (Malloy's
      // private surface, as its own comment says) to populate
      // `ApiGiven.default`; `model.ts` filters on `g.default != null` to build
      // `declaredDefaults`; and `authorize.ts`'s `declaredDefaults.has(given)`
      // is the refusal that stops a defaulted given from carrying a row-level
      // gate.
      //
      // Every one of those degrades SILENTLY and FAIL-OPEN if the field moves:
      // each `default` becomes undefined, the map empties, `has()` is always
      // false, and the gates that refusal exists to reject start loading again.
      // There is no layer that can tell "no givens have defaults" from "the
      // input went missing", which is exactly why the canary belongs here
      // rather than in a test of the publisher's own handling.
      const givens = await compileGivens(`##! experimental.givens

given: ROLE :: string is 'analyst'
given: MAX_ROWS :: number is 2003
given: FLAG :: boolean is true
given: SINCE :: date is @2024-01-01
given: NO_DEFAULT :: string

source: s is duckdb.sql("SELECT 1 as x") extend { measure: c is count() }
`);

      // The rendered SOURCE literal, not the parsed AST node and not the
      // runtime value — quotes on the string, bare number, bare boolean, and
      // Malloy's own `@`-prefixed date spelling. `given.ts` forwards this
      // verbatim rather than re-implementing the printer, so the exact
      // spelling is the contract.
      expect(givens.get("ROLE")?._internal?.defaultText).toBe("'analyst'");
      expect(givens.get("MAX_ROWS")?._internal?.defaultText).toBe("2003");
      expect(givens.get("FLAG")?._internal?.defaultText).toBe("true");
      expect(givens.get("SINCE")?._internal?.defaultText).toBe("@2024-01-01");

      // The other half, and the one that makes the assertion non-vacuous: a
      // given with no default must be distinguishable from one whose default
      // Malloy stopped rendering. If a version bump made BOTH undefined, the
      // four assertions above go red rather than this quietly agreeing.
      expect(givens.get("NO_DEFAULT")).toBeDefined();
      expect(givens.get("NO_DEFAULT")?._internal?.defaultText).toBeUndefined();
   });
});
