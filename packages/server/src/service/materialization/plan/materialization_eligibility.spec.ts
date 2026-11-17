// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Real-compiler contract for the materialization-eligibility gate. The gate
// reads specific compiled-IR shapes (a Parameter's `value: null` for unbound
// params; `refSummary.givenUsage` / `given` IR nodes for given references), so
// it is pinned against the real compiler rather than hand-built stubs — a
// Malloy change to either shape must fail here, not leak an ineligible source
// (a frozen tenant-filtered table) into the tier.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { PersistSource } from "@malloydata/malloy";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it, spyOn } from "bun:test";
import * as gatePolicy from "../domain/gate_policy";
import { MaterializationEligibilityError } from "../../../errors";
import { deriveAnnotationFields, plannedSources } from "./build_plan";
import {
   assertDatabasePersistNotAuthorizeGated,
   assertMaterializationEligible,
   assertTierEligible,
   gateEnforcedAtRead,
   READ_ONLY_DIALECTS,
} from "./materialization_eligibility";
import { collectSourceEligibility } from "./eligibility_collect";
import { installSettings } from "../../../test_helpers/settings_env";
import type { PersistSourceGateOutcome } from "../domain/table_plans";
import { fakeSource } from "../../../test_helpers/materialization_test_fixtures";
import {
   admitGivenPositions,
   readTimeDynamicTerms,
} from "./persist_dynamic_terms";

const ROOT = "file:///elig/";
let connections: FixedConnectionMap;

beforeAll(() => {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
});

/** Compile a single-file model and return its persist sources by name. */
async function persistSources(
   model: string,
): Promise<Record<string, PersistSource>> {
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}m.malloy`, model]]),
   );
   const runtime = new Runtime({ urlReader, connections });
   const compiled = await runtime
      .loadModel(new URL(`${ROOT}m.malloy`), { importBaseURL: new URL(ROOT) })
      .getModel();
   const byName: Record<string, PersistSource> = {};
   for (const source of plannedSources(
      await runtime.getBuildTargets(compiled),
   )) {
      byName[source.name] = source;
   }
   return byName;
}

describe("assertMaterializationEligible", () => {
   it("accepts a plain persist source (no params, no givens)", async () => {
      const sources = await persistSources(`##! experimental.persistence
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#@ persist name="mz_plain"
source: mz_plain is base -> { aggregate: c is count() }`);
      expect(sources.mz_plain).toBeDefined();
      expect(() =>
         assertMaterializationEligible(
            sources.mz_plain,
            deriveAnnotationFields(sources.mz_plain),
         ),
      ).not.toThrow();
   });

   it("accepts a parameter bound to a constant", async () => {
      const sources = await persistSources(`##! experimental.persistence
##! experimental.parameters
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#@ persist name="mz_bound"
source: mz_bound(threshold::number is 5) is base -> { aggregate: c is count() }`);
      expect(sources.mz_bound).toBeDefined();
      expect(() =>
         assertMaterializationEligible(
            sources.mz_bound,
            deriveAnnotationFields(sources.mz_bound),
         ),
      ).not.toThrow();
   });

   it("refuses a source with an unbound (free) parameter", async () => {
      const sources = await persistSources(`##! experimental.persistence
##! experimental.parameters
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#@ persist name="mz_free"
source: mz_free(threshold::number) is base -> { aggregate: c is count() }`);
      expect(sources.mz_free).toBeDefined();
      expect(() =>
         assertMaterializationEligible(
            sources.mz_free,
            deriveAnnotationFields(sources.mz_free),
         ),
      ).toThrow(MaterializationEligibilityError);
      expect(() =>
         assertMaterializationEligible(
            sources.mz_free,
            deriveAnnotationFields(sources.mz_free),
         ),
      ).toThrow(/unbound parameter/i);
   });

   it("refuses a given the BUILD would substitute, which is the one the artifact freezes", async () => {
      // Inside the persisted query the only value available at build time is
      // the declaration default, so it is baked into the relation and no
      // read-time term can undo it.
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: tenant :: string is 'acme'
source: base is duckdb.sql("SELECT 1 AS amount, 'acme' AS tenant")
#@ persist name="mz_given"
source: mz_given is base -> { where: tenant = $tenant; aggregate: c is count() }`);
      expect(sources.mz_given).toBeDefined();
      expect(() =>
         assertMaterializationEligible(
            sources.mz_given,
            deriveAnnotationFields(sources.mz_given),
         ),
      ).toThrow(MaterializationEligibilityError);
      expect(() =>
         assertMaterializationEligible(
            sources.mz_given,
            deriveAnnotationFields(sources.mz_given),
         ),
      ).toThrow(/given/i);
   });

   it("admits the same given in the extend block, and reports it as a stripped term", async () => {
      // The half that makes the refusal above acceptable: the safe placement is
      // one move away from the refused one, and it is the documented form.
      // Malloy leaves an extend-block `where:` out of the build, so the
      // artifact holds every caller's rows and the term is the reader's to
      // apply — which is what the returned plan commits the serve shape to.
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: tenant :: string is 'acme'
source: base is duckdb.sql("SELECT 1 AS amount, 'acme' AS tenant")
#@ persist name="mz_given_outside" storage=lake
source: mz_given_outside is base -> { select: * } extend { where: tenant = $tenant }`);
      expect(sources.mz_given_outside).toBeDefined();
      assertMaterializationEligible(
         sources.mz_given_outside,
         deriveAnnotationFields(sources.mz_given_outside),
      );
      const admitted = admitGivenPositions(sources.mz_given_outside);
      expect(admitted.ok && admitted.terms).toEqual([
         { code: "tenant = $tenant", givens: ["tenant"], columns: ["tenant"] },
      ]);
   });

   it("refuses a source protected by its own #(authorize) gate", async () => {
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#(authorize) $role = 'analyst'
#@ persist name="mz_authz"
source: mz_authz is base -> { aggregate: c is count() }`);
      expect(sources.mz_authz).toBeDefined();
      expect(() =>
         assertMaterializationEligible(
            sources.mz_authz,
            deriveAnnotationFields(sources.mz_authz),
         ),
      ).toThrow(MaterializationEligibilityError);
      expect(() =>
         assertMaterializationEligible(
            sources.mz_authz,
            deriveAnnotationFields(sources.mz_authz),
         ),
      ).toThrow(/authorize/i);
   });

   it("refuses a source protected by a no-given, fixed-predicate #(authorize) gate", async () => {
      // Deliberately references NO given (`org_id = 999`, a fixed predicate —
      // `source_line_gate_no_given_reference` at load time, still a valid
      // gate) rather than a `given:`-keyed comparison:
      // `assertMaterializationEligible` classifies givens BEFORE
      // `referencesAuthorize`, so a given-keyed gate here could refuse on the
      // given check first and never reach the authorize path this test
      // exists to exercise.
      const sources = await persistSources(`##! experimental.persistence
#(access_filter) org_id = 999
source: base is duckdb.sql("SELECT 1 AS org_id") extend {}
#@ persist name="mz_dim_authz"
source: mz_dim_authz is base -> { aggregate: c is count() }`);
      expect(sources.mz_dim_authz).toBeDefined();
      expect(() =>
         assertMaterializationEligible(
            sources.mz_dim_authz,
            deriveAnnotationFields(sources.mz_dim_authz),
         ),
      ).toThrow(MaterializationEligibilityError);
      expect(() =>
         assertMaterializationEligible(
            sources.mz_dim_authz,
            deriveAnnotationFields(sources.mz_dim_authz),
         ),
      ).toThrow(/authorize/i);
   });

   it("refuses a source gated by the `true` admit-all sentinel, same as any other gate", async () => {
      // Pins current behavior deliberately: `hasAnyAuthorizeNote` /
      // `isAuthorizeAnnotation` see any gate, `true` included, so a
      // `true`-gated source stays out of the storage companion. Treating it
      // as ungated is a later, separate decision, not a side effect.
      const sources = await persistSources(`##! experimental.persistence
#(authorize) true
source: base is duckdb.sql("SELECT 1 AS org_id") extend {}
#@ persist name="mz_admit_all"
source: mz_admit_all is base -> { aggregate: c is count() }`);
      expect(sources.mz_admit_all).toBeDefined();
      expect(() =>
         assertMaterializationEligible(
            sources.mz_admit_all,
            deriveAnnotationFields(sources.mz_admit_all),
         ),
      ).toThrow(MaterializationEligibilityError);
      expect(() =>
         assertMaterializationEligible(
            sources.mz_admit_all,
            deriveAnnotationFields(sources.mz_admit_all),
         ),
      ).toThrow(/authorize/i);
   });

   it("refuses a source protected ONLY by its own #(authorize) gate", async () => {
      // A source carrying no #(authorize) at all, only #(authorize) —
      // this must draw the same materialization refusal as an ordinary
      // row-level gate, otherwise a authorize-only source freezes
      // into a materialized artifact served to everyone. `isAuthorizeAnnotation`
      // (via `parseAuthorizeAnnotation`) is widened to recognize both routes,
      // so this is automatic rather than a special case.
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#(authorize) 'finance' = $role
#@ persist name="mz_source_authz"
source: mz_source_authz is base -> { aggregate: c is count() }`);
      expect(sources.mz_source_authz).toBeDefined();
      expect(() =>
         assertMaterializationEligible(
            sources.mz_source_authz,
            deriveAnnotationFields(sources.mz_source_authz),
         ),
      ).toThrow(MaterializationEligibilityError);
      expect(() =>
         assertMaterializationEligible(
            sources.mz_source_authz,
            deriveAnnotationFields(sources.mz_source_authz),
         ),
      ).toThrow(/authorize/i);
   });

   it("refuses a source that reaches an #(authorize) gate through a JOIN", async () => {
      // The gate is on the joined source, not on mz_authz_joined itself — a join
      // must not launder an authorize-gated source into a frozen table.
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string
#(authorize) $role = 'analyst'
source: gated is duckdb.sql("SELECT 1 AS amount, 'acme' AS tenant")
source: joiner is duckdb.sql("SELECT 2 AS n, 'acme' AS tenant")
#@ persist name="mz_authz_joined"
source: mz_authz_joined is joiner extend {
  join_one: g is gated on tenant = g.tenant
} -> { aggregate: c is count() }`);
      expect(sources.mz_authz_joined).toBeDefined();
      expect(() =>
         assertMaterializationEligible(
            sources.mz_authz_joined,
            deriveAnnotationFields(sources.mz_authz_joined),
         ),
      ).toThrow(MaterializationEligibilityError);
   });

   it("refuses a source gated by the BLOCK annotation form #|(authorize)", async () => {
      // The classification is Malloy's route, not a prefix regex — and a regex
      // could not see this form at all (`@malloydata/malloy`'s own type docs say
      // so). While it was one, a `#|(authorize)`-gated source was BOTH ungated at
      // query time and materialization-eligible: freezable into an artifact and
      // served to everyone, with a clean load either way.
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#|(authorize)
$role = 'analyst'
|#
#@ persist name="mz_block_authz"
source: mz_block_authz is base -> { aggregate: c is count() }`);
      expect(sources.mz_block_authz).toBeDefined();
      expect(() =>
         assertMaterializationEligible(
            sources.mz_block_authz,
            deriveAnnotationFields(sources.mz_block_authz),
         ),
      ).toThrow(MaterializationEligibilityError);
      expect(() =>
         assertMaterializationEligible(
            sources.mz_block_authz,
            deriveAnnotationFields(sources.mz_block_authz),
         ),
      ).toThrow(/authorize/i);
   });

   it("is ELIGIBLE for a gate reached through an ANNOTATED join — the documented gap", async () => {
      // The join-reach limit this file's header measures in prose, pinned as a
      // test. An annotated `join_*` REPLACES the joined struct's annotations
      // outright, leaving no authorize byte and no `inherits` in the subtree —
      // only a `sourceID` into `ModelDef.sourceRegistry`, which this pass has no
      // modelDef to resolve. Same model as the plain-join test above, one render
      // tag apart, so the delta is unambiguous.
      //
      // Asserted as eligible rather than left untested because the header's claim
      // that nothing is exposed by it rests on the serve path not gating joins
      // either. If either half moves, one of these two tests has to change, and
      // that is the point.
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string
#(authorize) $role = 'analyst'
source: gated is duckdb.sql("SELECT 1 AS amount, 'acme' AS tenant")
source: joiner is duckdb.sql("SELECT 2 AS n, 'acme' AS tenant")
#@ persist name="mz_authz_annotated_join"
source: mz_authz_annotated_join is joiner extend {
  # render_tag
  join_one: g is gated on tenant = g.tenant
} -> { aggregate: c is count() }`);
      expect(sources.mz_authz_annotated_join).toBeDefined();
      expect(() =>
         assertMaterializationEligible(
            sources.mz_authz_annotated_join,
            deriveAnnotationFields(sources.mz_authz_annotated_join),
         ),
      ).not.toThrow();
   });

   it("refuses a pre-aggregation ROLLUP whose base is #(authorize)-gated", async () => {
      // The shape `synthesizePreaggregationModel` emits: the author's gated source
      // imported under an alias, rolled up as a `#@ persist`. This refusal is what
      // keeps `#@ preaggregate` + a row-level gate safe — a rollup groups ACROSS
      // the gated column, so a frozen one could not be row-filtered afterwards at
      // all. The serve path does not rely on it (see `preaggregation_seams.spec.ts`
      // and the routing pre-check in `model.ts`), but nothing else refuses it.
      const sources =
         await persistSources(`##! experimental { persistence composite_sources givens }
given: GROUPS :: number[]
#(access_filter) org_id in $GROUPS
source: orders is duckdb.sql("SELECT 10 AS amount, 'A' AS category, 1 AS org_id")

#@ persist
source: orders__preagg__category is orders -> {
  group_by: category
  aggregate: total__partial is amount.sum()
}`);
      expect(sources.orders__preagg__category).toBeDefined();
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(
            sources.orders__preagg__category,
         ),
      ).toThrow(/authorize/i);
      expect(() =>
         assertMaterializationEligible(
            sources.orders__preagg__category,
            deriveAnnotationFields(sources.orders__preagg__category),
         ),
      ).toThrow(/authorize/i);
   });

   it("refuses a pre-aggregation ROLLUP whose base is a no-given, fixed-predicate #(authorize)-gated source", async () => {
      // Same shape as the persist-origin no-given test above, for the
      // preaggregate/rollup origin: a rollup groups ACROSS the gated column,
      // so a gated source's rollup must not freeze into an ungated,
      // un-row-filterable artifact served to everyone. No-given predicate for
      // the same reason as the persist-origin test above — a given-keyed
      // gate would refuse on `referencesGiven` first.
      const sources =
         await persistSources(`##! experimental { persistence composite_sources }
#(access_filter) org_id = 999
source: orders is duckdb.sql("SELECT 10 AS amount, 'A' AS category, 1 AS org_id") extend {}

#@ persist
source: orders__preagg__dim_category is orders -> {
  group_by: category
  aggregate: total__partial is amount.sum()
}`);
      expect(sources.orders__preagg__dim_category).toBeDefined();
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(
            sources.orders__preagg__dim_category,
         ),
      ).toThrow(/authorize/i);
      expect(() =>
         assertMaterializationEligible(
            sources.orders__preagg__dim_category,
            deriveAnnotationFields(sources.orders__preagg__dim_category),
         ),
      ).toThrow(/authorize/i);
   });

   it("tells a rollup's author to remove #@ preaggregate, not #@ persist they never wrote", async () => {
      // A rollup's name is synthesized and appears nowhere in the author's model,
      // so the default message ("Source 'orders__preagg__category__<hash>' …
      // Drop '#@ persist' from this source") sends them looking for a line that
      // does not exist. The caller passes the `origin` that `build_plan.ts`
      // already reports.
      const sources =
         await persistSources(`##! experimental { persistence composite_sources givens }
given: GROUPS :: number[]
#(access_filter) org_id in $GROUPS
source: orders is duckdb.sql("SELECT 10 AS amount, 'A' AS category, 1 AS org_id")

#@ persist
source: orders__preagg__category is orders -> {
  group_by: category
  aggregate: total__partial is amount.sum()
}`);
      let message = "";
      try {
         assertDatabasePersistNotAuthorizeGated(
            sources.orders__preagg__category,
            sources.orders__preagg__category.name,
            "preaggregate",
         );
      } catch (err) {
         message = (err as Error).message;
      }
      expect(message).toContain("#@ preaggregate");
      expect(message).not.toContain("Drop '#@ persist'");
      // The reason a rollup is a sharper case than an ordinary persist.
      expect(message).toMatch(/groups ACROSS the gated column/);
   });

   it("scopes a merge off the source's own terms, not off the classification", async () => {
      // The two gates refuse different sets: the database one deliberately
      // admits the positional cases (a given in a declared `dimension:`, a
      // join's `on:`, a given-scoped join) that the storage gate refuses. A
      // merge scope taken from the classification is therefore EMPTY for a
      // source one gate admitted while the classifier refused it for an
      // unrelated position — and an empty scope is indistinguishable from "this
      // source is not caller-scoped", so the merge would go out unscoped.
      const sources =
         await persistSources(`##! experimental { persistence givens }
given: ORG_ID :: number is 1
source: raw is duckdb.sql("SELECT 1 as org_id, 2 as amount")

#@ persist name="t"
source: mixed is raw -> { select: * } extend {
  where: org_id = $ORG_ID
  dimension: flagged is org_id = $ORG_ID
}`);
      expect(admitGivenPositions(sources.mixed).ok).toBe(false);
      // Still names the column the source is actually scoped by.
      expect(
         readTimeDynamicTerms(sources.mixed).flatMap((t) => t.columns),
      ).toEqual(["org_id"]);
   });

   it("refuses a merge_key whose scope cannot be fully resolved, on BOTH gates", async () => {
      // One term resolves to a column of the source, one does not: `vis.user_id`
      // is a joined field and names no column of the stored table. Scoping by the
      // half that resolves would give a match narrower than the bare key and
      // still wider than the author's relation, so the source is refused instead.
      const sources =
         await persistSources(`##! experimental { persistence givens }
given:
  ORG_ID  :: number is 1
  USER_ID :: number is 7
source: raw is duckdb.sql("SELECT 1 as org_id, 1 as list_id, 2 as amount")
source: vis is duckdb.sql("SELECT 1 as list_id, 7 as user_id")

#@ persist name="t" storage=lake refresh="incremental" watermark="amount" merge_key="org_id"
source: mixed is raw -> { select: * } extend {
  join_one: v is vis on v.list_id = list_id
  where: org_id = $ORG_ID
  where: v.user_id = $USER_ID
}`);
      const fields = {
         merge_key: "org_id",
         refresh: "incremental",
         watermark: "amount",
         storage: "lake",
      };
      // Storage gate.
      let storageReason = "";
      try {
         assertMaterializationEligible(sources.mixed, fields);
      } catch (err) {
         storageReason = (err as MaterializationEligibilityError).reason ?? "";
      }
      // Database gate, which does not refuse the positional cases and so is the
      // one that would otherwise let this through.
      let databaseReason = "";
      try {
         assertDatabasePersistNotAuthorizeGated(
            sources.mixed,
            "mixed",
            "persist",
            undefined,
            fields,
         );
      } catch (err) {
         databaseReason = (err as MaterializationEligibilityError).reason ?? "";
      }
      expect([storageReason, databaseReason]).toEqual([
         "merge_key_scope_unresolved",
         "merge_key_scope_unresolved",
      ]);
   });

   it("refuses a rollup over a caller-scoped source with followable advice", async () => {
      // Under the one reason every mechanism gives a build that reads a given,
      // with a rollup's own advice. The old message told the author to move the given
      // into the source's extend block, where for a rollup it ALREADY is: it is
      // the rollup READING the source that applies that `where:` and bakes the
      // value.
      const sources =
         await persistSources(`##! experimental { persistence composite_sources givens }
given: tenant :: string is 'acme'
source: base is duckdb.sql("SELECT 10 AS amount, 'A' AS category, 'acme' AS tenant")
source: scoped is base extend { where: tenant = $tenant }

#@ persist
source: scoped__preagg__category is scoped -> {
  group_by: category
  aggregate: total__partial is amount.sum()
}`);
      let err: MaterializationEligibilityError | undefined;
      try {
         assertDatabasePersistNotAuthorizeGated(
            sources.scoped__preagg__category,
            sources.scoped__preagg__category.name,
            "preaggregate",
         );
      } catch (caught) {
         err = caught as MaterializationEligibilityError;
      }
      // The one reason every mechanism gives a build that reads a given;
      // `preaggregate_over_dynamic_source` is retained, no longer produced.
      expect(err?.reason).toBe("given_in_persisted_query");
      // Not the advice that leads nowhere.
      expect(err?.message).not.toContain("extend block");
      expect(err?.message).toContain("no read-time re-application");
   });

   it("refuses a given a joined source contributes to the build, and admits one it does not", async () => {
      // A join reaches the build SQL only when the persisted query READS it,
      // and that is exactly when the given-filtered join condition is baked. So
      // the two halves of this shape get opposite answers, and each asserts the
      // build SQL that justifies its own answer rather than trusting the gate.
      //
      // The pruned half is deliberately admitted, not overlooked. Refusing it
      // would refuse every derivation of a base that merely OFFERS a
      // given-filtered join — a base joining a visibility source, with
      // persisted derivations over it reading different subsets, is an ordinary
      // shape — and a structural walk cannot be narrowed to allow it, because an
      // unread join sits exactly where a read one sits.
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: tenant :: string is 'acme'
source: gated is duckdb.sql("SELECT 1 AS amount, 'acme' AS tenant") extend {
  where: tenant = $tenant
}
source: joiner is duckdb.sql("SELECT 2 AS n, 'acme' AS tenant")
#@ persist name="mz_joined_read"
source: mz_joined_read is joiner extend {
  join_one: g is gated on tenant = g.tenant
} -> { group_by: amt is g.amount; aggregate: c is count() }

#@ persist name="mz_joined_unread"
source: mz_joined_unread is joiner extend {
  join_one: g is gated on tenant = g.tenant
} -> { aggregate: c is count() }`);

      // Read: the join is in the build, and the given's default is baked into
      // its ON condition, where no read-time term could undo it.
      expect(await sources.mz_joined_read.getSQL()).toContain(
         `g_0."tenant"='acme'`,
      );
      expect(() =>
         assertMaterializationEligible(
            sources.mz_joined_read,
            deriveAnnotationFields(sources.mz_joined_read),
         ),
      ).toThrow(MaterializationEligibilityError);

      // Unread: the join is pruned, so nothing given-derived reaches the
      // artifact and there is nothing to strip or re-apply.
      expect(await sources.mz_joined_unread.getSQL()).not.toContain("JOIN");
      assertMaterializationEligible(
         sources.mz_joined_unread,
         deriveAnnotationFields(sources.mz_joined_unread),
      );
      const admitted = admitGivenPositions(sources.mz_joined_unread);
      expect(admitted.ok && admitted.terms).toEqual([]);
   });
});

describe("assertTierEligible", () => {
   const gatedModel = `##! experimental { persistence givens }
given: GROUPS :: number[]
#(access_filter) org_id in $GROUPS
source: orders is duckdb.sql("SELECT 10 AS amount, 'A' AS category, 1 AS org_id")

#@ persist
source: by_category is orders -> {
  group_by: category
  aggregate: total is amount.sum()
}`;
   const refusal = (run: () => void): MaterializationEligibilityError => {
      try {
         run();
      } catch (err) {
         return err as MaterializationEligibilityError;
      }
      throw new Error("expected a refusal");
   };

   it("runs the database gate for a database source, naming the annotation the plan says the author wrote", async () => {
      const source = (await persistSources(gatedModel)).by_category;
      const asPersist = refusal(() => assertTierEligible(source, false, {}));
      expect(asPersist).toBeInstanceOf(MaterializationEligibilityError);
      expect(asPersist.message).toContain("#@ persist");
      const asRollup = refusal(() =>
         assertTierEligible(source, false, {
            tables: {
               [source.sourceID]: { kind: "rollup", rollup: {} as never },
            },
         }),
      );
      expect(asRollup.message).toContain("#@ preaggregate");
   });

   describe("a gate enforced at read (R3)", () => {
      const R3_ON = {
         PUBLISHER_MATERIALIZE: "true",
         PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
      };
      const withEnv = <T>(env: Record<string, string>, run: () => T): T => {
         const { restore } = installSettings({ env });
         try {
            return run();
         } finally {
            restore();
         }
      };
      const inputs = (
         sourceID: string,
         outcome: PersistSourceGateOutcome = {
            classification: "row_level",
            attributed: true,
         },
         materialized = true,
      ) => ({
         tables: {
            [sourceID]: materialized
               ? {
                    kind: "materialize" as const,
                    materialize: {} as never,
                    gate: outcome,
                 }
               : { kind: "persist" as const, gate: outcome },
         },
      });

      it("admits a #(materialize) source's own row-level gate to the storage tier with the gated-sources switch on or off (G3), and only with v2's switch", async () => {
         const source = (await persistSources(gatedModel)).by_category;
         for (const env of [
            R3_ON,
            {
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
            },
         ]) {
            withEnv(env, () =>
               assertTierEligible(source, true, inputs(source.sourceID)),
            );
         }
         const envs: Record<string, string>[] = [
            // A `#(materialize)` plan is v2's: without v2's switch it keeps
            // no gate, whatever the gated switch says.
            {
               PUBLISHER_MATERIALIZE: "false",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
            },
            {
               PUBLISHER_MATERIALIZE: "false",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
            },
         ];
         for (const env of envs) {
            const refused = withEnv(env, () =>
               refusal(() =>
                  assertTierEligible(source, true, inputs(source.sourceID)),
               ),
            );
            expect(refused.reason).toBe("access_filter");
         }
      });

      it("keeps refusing what the served copy cannot enforce: an unproven or joined gate", async () => {
         const source = (await persistSources(gatedModel)).by_category;
         const id = source.sourceID;
         for (const plan of [
            inputs(id, { classification: "rejected", attributed: true }),
            // A gate reached only through a join is not the entry point's.
            inputs(id, { classification: "row_level", attributed: false }),
            {
               tables: {
                  [id]: {
                     kind: "materialize" as const,
                     materialize: {} as never,
                  },
               },
            },
         ]) {
            expect(withEnv(R3_ON, () => gateEnforcedAtRead(id, plan))).toBe(
               false,
            );
            const refused = withEnv(R3_ON, () =>
               refusal(() => assertTierEligible(source, true, plan)),
            );
            expect(refused.reason).toBe("access_filter");
         }
      });

      it("admits a database #(materialize) source's own row-level gate with the gated-sources switch off (G3), and nothing more", async () => {
         const source = (await persistSources(gatedModel)).by_category;
         const id = source.sourceID;
         const G3_ONLY = {
            PUBLISHER_MATERIALIZE: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
         };
         expect(
            withEnv(G3_ONLY, () => gateEnforcedAtRead(id, inputs(id))),
         ).toBe(true);
         withEnv(G3_ONLY, () => assertTierEligible(source, false, inputs(id)));
         // Asked as storage, by the caller or by the plan, it is admitted on
         // the same conditions: G3 keeps the gate on both tiers.
         expect(
            withEnv(G3_ONLY, () => gateEnforcedAtRead(id, inputs(id), true)),
         ).toBe(true);
         expect(
            withEnv(G3_ONLY, () =>
               gateEnforcedAtRead(id, {
                  tables: {
                     [id]: {
                        ...inputs(id).tables[id],
                        materialize: {
                           persist: { storage: "lake" },
                        } as never,
                     },
                  },
               }),
            ),
         ).toBe(true);
         // G3's other conditions hold as for v1: an inherited, unproven or
         // joined gate, a #@ persist source asked as database (the database
         // tier check's), and v2's switch off.
         for (const plan of [
            inputs(id, {
               classification: "row_level",
               attributed: true,
               inheritedRowFilter: true,
            }),
            inputs(id, { classification: "rejected", attributed: true }),
            inputs(id, { classification: "row_level", attributed: false }),
            inputs(id, undefined, false),
         ]) {
            expect(withEnv(G3_ONLY, () => gateEnforcedAtRead(id, plan))).toBe(
               false,
            );
         }
         expect(
            withEnv({ ...G3_ONLY, PUBLISHER_MATERIALIZE: "false" }, () =>
               gateEnforcedAtRead(id, inputs(id)),
            ),
         ).toBe(false);
      });

      it("refuses a database #(materialize) source's own row-level gate with v2's switch off, naming the served copy and the #@ persist alternative", async () => {
         const source = (await persistSources(gatedModel)).by_category;
         const id = source.sourceID;
         for (const gated of ["true", "false"]) {
            const env = {
               PUBLISHER_MATERIALIZE: "false",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: gated,
            };
            expect(withEnv(env, () => gateEnforcedAtRead(id, inputs(id)))).toBe(
               false,
            );
            const refused = withEnv(env, () =>
               refusal(() => assertTierEligible(source, false, inputs(id))),
            );
            expect(refused).toBeInstanceOf(MaterializationEligibilityError);
            expect(refused.reason).toBe("access_filter");
            expect(refused.message).toContain("(database '#(materialize)')");
            expect(refused.message).toContain("PUBLISHER_MATERIALIZE on");
            expect(refused.message).toContain("'#@ persist'");
         }
      });

      it("builds the same gated source database as a #@ persist, whose own gate the served copy keeps (G3)", async () => {
         const source = (await persistSources(gatedModel)).by_category;
         const id = source.sourceID;
         withEnv(
            {
               PUBLISHER_MATERIALIZE: "false",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
            },
            () =>
               assertTierEligible(source, false, inputs(id, undefined, false)),
         );
      });

      it("admits a v1 #@ persist storage= source's own row-level gate under G3, whatever the switches, and nothing more", async () => {
         const source = (await persistSources(gatedModel)).by_category;
         const id = source.sourceID;
         const v1 = (outcome?: PersistSourceGateOutcome) =>
            inputs(id, outcome, false);
         for (const env of [
            R3_ON,
            {
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
            },
            {
               PUBLISHER_MATERIALIZE: "false",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
            },
         ]) {
            expect(withEnv(env, () => gateEnforcedAtRead(id, v1(), true))).toBe(
               true,
            );
            withEnv(env, () => assertTierEligible(source, true, v1()));
            // Asked as database, it is the database tier check's to decide.
            expect(withEnv(env, () => gateEnforcedAtRead(id, v1()))).toBe(
               false,
            );
            for (const plan of [
               v1({ classification: "rejected", attributed: true }),
               v1({ classification: "row_level", attributed: false }),
               { tables: {} },
            ]) {
               expect(
                  withEnv(env, () => gateEnforcedAtRead(id, plan, true)),
               ).toBe(false);
               const refused = withEnv(env, () =>
                  refusal(() => assertTierEligible(source, true, plan)),
               );
               expect(refused.reason).toBe("access_filter");
            }
            const inherited = withEnv(env, () =>
               refusal(() =>
                  assertTierEligible(
                     source,
                     true,
                     v1({
                        classification: "row_level",
                        attributed: true,
                        inheritedRowFilter: true,
                     }),
                  ),
               ),
            );
            expect(inherited.message).toContain(
               "reads an #(access_filter) row-level gate from a source it derives from",
            );
            // A persisted view's or query's build is not a v1 source: its
            // leg passes a gate only with the gated-sources switch.
            const view =
               "v__by_x__view@file:///elig/m.malloy.persist-view.malloy";
            expect(
               withEnv(env, () =>
                  gateEnforcedAtRead(
                     view,
                     inputs(view, undefined, false),
                     true,
                  ),
               ),
            ).toBe(false);
         }
      });

      it("collectSourceEligibility asks as storage: a database-planned gated #(materialize) table is admitted for a storage= bind under G3, and marked kept past its own gate", async () => {
         // The storage bind (admitStorage / refusedAtBind) reads this record,
         // and marks a binding kept past its own gate from `gateKept`.
         const source = (await persistSources(gatedModel)).by_category;
         const id = source.sourceID;
         const G3_ONLY = {
            PUBLISHER_MATERIALIZE: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
         };
         const plan = {
            connectionDigests: {},
            tables: { [id]: { ...inputs(id).tables[id], source } },
         } as unknown as Parameters<typeof collectSourceEligibility>[0];
         // Database, the plan admits it (the precondition for the guard).
         expect(withEnv(G3_ONLY, () => gateEnforcedAtRead(id, plan))).toBe(
            true,
         );
         const { eligible, refused, gateKept } = withEnv(G3_ONLY, () =>
            collectSourceEligibility(plan),
         );
         expect(eligible).toEqual([id]);
         expect(gateKept).toEqual([id]);
         expect(refused).toEqual({});
         // Not admitted past its gate (here, a gate not its own): refused,
         // and never marked kept.
         const joined = withEnv(G3_ONLY, () =>
            collectSourceEligibility({
               ...plan,
               tables: {
                  [id]: {
                     ...inputs(id, {
                        classification: "row_level",
                        attributed: false,
                     }).tables[id],
                     source,
                  },
               },
            } as unknown as Parameters<typeof collectSourceEligibility>[0]),
         );
         expect(joined.eligible).toEqual([]);
         expect(joined.gateKept).toEqual([]);
         expect(joined.refused[id]).toMatch(/authorize|access_filter/i);
      });

      it("collectSourceEligibility never marks an ungated source kept past a gate, so its table keeps a gated answer off G3", async () => {
         const source = (
            await persistSources(`##! experimental { persistence }
source: orders is duckdb.sql("SELECT 10 AS amount, 'A' AS category")

#@ persist storage="lake"
source: by_category is orders -> {
  group_by: category
  aggregate: total is amount.sum()
}`)
         ).by_category;
         const id = source.sourceID;
         for (const env of [
            R3_ON,
            {
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
            },
         ]) {
            // An ungated source classifies as row-level and attributed (no
            // gate to reject): only the gate walk tells it from a gated one.
            for (const materialized of [true, false]) {
               const { eligible, gateKept } = withEnv(env, () =>
                  collectSourceEligibility({
                     connectionDigests: {},
                     tables: {
                        [id]: {
                           ...inputs(id, undefined, materialized).tables[id],
                           source,
                        },
                     },
                  } as unknown as Parameters<
                     typeof collectSourceEligibility
                  >[0]),
               );
               expect(eligible).toEqual([id]);
               expect(gateKept).toEqual([]);
            }
         }
      });

      it("never admits a row filter the source inherits: the served copy cannot graft it", async () => {
         const source = (await persistSources(gatedModel)).by_category;
         const id = source.sourceID;
         const plan = inputs(id, {
            classification: "row_level",
            attributed: true,
            inheritedRowFilter: true,
         });
         // Asked by the build, the serve bind and the host-instructed build.
         expect(withEnv(R3_ON, () => gateEnforcedAtRead(id, plan))).toBe(false);
         const refused = withEnv(R3_ON, () =>
            refusal(() => assertTierEligible(source, true, plan)),
         );
         expect(refused.reason).toBe("access_filter");
         expect(refused.message).toContain(
            "reads an #(access_filter) row-level gate from a source it derives from",
         );
      });

      it("refuses a database persisted view's build over a gated source with the gated-sources switch off, as storage= does, and builds it with the switch on", async () => {
         const author = (await persistSources(gatedModel)).by_category;
         // The same compiled source, as a persisted view's companion build.
         const viewID =
            "by_category__v__view@file:///elig/m.malloy.persist-view.malloy";
         const view = new Proxy(author, {
            get: (target, key) =>
               key === "sourceID" ? viewID : Reflect.get(target, key, target),
         });
         const plan = inputs(view.sourceID, undefined, false);
         const off = withEnv(
            {
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
            },
            () => refusal(() => assertTierEligible(view, false, plan)),
         );
         expect(off.reason).toBe("access_filter");
         expect(off.message).toContain("PUBLISHER_MATERIALIZE_GATED_SOURCES");
         withEnv(R3_ON, () => assertTierEligible(view, false, plan));
         // A v1 source of the same shape is the database tier check's, as
         // before: admitted with the switch off.
         withEnv(
            {
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
            },
            () =>
               assertTierEligible(
                  author,
                  false,
                  inputs(author.sourceID, undefined, false),
               ),
         );
      });

      it("lifts only the gate refusal: every other storage check still runs", async () => {
         const source = (
            await persistSources(`##! experimental { persistence givens }
given: GROUPS :: number[]
given: REGION :: string
#(access_filter) org_id in $GROUPS
source: orders is duckdb.sql("SELECT 10 AS amount, 'A' AS category, 1 AS org_id")

#@ persist storage="lake"
source: mine is orders -> { select: * } extend { dimension: r is $REGION }`)
         ).mine;
         const refused = withEnv(R3_ON, () =>
            refusal(() =>
               assertTierEligible(source, true, inputs(source.sourceID)),
            ),
         );
         expect(refused.reason).not.toBe("authorize");
      });
   });

   it("runs the storage gate for a storage source", async () => {
      const source = (await persistSources(gatedModel)).by_category;
      const viaTier = refusal(() =>
         assertTierEligible(source, true, undefined),
      );
      const direct = refusal(() =>
         assertMaterializationEligible(source, deriveAnnotationFields(source)),
      );
      expect(viaTier.message).toBe(direct.message);
   });
});

describe("assertDatabasePersistNotAuthorizeGated", () => {
   it("refuses a database persist source protected by its own #(authorize) gate", async () => {
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#(authorize) $role = 'analyst'
#@ persist name="mz_database_authz"
source: mz_database_authz is base -> { aggregate: c is count() }`);
      expect(sources.mz_database_authz).toBeDefined();
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(sources.mz_database_authz),
      ).toThrow(MaterializationEligibilityError);
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(sources.mz_database_authz),
      ).toThrow(/authorize/i);
   });

   it("accepts an ungated database persist source", async () => {
      const sources = await persistSources(`##! experimental.persistence
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#@ persist name="mz_database_plain"
source: mz_database_plain is base -> { aggregate: c is count() }`);
      expect(sources.mz_database_plain).toBeDefined();
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(sources.mz_database_plain),
      ).not.toThrow();
   });

   it("accepts a database persist source that references a given but carries no gate, and so does the storage gate", async () => {
      // The given sits in the source's extend block, so it is absent from the
      // build and applied over the artifact at read with each caller's value.
      // Previously written with the given INSIDE the persisted query, which is
      // the shape that bakes the default and serves it to everyone — that one is
      // refused, so this test would otherwise have been asserting the defect as
      // the contract.
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: tenant :: string is 'acme'
source: base is duckdb.sql("SELECT 1 AS amount, 'acme' AS tenant")
#@ persist name="mz_database_given"
source: mz_database_given is base -> { select: * } extend { where: tenant = $tenant }`);
      expect(sources.mz_database_given).toBeDefined();
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(sources.mz_database_given),
      ).not.toThrow();

      // The two gates AGREE on this shape, and the agreement is the point. This
      // assertion used to be the contrast — the storage gate refused a given
      // wherever it sat, and that difference was what "the database check is
      // narrower" meant. The storage gate now asks the same question both do:
      // does the BUILD substitute a value? Here it does not, so both admit it,
      // and the storage side additionally reports the term it will re-apply at
      // read. What still separates the tiers is what they do with that term,
      // not whether they tolerate it.
      assertMaterializationEligible(
         sources.mz_database_given,
         deriveAnnotationFields(sources.mz_database_given),
      );
      const admitted = admitGivenPositions(sources.mz_database_given);
      expect(admitted.ok && admitted.terms).toEqual([
         { code: "tenant = $tenant", givens: ["tenant"], columns: ["tenant"] },
      ]);
   });

   describe("row-level relaxation (gateOutcome)", () => {
      it("admits a gated database source given a proven row_level + attributed outcome", async () => {
         const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#(authorize) $role = 'analyst'
#@ persist name="mz_relaxed"
source: mz_relaxed is base -> { aggregate: c is count() }`);
         expect(sources.mz_relaxed).toBeDefined();
         expect(() =>
            assertDatabasePersistNotAuthorizeGated(
               sources.mz_relaxed,
               sources.mz_relaxed.name,
               "persist",
               { classification: "row_level", attributed: true },
            ),
         ).not.toThrow();
      });

      it("still refuses when the outcome is row_level but not attributed", async () => {
         const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#(authorize) $role = 'analyst'
#@ persist name="mz_unattributed"
source: mz_unattributed is base -> { aggregate: c is count() }`);
         expect(() =>
            assertDatabasePersistNotAuthorizeGated(
               sources.mz_unattributed,
               sources.mz_unattributed.name,
               "persist",
               { classification: "row_level", attributed: false },
            ),
         ).toThrow(MaterializationEligibilityError);
      });

      it("still refuses when the outcome classifies rejected", async () => {
         const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#(authorize) $role = 'analyst'
#@ persist name="mz_rejected_outcome"
source: mz_rejected_outcome is base -> { aggregate: c is count() }`);
         expect(() =>
            assertDatabasePersistNotAuthorizeGated(
               sources.mz_rejected_outcome,
               sources.mz_rejected_outcome.name,
               "persist",
               { classification: "rejected", attributed: true },
            ),
         ).toThrow(MaterializationEligibilityError);
      });

      it("keeps refusing a pre-aggregation rollup UNCONDITIONALLY, even with a row_level + attributed outcome", async () => {
         // A rollup groups across the gated column by construction, so there is
         // no row left to filter afterward — the relaxation must never reach
         // `origin === "preaggregate"` no matter what the outcome says.
         const sources =
            await persistSources(`##! experimental { persistence composite_sources givens }
given: GROUPS :: number[]
#(access_filter) org_id in $GROUPS
source: orders is duckdb.sql("SELECT 10 AS amount, 'A' AS category, 1 AS org_id")

#@ persist
source: orders__preagg__category is orders -> {
  group_by: category
  aggregate: total__partial is amount.sum()
}`);
         expect(() =>
            assertDatabasePersistNotAuthorizeGated(
               sources.orders__preagg__category,
               sources.orders__preagg__category.name,
               "preaggregate",
               { classification: "row_level", attributed: true },
            ),
         ).toThrow(/authorize/i);
      });
   });
});

describe("a given inside the persisted query (database)", () => {
   const MODEL = `##! experimental { persistence givens }
given: ORG_ID :: number is 1
source: raw is duckdb.sql("SELECT 1 AS org_id, 7 AS user_id")

#@ persist name="inside"
source: inside is raw -> { where: org_id = $ORG_ID; select: * }

#@ persist name="outside"
source: outside is raw -> { select: * } extend { where: org_id = $ORG_ID }

#@ persist name="clean"
source: clean is raw -> { select: * }`;

   it("refuses a given the persisted query is built with", async () => {
      // Built with the default substituted, so the table holds one caller's
      // slice and the read path — which swaps only the FROM — serves it to all.
      const sources = await persistSources(MODEL);
      expect(sources.inside).toBeDefined();
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(sources.inside),
      ).toThrow(MaterializationEligibilityError);
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(sources.inside),
      ).toThrow(/persisted query references a given/i);
      // The message has to name the remedy, since the safe shape is one move of
      // placement away from the refused one — and name it as the RULE rather
      // than one spelling of it, because a given applied at read is equally
      // correct written as a dimension, a measure or a join.
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(sources.inside),
      ).toThrow(/out of the persisted query/i);
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(sources.inside),
      ).toThrow(/dimension, measure or join/i);
   });

   it("admits the same given in the source's extend block", async () => {
      // Absent from the build, applied over the artifact at read with each
      // caller's own value. This is the documented form; refusing it would
      // leave row-level access with no materializable shape at all.
      const sources = await persistSources(MODEL);
      expect(sources.outside).toBeDefined();
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(sources.outside),
      ).not.toThrow();
   });

   it("admits a predicate factored through a dimension, measure or join", async () => {
      // These differ from the admitted `where:` only in how the predicate is
      // written, and none of them reaches the build: each compiles to the same
      // unfiltered relation. Refusing them would reject ordinary row-level
      // access models and tell their authors to make a move they already made.
      const sources =
         await persistSources(`##! experimental { persistence givens }
given: ORG_ID :: number is 1
source: raw is duckdb.sql("SELECT 1 AS org_id, 100 AS amount")

#@ persist name="via_dimension"
source: via_dimension is raw -> { select: * } extend {
  dimension: mine is org_id = $ORG_ID
  where: mine
}

#@ persist name="via_measure"
source: via_measure is raw -> { select: * } extend {
  measure: mine_total is amount.sum() { where: org_id = $ORG_ID }
}

#@ persist name="via_extend_only"
source: via_extend_only is raw extend {
  dimension: mine is org_id = $ORG_ID
  where: mine
}`);
      for (const name of ["via_dimension", "via_measure", "via_extend_only"]) {
         expect(sources[name]).toBeDefined();
         expect(() =>
            assertDatabasePersistNotAuthorizeGated(sources[name]),
         ).not.toThrow();
      }
   });

   it("refuses a field the persisted QUERY is built with, wherever it is declared", async () => {
      // The mirror of the case above: the same dimension, used INSIDE the query
      // rather than in the extend block, is substituted into the build. The
      // query carries the usage itself, which is why excluding `fields` from the
      // walk does not let this through.
      const sources =
         await persistSources(`##! experimental { persistence givens }
given: ORG_ID :: number is 1
source: raw is duckdb.sql("SELECT 1 AS org_id, 100 AS amount")

#@ persist name="used_in_query"
source: used_in_query is raw extend {
  dimension: mine is org_id = $ORG_ID
} -> { where: mine; select: * }

#@ persist name="given_in_group_by"
source: given_in_group_by is raw -> {
  group_by: flag is org_id = $ORG_ID
  aggregate: c is count()
}`);
      for (const name of ["used_in_query", "given_in_group_by"]) {
         expect(sources[name]).toBeDefined();
         expect(() =>
            assertDatabasePersistNotAuthorizeGated(sources[name]),
         ).toThrow(MaterializationEligibilityError);
      }
   });

   it("admits a derivation that does not READ the input source's given", async () => {
      // The shape a structural walk cannot get right: a base source that merely
      // OFFERS a given-filtered join, dimension or measure, with persisted
      // derivations over it that use different subsets. None of these three
      // reads the given, none bakes it, and refusing them would reject every
      // derivation of a base that joins a visibility source.
      const sources =
         await persistSources(`##! experimental { persistence givens }
given: ORG_ID :: number is 1
source: raw is duckdb.sql("SELECT 1 AS org_id, 7 AS user_id, 100 AS amt")
source: orgs is duckdb.sql("SELECT 1 AS org_id, 'acme' AS name") extend {
  where: org_id = $ORG_ID
}

source: offers_join is raw extend { join_one: o is orgs on org_id = o.org_id }
#@ persist name="unused_join"
source: unused_join is offers_join -> { select: org_id, user_id }

source: offers_dim is raw extend { dimension: mine is org_id = $ORG_ID }
#@ persist name="unused_dimension"
source: unused_dimension is offers_dim -> { select: org_id, user_id }

source: offers_measure is raw extend {
  measure: scoped is amt.sum() { where: org_id = $ORG_ID }
}
#@ persist name="unused_measure"
source: unused_measure is offers_measure -> {
  group_by: user_id
  aggregate: c is count()
}`);
      for (const name of [
         "unused_join",
         "unused_dimension",
         "unused_measure",
      ]) {
         expect(sources[name]).toBeDefined();
         expect(() =>
            assertDatabasePersistNotAuthorizeGated(sources[name]),
         ).not.toThrow();
      }
   });

   it("refuses the same declarations once the query READS them", async () => {
      // The mirror of the case above, and the pair that decides the whole
      // check: same declarations, same input sources, and the only difference is
      // that the persisted query consumes them — which is exactly when the build
      // substitutes the default.
      const sources =
         await persistSources(`##! experimental { persistence givens }
given: ORG_ID :: number is 1
source: raw is duckdb.sql("SELECT 1 AS org_id, 7 AS user_id, 100 AS amt")
source: orgs is duckdb.sql("SELECT 1 AS org_id, 'acme' AS name") extend {
  where: org_id = $ORG_ID
}

source: offers_join is raw extend { join_one: o is orgs on org_id = o.org_id }
#@ persist name="reads_join"
source: reads_join is offers_join -> {
  group_by: nm is o.name
  aggregate: c is count()
}

source: offers_dim is raw extend { dimension: mine is org_id = $ORG_ID }
#@ persist name="reads_dimension"
source: reads_dimension is offers_dim -> { where: mine; select: org_id, user_id }

source: offers_measure is raw extend {
  measure: scoped is amt.sum() { where: org_id = $ORG_ID }
}
#@ persist name="reads_measure"
source: reads_measure is offers_measure -> {
  group_by: user_id
  aggregate: t is scoped
}`);
      for (const name of ["reads_join", "reads_dimension", "reads_measure"]) {
         expect(sources[name]).toBeDefined();
         expect(() =>
            assertDatabasePersistNotAuthorizeGated(sources[name]),
         ).toThrow(MaterializationEligibilityError);
      }
   });

   it("falls back to the structural walk when the marker is absent", () => {
      // `query.givenUsage` is the compiler's own summary, so if a future version
      // stops emitting it this check would otherwise read "no givens" and admit
      // everything. Synthetic IR rather than Malloy source, because the marker
      // cannot be removed from the compiler's output from here — and this is the
      // branch a compiler change would silently take.
      const noMarker = {
         name: "no_marker",
         _sourceDef: {
            // A query carrying a given, with no `givenUsage` alongside it.
            query: {
               pipeline: [
                  {
                     filterList: [
                        { node: "filterCondition", e: { node: "given" } },
                     ],
                  },
               ],
            },
         },
      } as unknown as PersistSource;
      expect(() => assertDatabasePersistNotAuthorizeGated(noMarker)).toThrow(
         MaterializationEligibilityError,
      );

      // And the same shape with the given only where it is applied at read
      // (a term the source's own statement writes) stays admitted, so the
      // fallback is not a blanket refusal.
      const url = "file:///m.malloy";
      const at = (line: number) => ({
         url,
         range: {
            start: { line, character: 0 },
            end: { line, character: 5 },
         },
      });
      const readTimeOnly = (located: boolean) =>
         ({
            name: "read_time_only",
            _sourceDef: {
               ...(located
                  ? {
                       location: {
                          url,
                          range: {
                             start: { line: 0, character: 0 },
                             end: { line: 5, character: 0 },
                          },
                       },
                    }
                  : {}),
               query: { pipeline: [{}] },
               filterList: [
                  {
                     node: "filterCondition",
                     e: { node: "given" },
                     ...(located
                        ? {
                             refSummary: {
                                givenUsage: [{ id: "G", at: at(2) }],
                             },
                          }
                        : {}),
                  },
               ],
               fields: [{ name: "d", e: { node: "given" } }],
            },
         }) as unknown as PersistSource;
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(readTimeOnly(true)),
      ).not.toThrow();
      // A read-time term that cannot be placed in the source's own statement
      // fails closed: it may be inherited, which the read would not re-apply.
      let refusal: unknown;
      try {
         assertDatabasePersistNotAuthorizeGated(readTimeOnly(false));
      } catch (error) {
         refusal = error;
      }
      expect(refusal).toBeInstanceOf(MaterializationEligibilityError);
      expect((refusal as { reason?: string }).reason).toBe(
         "inherited_where_with_given",
      );
   });

   it("refuses when the compiled definition cannot be read at all", () => {
      const unreadable = {
         name: "unreadable",
         _sourceDef: null,
      } as unknown as PersistSource;
      expect(() => assertDatabasePersistNotAuthorizeGated(unreadable)).toThrow(
         MaterializationEligibilityError,
      );
   });

   it("refuses a given bound as a source ARGUMENT, and admits a constant one", async () => {
      // The channel the marker cannot see: an argument binds while the source
      // the query reads is constructed, so nothing summarises it onto the query
      // and `givenUsage` reads empty — while the build SQL is byte-identical to
      // writing the given inside the query.
      //
      // The constant is the control that keeps this from being a blanket
      // refusal of parameterized reads: it bakes too, but a concrete
      // instantiation has no per-caller binding to lose.
      const sources =
         await persistSources(`##! experimental { persistence givens parameters }
given: ORG_ID :: number is 1
source: raw is duckdb.sql("SELECT 1 AS org_id, 7 AS user_id")
source: pp(x::number) is raw extend { where: org_id = x }

#@ persist name="arg_given"
source: arg_given is pp(x is $ORG_ID) -> { select: * }

#@ persist name="arg_constant"
source: arg_constant is pp(x is 1) -> { select: * }`);
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(sources.arg_given),
      ).toThrow(MaterializationEligibilityError);
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(sources.arg_constant),
      ).not.toThrow();
   });

   it("refuses a given bound into a joined source the query reads", async () => {
      // The argument walk has to descend for this: the given binds a joined
      // source declared on the INPUT, so the holder sits under
      // `structRef.fields`, and `query.givenUsage` is EMPTY even though the
      // build SQL carries the predicate. Checking only the query's own holders
      // admits it — a fail-open, which is why the descent is not narrowed.
      //
      // Its cost is stated rather than pinned: the same shape with a join the
      // query does NOT read is also refused, though nothing reaches the build.
      // That over-refusal is safe and deliberate, and a future signal precise
      // enough to flip it would be an improvement, not a regression — so it is
      // documented on `argumentBindsAGiven` and left unasserted here.
      const sources =
         await persistSources(`##! experimental { persistence givens parameters }
given: ORG_ID :: number is 1
source: raw is duckdb.sql("SELECT 1 AS org_id, 7 AS user_id")
source: joinee is duckdb.sql("SELECT 1 AS org_id, 'a' AS nm")
source: J(x::number) is joinee extend { where: org_id = x }
source: withjoin is raw extend { join_one: j is J(x is $ORG_ID) on org_id = j.org_id }

#@ persist name="join_arg_read"
source: join_arg_read is withjoin -> { group_by: n is j.nm }`);
      expect(sources.join_arg_read).toBeDefined();
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(sources.join_arg_read),
      ).toThrow(MaterializationEligibilityError);
   });

   it("admits a persisted query that references no given", async () => {
      const sources = await persistSources(MODEL);
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(sources.clean),
      ).not.toThrow();
   });

   it("names a rollup by the annotation its author wrote", async () => {
      const sources = await persistSources(MODEL);
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(
            sources.inside,
            "orders__preagg__category",
            "preaggregate",
         ),
      ).toThrow(/Pre-aggregation rollup/i);
   });
});

describe("read-only dialect (database persist)", () => {
   // The SQL Server connector creates no tables, so a database build has
   // nowhere in the source warehouse to write. Refused at load, naming the
   // connection and the dialect, so the author reads it from the package's
   // warnings rather than from a build's first DDL. A faked source, because
   // the gate reads only the dialect name; no SQL Server is needed.
   it("refuses a database #@ persist on a sqlserver source, naming connection and dialect", () => {
      const source = fakeSource({
         name: "orders",
         sourceEntityId: "orders@x",
         connectionName: "mssql",
         dialectName: "sqlserver",
      });
      let caught: unknown;
      try {
         assertDatabasePersistNotAuthorizeGated(source);
      } catch (err) {
         caught = err;
      }
      expect(caught).toBeInstanceOf(MaterializationEligibilityError);
      expect((caught as MaterializationEligibilityError).reason).toBe(
         "read_only_dialect",
      );
      expect((caught as Error).message).toContain("'mssql'");
      expect((caught as Error).message).toContain("'sqlserver'");
      expect((caught as Error).message).toContain("read-only");
   });

   it("names the rollup when the origin is a pre-aggregation", () => {
      const source = fakeSource({
         name: "orders__preagg",
         sourceEntityId: "orders__preagg@x",
         dialectName: "sqlserver",
      });
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(
            source,
            source.name,
            "preaggregate",
         ),
      ).toThrow(
         /Pre-aggregation rollup 'orders__preagg' cannot be materialized/,
      );
   });

   it("leaves a rollup that declares a storage destination to the storage gate", () => {
      // The rollup branch runs this gate before the storage one for every
      // rollup, so a `storage=` rollup over a SQL Server base must not be
      // refused here as database: the storage build names the source types
      // it can read, and this gate's doc says storage is not its question.
      const source = fakeSource({
         name: "orders__preagg",
         sourceEntityId: "orders__preagg@x",
         dialectName: "sqlserver",
         annotationFields: { storage: "lake" },
      });
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(
            source,
            source.name,
            "preaggregate",
            undefined,
            { storage: "lake" },
         ),
      ).not.toThrow();
   });

   it("agrees with canPersist in both directions, over every connection type", async () => {
      // READ_ONLY_DIALECTS (what the gate refuses) and the static canPersist
      // (what a client reads to decide whether to offer persistence) are two
      // statements of one fact. Checked over the whole Connection.type enum,
      // read from api-doc.yaml as the shared contract, so a dialect added to
      // either side alone fails here.
      const { getStaticConnectionAttributes } = await import(
         "../../connection_config"
      );
      const { readFileSync } = await import("node:fs");
      const { resolve } = await import("node:path");
      const apiDoc = readFileSync(
         resolve(import.meta.dir, "../../../../../../api-doc.yaml"),
         "utf8",
      ).replace(/\r\n/g, "\n");
      const start = apiDoc.indexOf("\n    Connection:\n");
      const enumAt = apiDoc.indexOf("enum:", start);
      const types = [
         ...apiDoc
            .slice(enumAt, apiDoc.indexOf("]", enumAt))
            .matchAll(/^\s+(\w+),$/gm),
      ].map((m) => m[1]);
      // Guard the extraction: a regex matching nothing would pass vacuously.
      expect(types).toContain("sqlserver");
      expect(types).toContain("postgres");
      for (const type of types) {
         const attributes = getStaticConnectionAttributes(
            type as Parameters<typeof getStaticConnectionAttributes>[0],
         );
         if (!attributes) continue;
         expect({
            type,
            canPersist: attributes.canPersist,
         }).toEqual({
            type,
            canPersist: !READ_ONLY_DIALECTS.has(attributes.dialectName ?? ""),
         });
      }
   });

   it("admits the same shape on a dialect that can write", () => {
      const source = fakeSource({
         name: "orders",
         sourceEntityId: "orders@x",
         dialectName: "postgres",
      });
      expect(() =>
         assertDatabasePersistNotAuthorizeGated(source),
      ).not.toThrow();
   });
});

describe("gateEnforcedAtRead's tier", () => {
   // The policy's materialize row does not read the tier today, so a wrong
   // tier would pass every table test; this pins what the predicate asks.
   it("asks the gate policy for a #(materialize) table's own tier", () => {
      const spy = spyOn(gatePolicy, "tableKeepsGates");
      try {
         const id = "s@m.malloy";
         const gate = { classification: "row_level", attributed: true };
         for (const [persist, tier] of [
            [{ storage: "lake" }, "storage"],
            [{}, "database"],
            [undefined, "database"],
         ] as const) {
            spy.mockClear();
            gateEnforcedAtRead(id, {
               tables: {
                  [id]: { kind: "materialize", materialize: { persist }, gate },
               },
            } as never);
            expect(spy).toHaveBeenCalledTimes(1);
            expect(spy.mock.calls[0][0]).toMatchObject({
               kind: "materialize",
               tier,
            });
         }
      } finally {
         spy.mockRestore();
      }
   });
});

describe("a storage= source on a connection the build cannot read", () => {
   // The build passes a query through to Postgres, BigQuery or Snowflake, or
   // builds a DuckLake source into its own catalog; the package's DuckDB
   // sandbox it cannot read at all. Decided from the connection's recorded
   // type, never its name.
   const model = `##! experimental.persistence
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#@ persist storage="lake"
source: stored is base -> { group_by: region; aggregate: c is count() }`;
   const refusalOf = (run: () => void): MaterializationEligibilityError => {
      try {
         run();
      } catch (err) {
         return err as MaterializationEligibilityError;
      }
      throw new Error("expected a refusal");
   };

   it("refuses a sandbox source, naming the connection and the remedy", async () => {
      const source = (await persistSources(model)).stored;
      const err = refusalOf(() =>
         assertTierEligible(source, true, {
            connectionTypes: { duckdb: "duckdb" },
         }),
      );
      expect(err).toBeInstanceOf(MaterializationEligibilityError);
      expect(err.reason).toBe("storage_source_unsupported");
      expect(err.message).toContain("connection 'duckdb'");
      expect(err.message).toContain("cannot be passed through");
      expect(err.message).toContain("Drop 'storage='");
      expect(err.message).toContain("warehouse connection");
   });

   it("decides by the recorded type, not the connection's name", async () => {
      const source = (await persistSources(model)).stored;
      for (const type of ["postgres", "bigquery", "snowflake", "ducklake"]) {
         expect(() =>
            assertTierEligible(source, true, {
               connectionTypes: { duckdb: type },
            }),
         ).not.toThrow();
      }
      for (const type of ["motherduck", "mysql", "trino", "sqlserver"]) {
         expect(
            refusalOf(() =>
               assertTierEligible(source, true, {
                  connectionTypes: { duckdb: type },
               }),
            ).reason,
         ).toBe("storage_source_unsupported");
      }
   });

   it("leaves a connection with no recorded type to the build", async () => {
      const source = (await persistSources(model)).stored;
      expect(() => assertTierEligible(source, true, {})).not.toThrow();
   });

   it("does not refuse the same source built in the database tier", async () => {
      const source = (await persistSources(model)).stored;
      expect(() =>
         assertTierEligible(source, false, {
            connectionTypes: { duckdb: "duckdb" },
         }),
      ).not.toThrow();
   });

   it("is refused by the serve side's storage eligibility too", async () => {
      const source = (await persistSources(model)).stored;
      const out = collectSourceEligibility({
         tables: { [source.sourceID]: { source, kind: "persist" } as never },
         connectionDigests: {},
         connectionTypes: { duckdb: "duckdb" },
         addresses: {},
      });
      expect(out.eligible).toEqual([]);
      expect(out.refused[source.sourceID]).toContain(
         "cannot be passed through",
      );
   });
});
