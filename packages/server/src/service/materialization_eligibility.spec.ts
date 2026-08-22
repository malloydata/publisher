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
import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { MaterializationEligibilityError } from "../errors";
import {
   assertColocatedPersistNotAuthorizeGated,
   assertMaterializationEligible,
} from "./materialization_eligibility";

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
   for (const source of Object.values(compiled.getBuildPlan().sources)) {
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
         assertMaterializationEligible(sources.mz_plain),
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
         assertMaterializationEligible(sources.mz_bound),
      ).not.toThrow();
   });

   it("refuses a source with an unbound (free) parameter", async () => {
      const sources = await persistSources(`##! experimental.persistence
##! experimental.parameters
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#@ persist name="mz_free"
source: mz_free(threshold::number) is base -> { aggregate: c is count() }`);
      expect(sources.mz_free).toBeDefined();
      expect(() => assertMaterializationEligible(sources.mz_free)).toThrow(
         MaterializationEligibilityError,
      );
      expect(() => assertMaterializationEligible(sources.mz_free)).toThrow(
         /unbound parameter/i,
      );
   });

   it("refuses a source that references a given (RLAC security refusal)", async () => {
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: tenant :: string is 'acme'
source: base is duckdb.sql("SELECT 1 AS amount, 'acme' AS tenant")
#@ persist name="mz_given"
source: mz_given is base -> { where: tenant = $tenant; aggregate: c is count() }`);
      expect(sources.mz_given).toBeDefined();
      expect(() => assertMaterializationEligible(sources.mz_given)).toThrow(
         MaterializationEligibilityError,
      );
      expect(() => assertMaterializationEligible(sources.mz_given)).toThrow(
         /given/i,
      );
   });

   it("refuses a source protected by its own #(authorize) gate", async () => {
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string is 'analyst'
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#(authorize) "$role = 'analyst'"
#@ persist name="mz_authz"
source: mz_authz is base -> { aggregate: c is count() }`);
      expect(sources.mz_authz).toBeDefined();
      expect(() => assertMaterializationEligible(sources.mz_authz)).toThrow(
         MaterializationEligibilityError,
      );
      expect(() => assertMaterializationEligible(sources.mz_authz)).toThrow(
         /authorize/i,
      );
   });

   it("refuses a source protected by a no-given, fixed-predicate #(authorize) gate", async () => {
      // Deliberately references NO given (`org_id = 999`, a fixed predicate —
      // `source_line_gate_no_given_reference` at load time, still a valid
      // gate) rather than a `given:`-keyed comparison:
      // `assertMaterializationEligible` checks `referencesGiven` BEFORE
      // `referencesAuthorize`, so a given-keyed gate here would refuse on the
      // given check first and never reach the authorize path this test
      // exists to exercise.
      const sources = await persistSources(`##! experimental.persistence
#(authorize) org_id = 999
source: base is duckdb.sql("SELECT 1 AS org_id") extend {}
#@ persist name="mz_dim_authz"
source: mz_dim_authz is base -> { aggregate: c is count() }`);
      expect(sources.mz_dim_authz).toBeDefined();
      expect(() => assertMaterializationEligible(sources.mz_dim_authz)).toThrow(
         MaterializationEligibilityError,
      );
      expect(() => assertMaterializationEligible(sources.mz_dim_authz)).toThrow(
         /authorize/i,
      );
   });

   it("refuses a source that reaches an #(authorize) gate through a JOIN", async () => {
      // The gate is on the joined source, not on mz_authz_joined itself — a join
      // must not launder an authorize-gated source into a frozen table.
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string is 'analyst'
#(authorize) "$role = 'analyst'"
source: gated is duckdb.sql("SELECT 1 AS amount, 'acme' AS tenant")
source: joiner is duckdb.sql("SELECT 2 AS n, 'acme' AS tenant")
#@ persist name="mz_authz_joined"
source: mz_authz_joined is joiner extend {
  join_one: g is gated on tenant = g.tenant
} -> { aggregate: c is count() }`);
      expect(sources.mz_authz_joined).toBeDefined();
      expect(() =>
         assertMaterializationEligible(sources.mz_authz_joined),
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
given: role :: string is 'analyst'
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#|(authorize)
"$role = 'analyst'"
|#
#@ persist name="mz_block_authz"
source: mz_block_authz is base -> { aggregate: c is count() }`);
      expect(sources.mz_block_authz).toBeDefined();
      expect(() =>
         assertMaterializationEligible(sources.mz_block_authz),
      ).toThrow(MaterializationEligibilityError);
      expect(() =>
         assertMaterializationEligible(sources.mz_block_authz),
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
given: role :: string is 'analyst'
#(authorize) "$role = 'analyst'"
source: gated is duckdb.sql("SELECT 1 AS amount, 'acme' AS tenant")
source: joiner is duckdb.sql("SELECT 2 AS n, 'acme' AS tenant")
#@ persist name="mz_authz_annotated_join"
source: mz_authz_annotated_join is joiner extend {
  # render_tag
  join_one: g is gated on tenant = g.tenant
} -> { aggregate: c is count() }`);
      expect(sources.mz_authz_annotated_join).toBeDefined();
      expect(() =>
         assertMaterializationEligible(sources.mz_authz_annotated_join),
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
#(authorize) "org_id in $GROUPS"
source: orders is duckdb.sql("SELECT 10 AS amount, 'A' AS category, 1 AS org_id")

#@ persist
source: orders__preagg__category is orders -> {
  group_by: category
  aggregate: total__partial is amount.sum()
}`);
      expect(sources.orders__preagg__category).toBeDefined();
      expect(() =>
         assertColocatedPersistNotAuthorizeGated(
            sources.orders__preagg__category,
         ),
      ).toThrow(/authorize/i);
      expect(() =>
         assertMaterializationEligible(sources.orders__preagg__category),
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
#(authorize) org_id = 999
source: orders is duckdb.sql("SELECT 10 AS amount, 'A' AS category, 1 AS org_id") extend {}

#@ persist
source: orders__preagg__dim_category is orders -> {
  group_by: category
  aggregate: total__partial is amount.sum()
}`);
      expect(sources.orders__preagg__dim_category).toBeDefined();
      expect(() =>
         assertColocatedPersistNotAuthorizeGated(
            sources.orders__preagg__dim_category,
         ),
      ).toThrow(/authorize/i);
      expect(() =>
         assertMaterializationEligible(sources.orders__preagg__dim_category),
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
#(authorize) "org_id in $GROUPS"
source: orders is duckdb.sql("SELECT 10 AS amount, 'A' AS category, 1 AS org_id")

#@ persist
source: orders__preagg__category is orders -> {
  group_by: category
  aggregate: total__partial is amount.sum()
}`);
      let message = "";
      try {
         assertColocatedPersistNotAuthorizeGated(
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

   it("refuses a source that reaches a given through a JOIN (not just its own pipeline)", async () => {
      // The given lives on a joined source, not on mz_joined's own where/fields.
      // The compiled struct embeds the joined SourceDef, so the fail-closed walk
      // must still reach it — a join must not launder a given-filtered source.
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: tenant :: string is 'acme'
source: gated is duckdb.sql("SELECT 1 AS amount, 'acme' AS tenant") extend {
  where: tenant = $tenant
}
source: joiner is duckdb.sql("SELECT 2 AS n, 'acme' AS tenant")
#@ persist name="mz_joined"
source: mz_joined is joiner extend {
  join_one: g is gated on tenant = g.tenant
} -> { aggregate: c is count() }`);
      expect(sources.mz_joined).toBeDefined();
      expect(() => assertMaterializationEligible(sources.mz_joined)).toThrow(
         MaterializationEligibilityError,
      );
   });
});

describe("assertColocatedPersistNotAuthorizeGated", () => {
   it("refuses a colocated persist source protected by its own #(authorize) gate", async () => {
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string is 'analyst'
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#(authorize) "$role = 'analyst'"
#@ persist name="mz_colocated_authz"
source: mz_colocated_authz is base -> { aggregate: c is count() }`);
      expect(sources.mz_colocated_authz).toBeDefined();
      expect(() =>
         assertColocatedPersistNotAuthorizeGated(sources.mz_colocated_authz),
      ).toThrow(MaterializationEligibilityError);
      expect(() =>
         assertColocatedPersistNotAuthorizeGated(sources.mz_colocated_authz),
      ).toThrow(/authorize/i);
   });

   it("accepts an ungated colocated persist source", async () => {
      const sources = await persistSources(`##! experimental.persistence
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#@ persist name="mz_colocated_plain"
source: mz_colocated_plain is base -> { aggregate: c is count() }`);
      expect(sources.mz_colocated_plain).toBeDefined();
      expect(() =>
         assertColocatedPersistNotAuthorizeGated(sources.mz_colocated_plain),
      ).not.toThrow();
   });

   it("accepts a colocated persist source that references a given but carries no gate (narrow check does not pull in referencesGiven)", async () => {
      const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: tenant :: string is 'acme'
source: base is duckdb.sql("SELECT 1 AS amount, 'acme' AS tenant")
#@ persist name="mz_colocated_given"
source: mz_colocated_given is base -> { where: tenant = $tenant; aggregate: c is count() }`);
      expect(sources.mz_colocated_given).toBeDefined();
      // assertMaterializationEligible would refuse this (referencesGiven), but
      // the colocated check deliberately does not apply that rule.
      expect(() =>
         assertMaterializationEligible(sources.mz_colocated_given),
      ).toThrow(MaterializationEligibilityError);
      expect(() =>
         assertColocatedPersistNotAuthorizeGated(sources.mz_colocated_given),
      ).not.toThrow();
   });

   describe("row-level relaxation (gateOutcome)", () => {
      it("admits a gated colocated source given a proven row_level + attributed outcome", async () => {
         const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string is 'analyst'
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#(authorize) "$role = 'analyst'"
#@ persist name="mz_relaxed"
source: mz_relaxed is base -> { aggregate: c is count() }`);
         expect(sources.mz_relaxed).toBeDefined();
         expect(() =>
            assertColocatedPersistNotAuthorizeGated(
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
given: role :: string is 'analyst'
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#(authorize) "$role = 'analyst'"
#@ persist name="mz_unattributed"
source: mz_unattributed is base -> { aggregate: c is count() }`);
         expect(() =>
            assertColocatedPersistNotAuthorizeGated(
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
given: role :: string is 'analyst'
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#(authorize) "$role = 'analyst'"
#@ persist name="mz_rejected_outcome"
source: mz_rejected_outcome is base -> { aggregate: c is count() }`);
         expect(() =>
            assertColocatedPersistNotAuthorizeGated(
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
#(authorize) "org_id in $GROUPS"
source: orders is duckdb.sql("SELECT 10 AS amount, 'A' AS category, 1 AS org_id")

#@ persist
source: orders__preagg__category is orders -> {
  group_by: category
  aggregate: total__partial is amount.sum()
}`);
         expect(() =>
            assertColocatedPersistNotAuthorizeGated(
               sources.orders__preagg__category,
               sources.orders__preagg__category.name,
               "preaggregate",
               { classification: "row_level", attributed: true },
            ),
         ).toThrow(/authorize/i);
      });
   });

   describe("PERSIST_COLOCATED_RELAXATION_ENABLED rollback lever", () => {
      const prev = process.env.PERSIST_COLOCATED_RELAXATION_ENABLED;
      afterEach(() => {
         if (prev === undefined) {
            delete process.env.PERSIST_COLOCATED_RELAXATION_ENABLED;
         } else {
            process.env.PERSIST_COLOCATED_RELAXATION_ENABLED = prev;
         }
      });

      it("still admits a proven row_level + attributed outcome with the flag left at its default (on)", async () => {
         delete process.env.PERSIST_COLOCATED_RELAXATION_ENABLED;
         const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string is 'analyst'
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#(authorize) "$role = 'analyst'"
#@ persist name="mz_relaxed_default"
source: mz_relaxed_default is base -> { aggregate: c is count() }`);
         expect(() =>
            assertColocatedPersistNotAuthorizeGated(
               sources.mz_relaxed_default,
               sources.mz_relaxed_default.name,
               "persist",
               { classification: "row_level", attributed: true },
            ),
         ).not.toThrow();
      });

      it("refuses unconditionally when the flag is disabled, even for an otherwise-admissible proven row_level + attributed outcome", async () => {
         process.env.PERSIST_COLOCATED_RELAXATION_ENABLED = "false";
         const sources = await persistSources(`##! experimental.persistence
##! experimental.givens
given: role :: string is 'analyst'
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#(authorize) "$role = 'analyst'"
#@ persist name="mz_rolled_back"
source: mz_rolled_back is base -> { aggregate: c is count() }`);
         expect(() =>
            assertColocatedPersistNotAuthorizeGated(
               sources.mz_rolled_back,
               sources.mz_rolled_back.name,
               "persist",
               { classification: "row_level", attributed: true },
            ),
         ).toThrow(/authorize/i);
      });

      it("does not affect an ungated colocated persist source when the flag is disabled", async () => {
         process.env.PERSIST_COLOCATED_RELAXATION_ENABLED = "false";
         const sources = await persistSources(`##! experimental.persistence
source: base is duckdb.sql("SELECT 1 AS amount, 'US' AS region")
#@ persist name="mz_plain_flag_off"
source: mz_plain_flag_off is base -> { aggregate: c is count() }`);
         expect(() =>
            assertColocatedPersistNotAuthorizeGated(sources.mz_plain_flag_off),
         ).not.toThrow();
      });
   });
});
