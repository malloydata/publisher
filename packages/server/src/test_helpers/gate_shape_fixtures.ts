// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Gate-shape fixture packages shared by the specs that load them
 * (`customer_gate_shapes.spec.ts`, which runs their queries) and the specs
 * that only compile them (`gate_walk_differential.spec.ts`, which compares the
 * three gate-inheritance walks over every source in them).
 *
 * Each fixture is a package's files: a map of relative path to Malloy text.
 * Data is inline `duckdb.sql(...)`, so no seeded table is needed.
 */

export const GIVENS_FLAG = "##! experimental.givens\n";

/** An inline-rows source expression over DuckDB. */
export const inlineRows = (sql: string): string => `duckdb.sql("${sql}")`;

const REPORTS = inlineRows(
   "select * from (values (1,'a'),(2,'b'),(1,'c')) as t(internal_report_id, label)",
);

const reportModel = (name: string, extra = "") => `import "metadata.malloy"

#(access_filter) internal_report_id = $INTERNAL_REPORT_ID
source: ${name} is ${REPORTS} extend {
   measure: n is count()
${extra}}
`;

/**
 * A plain scalar given declared in a metadata model and imported by every
 * gated model; `placements` also joins a gated `campaigns`.
 */
export const importedScalarGivenPackage = (): Record<string, string> => ({
   "metadata.malloy": `${GIVENS_FLAG}given: INTERNAL_REPORT_ID :: number\n`,
   "campaigns.malloy": reportModel("campaigns"),
   "creatives.malloy": reportModel("creatives"),
   "placements.malloy": `import "metadata.malloy"
import "campaigns.malloy"

#(access_filter) internal_report_id = $INTERNAL_REPORT_ID
source: placements is ${REPORTS} extend {
   join_one: c is campaigns on c.internal_report_id = internal_report_id
   measure: n is count()
}
`,
   "summary.malloy": reportModel("summary"),
});

/** A plain string given gating `count` sibling sources (`s0`..`s{count-1}`). */
export const stringGivenSiblingsPackage = (
   count = 8,
): Record<string, string> => {
   const orgs = inlineRows(
      "select * from (values ('o1', 1), ('o2', 2), ('o1', 3)) as t(org_id, v)",
   );
   const sources = Array.from({ length: count }, (_, i) => i)
      .map(
         (i) => `#(access_filter) org_id = $AFFINITY_ORG_ID
source: s${i} is ${orgs} extend { measure: n is count() }
`,
      )
      .join("\n");
   return {
      "index.malloy": `${GIVENS_FLAG}given: AFFINITY_ORG_ID :: string\n\n${sources}`,
   };
};

/**
 * Scalar, list and role givens declared in the index model: an `=` row
 * filter, an `in` row filter, a role lock, and a row-filtered source joining
 * another row-filtered one.
 */
export const indexGivensPackage = (): Record<string, string> => {
   const camps = inlineRows(
      "select * from (values (1, 'x'), (2, 'y'), (3, 'z')) as t(camp_id, label)",
   );
   return {
      "index.malloy": `${GIVENS_FLAG}given: CAMP_ID :: number
given: CAMPS :: number[]
given: ROLES :: string[]

#(access_filter) camp_id = $CAMP_ID
source: one_camp is ${camps} extend { measure: n is count() }

#(access_filter) camp_id in $CAMPS
source: many_camps is ${camps} extend { measure: n is count() }

#(authorize) 'director' in $ROLES
source: directors is ${camps} extend { measure: n is count() }

#(access_filter) camp_id = $CAMP_ID
source: audit is ${camps} extend {
   join_one: c is one_camp on c.camp_id = camp_id
   measure: n is count()
}
`,
   };
};

/** The three deployed-customer packages above, by name. */
export const CUSTOMER_GATE_PACKAGES: ReadonlyArray<{
   name: string;
   files: () => Record<string, string>;
}> = [
   { name: "imported scalar given", files: importedScalarGivenPackage },
   { name: "string given siblings", files: () => stringGivenSiblingsPackage() },
   { name: "index givens", files: indexGivensPackage },
];

const ACCOUNTS = inlineRows(
   "select * from (values (1, 'o1', 'east', 10), (2, 'o2', 'west', 20)) as t(id, org_id, region, amount)",
);

/**
 * One model per inheritance shape the gate walks distinguish: own gate,
 * `extend` with and without an annotation of its own (Malloy copies the
 * base's notes onto an unannotated extension and demotes them to
 * `inherits` on an annotated one), a gate on the other route over a locked
 * base, query-derived sources (`x is y -> {...}`) with and without their own
 * gate and chained, joins to gated sources, the bracket and block-form
 * spellings, and composites both bare and derived from.
 */
export const inheritanceMatrixPackage = (): Record<string, string> => ({
   "matrix.malloy": `##! experimental.composite_sources
${GIVENS_FLAG}given: ROLES :: string[]
given: ORGS :: string[]
given: REGION :: string

source: open is ${ACCOUNTS}

#(authorize) 'admin' in $ROLES
source: locked is ${ACCOUNTS}

#(access_filter) org_id in $ORGS
source: scoped is ${ACCOUNTS}

#(authorize) 'admin' in $ROLES
#(access_filter) org_id in $ORGS
source: both is ${ACCOUNTS}

#[authorize] 'bracket' in $ROLES
source: bracket is ${ACCOUNTS}

source:
   #(authorize) 'block' in $ROLES
   block_a is ${ACCOUNTS}
   block_b is ${ACCOUNTS}

source: ext_inherits is locked extend { }

source: ext_inherits_rename is locked extend { rename: area is region }

#(authorize) 'analyst' in $ROLES
source: ext_replaces is locked extend { }

#(access_filter) org_id in $ORGS
source: ext_filter_over_lock is locked extend { }

#(authorize) 'analyst' in $ROLES
source: ext_lock_over_both is both extend { }

# bar_chart
source: ext_annotated is locked extend { }

source: ext_chain is ext_inherits extend { }

# bar_chart
source: ext_chain_annotated is ext_annotated extend { }

source: qs_open is open -> { group_by: region }

source: qs_locked is locked -> { group_by: region, org_id }

#(access_filter) org_id in $ORGS
source: qs_own_over_locked is locked -> { group_by: region, org_id }

#(authorize) 'analyst' in $ROLES
source: qs_same_route_over_locked is locked -> { group_by: region, org_id }

source: qs_over_ext is ext_inherits -> { group_by: region, org_id }

source: qs_chain is qs_locked -> { group_by: region }

source: qs_then_extend is qs_locked extend { }

#(access_filter) region = $REGION
source: qs_then_extend_own is qs_own_over_locked extend { }

source: joins_locked is open extend {
   join_one: l is locked on l.id = id
}

#(access_filter) org_id in $ORGS
source: gated_joins_locked is open extend {
   join_one: l is locked on l.id = id
}

source: comp_bare is compose(locked, open)

source: comp_bare_second_gated is compose(open, scoped)

#(authorize) 'composer' in $ROLES
source: comp_own is compose(locked, scoped)

source: qs_over_comp is comp_bare -> { group_by: region }

source: qs_over_comp_second is comp_bare_second_gated -> { group_by: region }

source: sql_embeds_locked is duckdb.sql("""select * from (%{ locked -> { select: * } })""")
`,
});

/**
 * The import-chain-with-joins shape `entities/source_extraction.matrix.spec.ts`
 * pins (a bridge model reaching a source through transitive imports and
 * joins), with gates on the imported sources: an imported locked base, an
 * extension of it one import up, and a bridge that both derives from and joins
 * through the chain.
 */
export const gatedImportChainPackage = (): Record<string, string> => ({
   "base.malloy": `${GIVENS_FLAG}given: ROLES :: string[]
given: ORGS :: string[]

#(authorize) 'admin' in $ROLES
source: order_items is ${ACCOUNTS} extend { primary_key: id }
`,
   "mid.malloy": `import "base.malloy"

#(access_filter) org_id in $ORGS
source: users is order_items extend { }

source: users_plain is order_items extend { }
`,
   "bridge.malloy": `import "mid.malloy"

source: bridge_model is ${ACCOUNTS} extend {
   join_one: u is users on u.id = id
}

source: bridge_derived is users -> { group_by: org_id, region }

source: bridge_ext is users_plain extend { }
`,
});

/** Every compile-only gate fixture: the customer packages plus the matrix. */
export const GATE_WALK_PACKAGES: ReadonlyArray<{
   name: string;
   files: () => Record<string, string>;
}> = [
   ...CUSTOMER_GATE_PACKAGES,
   { name: "inheritance matrix", files: inheritanceMatrixPackage },
   { name: "gated import chain", files: gatedImportChainPackage },
];
