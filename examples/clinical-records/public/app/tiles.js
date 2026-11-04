// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Everything this page queries or states, in one place. Source and field names
// are the model's (surfaces.malloy, core.malloy, deidentify.malloy), read from
// the files, never derived. Policy text is quoted from
// policies/phi-access-and-minimum-necessary.md.
//
// Design brief (agreed before building): for Riverbend's privacy officer and
// the auditor or release engineer who signs off with them, opened at each model
// release and at audit time, to decide whether to sign off: does what this
// model serves match the policy for every workforce class? A scorecard, with a
// per-surface inventory. It leads with four verdicts: classified fields, gates
// on identity-provider attributes, surfaces within their declared ceiling, and
// access decisions that match the policy's workforce table.

export const MODEL = "index.malloy";

/** The data classes, in the policy's order (its Data classification table). */
export const CLASSES = [
  { id: "phi.direct_identifier", label: "Direct identifier" },
  { id: "phi.quasi_identifier", label: "Quasi-identifier" },
  { id: "phi.clinical", label: "Clinical" },
  { id: "phi.financial", label: "Financial" },
  { id: "none", label: "None" },
];

/** The published surfaces, in the policy's workforce-table order. */
export const SURFACES = [
  {
    name: "patient_chart",
    label: "Patient chart",
    who: "Treating clinician, care team",
  },
  { name: "billing", label: "Billing", who: "Billing and claims" },
  {
    name: "population_health",
    label: "Population health",
    who: "Quality analyst",
  },
  {
    name: "partner_lds",
    label: "Partner data set",
    who: "External partner, under a DUA",
  },
  { name: "research", label: "Research", who: "IRB-approved researcher" },
];

/**
 * One organization a demo clinician and biller belong to. In a deployment the
 * identity provider supplies it; this is Cambridge Health Alliance in the
 * synthetic data.
 */
export const DEMO_ORG = "e002090d-4e92-300e-b41e-7d1f21dee4c6";

/**
 * The personas the access grid queries as. Each carries the #(secure) givens
 * the identity provider would set for that workforce class, and what the
 * policy says it may reach: "rows" (admitted, every row), "scoped" (admitted,
 * only its organizations' rows), or nothing (refused).
 */
export const PERSONAS = [
  {
    id: "clinician",
    label: "Clinician",
    givens: { ROLES: ["clinical"], ORGANIZATIONS: [DEMO_ORG], ATTRIBUTES: [] },
    expect: { patient_chart: "scoped" },
  },
  {
    id: "biller",
    label: "Biller",
    givens: { ROLES: ["billing"], ORGANIZATIONS: [DEMO_ORG], ATTRIBUTES: [] },
    expect: { billing: "scoped" },
  },
  {
    id: "analyst",
    label: "Quality analyst",
    givens: { ROLES: ["quality"], ORGANIZATIONS: [], ATTRIBUTES: [] },
    expect: { population_health: "rows" },
  },
  {
    id: "researcher",
    label: "Researcher",
    givens: { ROLES: ["researcher"], ORGANIZATIONS: [], ATTRIBUTES: [] },
    expect: { research: "rows" },
  },
  {
    id: "partner",
    label: "Partner, DUA signed",
    givens: {
      ROLES: ["partner"],
      ORGANIZATIONS: [],
      ATTRIBUTES: ["dua-signed"],
    },
    expect: { partner_lds: "rows" },
  },
  {
    id: "partner-no-dua",
    label: "Partner, no DUA",
    givens: { ROLES: ["partner"], ORGANIZATIONS: [], ATTRIBUTES: [] },
    expect: {},
  },
];

/** The one question every cell of the access grid asks: how many encounters can I read? */
export const probeQuery = (surface) =>
  `run: ${surface} -> { aggregate: encounters is count() }`;

/** The de-identification comparison: the same patients on the limited data set and on research. */
export const DEID = {
  lds: {
    surface: "population_health",
    persona: "analyst",
    ages: "run: population_health -> { group_by: age is patient.age; aggregate: patients is patient.patient_count; order_by: age }",
    precision:
      "run: population_health -> { aggregate: oldest is max(patient.age), places is count(patient.zip), dates is count(started_at.day) }",
  },
  deid: {
    surface: "research",
    persona: "researcher",
    ages: "run: research -> { group_by: age is patient.age; aggregate: patients is patient.patient_count; order_by: age }",
    precision:
      "run: research -> { aggregate: oldest is max(patient.age), places is count(patient.zip3), dates is count(encounter_year) }",
  },
};

/**
 * The policy's controls, with how each is evidenced. "model": declared on a
 * source or field with #(control) and enforced at load. "server": Publisher
 * itself, for every package. "review": a person or a review skill must judge
 * it against the data; nothing mechanical can.
 */
export const CONTROLS = [
  {
    id: "CL-01",
    text: "Every field on every published domain carries a classification.",
    evidence: "model",
    check: "classified",
  },
  {
    id: "CL-02",
    text: "Classification is declared where a field is defined and travels with it.",
    evidence: "review",
    note: "A declaration can be false; lineage shows what a field was derived from, a review judges whether its class is right.",
  },
  {
    id: "AC-01",
    text: "Every domain that serves PHI is gated by workforce class.",
    evidence: "model",
    check: "gated",
  },
  {
    id: "AC-02",
    text: "The base tables are not queryable by anyone.",
    evidence: "model",
  },
  {
    id: "AC-03",
    text: "A gate reads only attributes the identity provider sets.",
    evidence: "model",
    check: "secure",
  },
  {
    id: "AC-04",
    text: "A domain serves only the classes it declares.",
    evidence: "model",
    check: "ceiling",
  },
  {
    id: "AC-05",
    text: "A join does not extend a domain's access.",
    evidence: "server",
    note: "Publisher refuses a package whose published source joins a gated source it does not scope.",
  },
  {
    id: "MN-01",
    text: "A clinician reads full PHI only for patients seen at their organizations.",
    evidence: "model",
    check: "probe",
    surfaces: ["patient_chart"],
  },
  {
    id: "MN-02",
    text: "Billing reads identifiers, codes and financial data for the organizations it bills for.",
    evidence: "review",
    check: "probe",
    surfaces: ["billing"],
    note: "The server cannot know the scope is required. The access grid on this page checks it against the policy's workforce table.",
  },
  {
    id: "MN-03",
    text: "A quality analyst reads a limited data set and no direct identifier.",
    evidence: "model",
    check: "probe",
    surfaces: ["population_health"],
  },
  {
    id: "MN-04",
    text: "A researcher reads de-identified data only.",
    evidence: "model",
    check: "probe",
    surfaces: ["research"],
  },
  {
    id: "MN-05",
    text: "A partner reads a limited data set only under an executed data use agreement.",
    evidence: "model",
    check: "probe",
    surfaces: ["partner_lds"],
  },
  {
    id: "DI-01",
    text: "Dates reduced to the year; ages over 89 reported as 90.",
    evidence: "model",
    check: "deid",
  },
  {
    id: "DI-02",
    text: "Geography reduced to the state or a three-digit ZIP.",
    evidence: "model",
    check: "deid",
  },
  {
    id: "DI-03",
    text: "No direct identifier on a de-identified domain.",
    evidence: "model",
    check: "ceiling",
  },
  {
    id: "DI-04",
    text: "No free-text field on a de-identified domain.",
    evidence: "review",
    note: "Free text is not a class; a review reads the research domain's fields.",
  },
  {
    id: "LDS-01",
    text: "No direct identifier on a limited data set.",
    evidence: "model",
    check: "ceiling",
  },
  {
    id: "LDS-02",
    text: "A limited data set is gated on a DUA for partners, on role for analysts.",
    evidence: "model",
    check: "probe",
    surfaces: ["partner_lds", "population_health"],
  },
  {
    id: "AU-01",
    text: "Every query produces an access record.",
    evidence: "server",
    note: "Publisher writes one record per query: caller, domain, gate decision, fields and classes read.",
  },
  {
    id: "AU-02",
    text: "An inventory of every domain, gate, class and field, from the served model.",
    evidence: "server",
    note: "This page reads it: GET …/packages/clinical-records/governance.",
  },
  {
    id: "AU-03",
    text: "A change that would violate a control is refused, not deployed with a warning.",
    evidence: "server",
    note: "See the seeded violations below.",
  },
];

/** The seeded violations in violations/, with what Publisher does to each. */
export const VIOLATIONS = [
  {
    id: "01",
    what: "Research joins back to the full patient record.",
    controls: "DI-03, AC-04",
    refused: true,
    reason:
      "source research carries [none, phi.clinical, phi.financial, phi.quasi_identifier] but exposes person.ssn, classified phi.direct_identifier.",
  },
  {
    id: "04",
    what: "The clinicians' organization scope reads a given the requester sets.",
    controls: "AC-03, MN-01",
    refused: true,
    reason:
      "source patient_chart is gated on $ORGANIZATIONS, which is not declared #(secure), so a caller can set it and the gate is a filter, not a control.",
  },
  {
    id: "05",
    what: "Billing is re-opened to every caller with #(authorize) true.",
    controls: "AC-01, AC-05",
    refused: true,
    reason:
      "source billing is open to every caller and joins patient, which is locked (#(authorize) false).",
  },
  {
    id: "02",
    what: "The limited data set serves the encounter's record number, declared #(classification) "none".",
    controls: "LDS-01, CL-02",
    refused: false,
    reason:
      "Loads. Every declaration is consistent; one of them is false. A review that reads a sample of the field sees a UUID where the model says there is nothing to protect.",
  },
  {
    id: "03",
    what: "Billing keeps its role gate but drops its organization scope.",
    controls: "MN-02",
    refused: false,
    reason:
      "Loads: a role-gated domain over locked cores is the designed pattern, and the server cannot know MN-02 requires a scope. A biller at one organization reads all 967. The access grid on this page catches it, because it checks every decision against the policy's workforce table.",
  },
];

/* ── Explore as a role ──────────────────────────────────────────────────── */

/** The values the identity editor offers for each #(secure) given. */
export const ROLE_VALUES = [
  { value: "clinical", label: "clinical" },
  { value: "billing", label: "billing" },
  { value: "quality", label: "quality" },
  { value: "researcher", label: "researcher" },
  { value: "partner", label: "partner" },
];

/** Four real organizations from the synthetic data, by encounter volume. */
export const ORGANIZATIONS = [
  {
    id: "be4c63f3-8d38-3fa9-a183-62045b5c85f6",
    name: "VA Boston Healthcare System",
  },
  {
    id: "d311e70d-86e7-3c03-b115-53892bcf7ef1",
    name: "Worcester Outpatient Clinic",
  },
  {
    id: "d692e283-0833-3201-8e55-4f868a9c0736",
    name: "Hallmark Health System",
  },
  { id: DEMO_ORG, name: "Cambridge Health Alliance" },
];

export const ATTRIBUTE_VALUES = [
  { value: "dua-signed", label: "dua-signed (an executed data use agreement)" },
];

/** What each surface's sample shows: the newest rows, every column a model field path. */
export const SAMPLES = {
  patient_chart: {
    fields: [
      "patient.first_name",
      "patient.last_name",
      "patient.ssn",
      "patient.birth_date",
      "organization.organization_name",
      "started_at",
      "encounter_type",
      "reason",
      "claim_cost",
    ],
    order: "started_at desc",
  },
  billing: {
    fields: [
      "encounter_id",
      "patient.last_name",
      "patient.ssn",
      "payer.payer_name",
      "started_at",
      "encounter_type",
      "claim_cost",
      "payer_coverage",
    ],
    order: "started_at desc",
  },
  population_health: {
    fields: [
      "started_at",
      "patient.age",
      "patient.city",
      "patient.zip",
      "encounter_type",
      "reason",
      "claim_cost",
    ],
    order: "started_at desc",
  },
  partner_lds: {
    fields: [
      "started_at",
      "patient.age",
      "patient.city",
      "patient.zip",
      "encounter_type",
      "reason",
      "claim_cost",
    ],
    order: "started_at desc",
  },
  research: {
    fields: [
      "encounter_year",
      "patient.age",
      "patient.zip3",
      "patient.state",
      "encounter_type",
      "reason",
      "claim_cost",
    ],
    order: "encounter_year desc",
  },
};

export const SAMPLE_ROWS = 12;

/** The query behind a surface's sample. One text per surface, built here only. */
export const sampleQuery = (surface) => {
  const s = SAMPLES[surface];
  return `run: ${surface} -> { select: ${s.fields.join(", ")}; order_by: ${s.order}; limit: ${SAMPLE_ROWS} }`;
};

/**
 * What an agent acting for this identity might try, over MCP or anywhere
 * else. Each runs live as the identity in the editor. `on` is a surface the
 * identity can read, for the attempts that start from one. `local` marks the
 * one attempt whose outcome differs on a bare local server: there is no
 * trusted tier here to replace the attributes a request claims.
 */
export const ATTEMPTS = [
  {
    id: "raw-table",
    label: "Read the raw patient table",
    query: () => "run: patients_base -> { select: SSN, FIRST, LAST; limit: 5 }",
    why: "Every table is a base source locked with #(authorize) false and left off the package's published list, index.malloy (AC-02).",
  },
  {
    id: "core",
    label: "Read the identified patient record behind every domain",
    query: () =>
      "run: patients_core -> { select: ssn, first_name, last_name; limit: 5 }",
    why: "Cores are building blocks: not exported from index.malloy, so not a place any caller can start.",
  },
  {
    id: "chart",
    label: "Query the full patient chart",
    query: () =>
      "run: patient_chart -> { select: patient.ssn, patient.last_name; limit: 5 }",
    why: "The chart is locked to the clinical role, and its rows to the caller's organizations (AC-01, MN-01).",
  },
  {
    id: "hidden-column",
    label: "Reach a raw column through a domain it can read",
    query: (on) => `run: ${on} -> { group_by: patient.BIRTHDATE; limit: 5 }`,
    why: "Every raw column is internal; a domain publishes only the named, classified fields.",
  },
  {
    id: "own-join",
    label: "Write its own join back to the identified record",
    query: (on) =>
      `run: ${on} extend { join_one: p is patients_core on PATIENT = p.Id } -> { group_by: p.ssn; limit: 5 }`,
    why: "A caller's join is the caller entering the joined source, which is held to the same published list and the same gates (AC-05).",
  },
  {
    id: "raw-sql",
    label: "Read the data file with SQL",
    query: () =>
      "run: duckdb.table('data/patients.parquet') -> { select: SSN; limit: 5 }",
    why: "A caller cannot declare a data root: query text is held to the model's published sources.",
  },
  {
    id: "claim-role",
    label: "Claim a role it was not given",
    query: () => "run: patient_chart -> { aggregate: encounters is count() }",
    extraRole: "clinical",
    local: true,
    why: "In a deployment ROLES is #(secure): the gateway replaces whatever the request claims, so the claim never reaches the engine. This local server has no gateway, so the claim is honored here, and that difference is the reason #(secure) exists.",
  },
];
