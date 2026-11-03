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
    text: "Every field on every published surface carries a classification.",
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
    text: "Every surface that serves PHI is gated by workforce class.",
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
    text: "A surface serves only the classes it declares.",
    evidence: "model",
    check: "ceiling",
  },
  {
    id: "AC-05",
    text: "A join does not extend a surface's access.",
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
    text: "No direct identifier on a de-identified surface.",
    evidence: "model",
    check: "ceiling",
  },
  {
    id: "DI-04",
    text: "No free-text field on a de-identified surface.",
    evidence: "review",
    note: "Free text is not a class; a review reads the research surface's fields.",
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
    note: "Publisher writes one record per query: caller, surface, gate decision, fields and classes read.",
  },
  {
    id: "AU-02",
    text: "An inventory of every surface, gate, class and field, from the served model.",
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
    what: "The limited data set serves the encounter's record number, declared #(classification) none.",
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
      "Loads: a role-gated surface over locked cores is the designed pattern, and the server cannot know MN-02 requires a scope. A biller at one organization reads all 967. The access grid on this page catches it, because it checks every decision against the policy's workforce table.",
  },
];
