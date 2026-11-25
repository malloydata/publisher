// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// node --test examples/clinical-records/tests
import assert from "node:assert/strict";
import { test } from "node:test";
import {
  count,
  declaredClasses,
  declaredFields,
  domainsFrom,
  ofTotal,
  probeOutcome,
  readHash,
  verdicts,
  writeHash,
} from "../public/app/format.js";

test("count shows a dash for missing, never zero", () => {
  assert.equal(count(null), "–");
  assert.equal(count(undefined), "–");
  assert.equal(count(0), "0");
  assert.equal(count(53346), "53,346");
  assert.equal(ofTotal(5, 5), "5 of 5");
});

test("probeOutcome compares the server's answer with the policy's", () => {
  assert.deepEqual(probeOutcome(null, 100, undefined), {
    actual: "refused",
    matches: true,
  });
  assert.deepEqual(probeOutcome(40, 100, "scoped"), {
    actual: "scoped",
    matches: true,
  });
  assert.deepEqual(probeOutcome(100, 100, "rows"), {
    actual: "rows",
    matches: true,
  });
  // A biller who reads every organization: admitted, but not as the policy says.
  assert.deepEqual(probeOutcome(100, 100, "scoped"), {
    actual: "rows",
    matches: false,
  });
  // A persona the policy refuses, admitted anyway.
  assert.deepEqual(probeOutcome(100, 100, undefined), {
    actual: "rows",
    matches: false,
  });
});

const domain = (over) => ({
  authorize: [
    { expression: "'x' in $ROLES", givens: [{ name: "ROLES", secure: true }] },
  ],
  accessFilter: [],
  carries: ["none", "phi.clinical"],
  classCounts: { none: 1, "phi.clinical": 1 },
  fieldCount: 2,
  unclassified: 0,
  fields: [],
  ...over,
});

test("verdicts count classification, secure gates and the ceiling", () => {
  const v = verdicts([
    domain(),
    domain({
      classCounts: { "phi.direct_identifier": 1 },
      unclassified: 1,
      authorize: [
        {
          expression: "'y' in $MODE",
          givens: [{ name: "MODE", secure: false }],
        },
      ],
    }),
  ]);
  assert.equal(v.fields, 4);
  assert.equal(v.classified, 3);
  assert.equal(v.gateGivens, 2);
  assert.equal(v.secureGivens, 1);
  assert.equal(v.surfaces, 2);
  assert.equal(v.gated, 2);
  // The second domain serves a direct identifier it does not carry.
  assert.equal(v.withinCeiling, 1);
});

test("a domain with no #(carries) is not within a ceiling", () => {
  assert.equal(verdicts([domain({ carries: undefined })]).withinCeiling, 0);
});

test("declaredClasses reads one quoted class or a quoted list", () => {
  assert.deepEqual(
    declaredClasses([
      { value: "#(doc) x\n" },
      { value: '#(classification) "none"\n' },
    ]),
    ["none"],
  );
  assert.deepEqual(declaredClasses(['#(classification) ["a", "b"]']), [
    "a",
    "b",
  ]);
  assert.deepEqual(declaredClasses([]), []);
});

test("declaredFields flattens joins to dotted paths", () => {
  const info = {
    schema: {
      fields: [
        {
          kind: "dimension",
          name: "d",
          annotations: [{ value: '#(classification) "none"' }],
        },
        { kind: "measure", name: "m", annotations: [] },
        {
          kind: "join",
          name: "patient",
          schema: {
            fields: [
              {
                kind: "dimension",
                name: "ssn",
                annotations: [
                  { value: '#(classification) "phi.direct_identifier"' },
                ],
              },
            ],
          },
        },
      ],
    },
  };
  assert.deepEqual(declaredFields(info), [
    { path: "d", kind: "dimension", classifications: ["none"] },
    { path: "m", kind: "measure", classifications: [] },
    {
      path: "patient.ssn",
      kind: "dimension",
      classifications: ["phi.direct_identifier"],
    },
  ]);
});

test("domainsFrom joins the summary to the model read's gates and secure givens", () => {
  const [d] = domainsFrom(
    {
      sources: [
        {
          modelPath: "index.malloy",
          name: "chart",
          carries: ["none"],
          fields: 3,
          unclassified: 1,
          classCounts: { none: 2 },
        },
      ],
    },
    {
      "index.malloy": {
        sources: [
          {
            name: "chart",
            authorize: ["'c' in $ROLES"],
            accessFilter: ["org in $ORGS"],
            givens: [
              { name: "ROLES", secure: true },
              { name: "ORGS", secure: false },
            ],
          },
        ],
        sourceInfos: [
          JSON.stringify({ name: "chart", schema: { fields: [] } }),
        ],
      },
    },
  );
  assert.equal(d.fieldCount, 3);
  assert.equal(d.unclassified, 1);
  assert.deepEqual(d.authorize[0].givens, [{ name: "ROLES", secure: true }]);
  assert.deepEqual(d.accessFilter[0].givens, [{ name: "ORGS", secure: false }]);
});

test("hash state round-trips and drops unknown keys", () => {
  const h = writeHash({
    surface: "research",
    cell: "billing:biller",
    empty: "",
  });
  assert.equal(h, "#surface=research&cell=billing%3Abiller");
  assert.deepEqual(readHash(h + "&evil=1", ["surface", "cell"]), {
    surface: "research",
    cell: "billing:biller",
  });
});
