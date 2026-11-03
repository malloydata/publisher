// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// node --test examples/clinical-records/tests
import assert from "node:assert/strict";
import { test } from "node:test";
import {
  classTally,
  count,
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

const surface = (over) => ({
  authorize: [
    { expression: "'x' in $ROLES", givens: [{ name: "ROLES", secure: true }] },
  ],
  accessFilter: [],
  carries: ["none", "phi.clinical"],
  fields: [
    { classifications: ["none"] },
    { classifications: ["phi.clinical"] },
  ],
  ...over,
});

test("verdicts count classification, secure gates and the ceiling", () => {
  const v = verdicts([
    surface(),
    surface({
      fields: [
        { classifications: [] },
        { classifications: ["phi.direct_identifier"] },
      ],
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
  // The second surface serves a direct identifier it does not carry.
  assert.equal(v.withinCeiling, 1);
});

test("a surface with no #(carries) is not within a ceiling", () => {
  assert.equal(verdicts([surface({ carries: undefined })]).withinCeiling, 0);
});

test("classTally counts a field under each of its classes", () => {
  assert.deepEqual(
    classTally([
      { classifications: ["a", "b"] },
      { classifications: ["a"] },
      { classifications: [] },
    ]),
    { a: 2, b: 1, "(none declared)": 1 },
  );
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
