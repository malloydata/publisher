// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { test } from "node:test";
import assert from "node:assert/strict";
import { describeRuns, groupByText, runsFor } from "../public/app/ledger.js";

const probe = (domain, persona, sections = ["access"]) => ({
  sections,
  kind: "malloy",
  model: "index.malloy",
  text: `run: ${domain} -> { aggregate: encounters is count() }`,
  persona,
});
const gov = { sections: ["matrix", "controls"], kind: "rest", text: "GET /governance" };

test("a section shows only the requests that fed it", () => {
  const all = [gov, probe("billing", "Biller", ["access", "controls"])];
  assert.equal(runsFor("matrix", all).length, 1);
  assert.equal(runsFor("controls", all).length, 2);
  assert.equal(runsFor("deid", all).length, 0);
});

test("one group per query text, each with every identity it was sent as", () => {
  const groups = groupByText([
    probe("billing", "Biller"),
    probe("research", "Researcher"),
    probe("billing", "Clinician"),
  ]);
  assert.deepEqual(
    groups.map((g) => [g.text.split(" ")[1], g.runs.map((r) => r.persona)]),
    [
      ["billing", ["Biller", "Clinician"]],
      ["research", ["Researcher"]],
    ],
  );
});

test("runs list in the order they were sent, not the order they finished", () => {
  const late = { ...probe("billing", "Biller"), seq: 1 };
  const early = { ...probe("billing", "Clinician"), seq: 2 };
  assert.deepEqual(runsFor("access", [early, late]).map((r) => r.persona), ["Biller", "Clinician"]);
});

test("the summary counts each kind", () => {
  assert.equal(describeRuns([gov, probe("a", "x"), probe("b", "y")]), "1 governance request · 2 Malloy queries");
  assert.equal(describeRuns([probe("a", "x")]), "1 Malloy query");
  assert.equal(describeRuns([]), "no requests");
});
