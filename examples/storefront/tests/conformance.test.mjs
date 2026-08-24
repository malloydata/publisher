// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The rules the controls are supposed to SHARE, checked against every control
// rather than against whichever one a bug surfaced through.
//
// Three rounds of review in a row found the same shape: a rule enforced on one
// control and not its sibling, each time discovered through a different door.
// The multiselect needed the fix the single select got; then three of the
// multiselect's behaviours were pinned by nothing while the single select's
// were; then the slider and the date box needed the rule both pickers had.
// This file exists so the fourth instance is a failing test rather than a
// fourth review round.
//
// The table below is remembered, not derived: `buildControls` dispatches
// through an if/else chain, so there is no registry to enumerate. The first
// test keeps it honest by counting the dispatch branches in the source, which
// is a source-shape check and is described as one rather than dressed up as
// reflection. Add a fifth control and it fails, naming this file.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { Window } from "happy-dom";

const window = new Window({ url: "http://localhost/" });
globalThis.window = window;
globalThis.document = window.document;
globalThis.requestAnimationFrame = (fn) => setTimeout(fn, 0);
globalThis.Publisher = { context: {}, query: async () => [{ v: "Denim" }] };

const SRC = new URL("../public/app/controls.js", import.meta.url);
const { buildControls } = await import(SRC);

// Each control that can receive a filter it cannot draw must say so the same
// way. The table records which controls have that concept; the assertion below
// keeps the table honest against the source.
const KINDS = [
   { kind: "select", opaque: true,
     contract: { name: "A", label: "A", type: "filter<string>", control: "select",
                 suggest: { source: "s", dimension: "v" } },
     unshowable: "-Denim", ordinary: "Denim",
     default: "f'Denim'", shows: (el) => el.selectedOptions[0]?.textContent ?? "",
     expect: "Denim",
     clear: (el) => { el.value = ""; el.dispatchEvent(new window.Event("change", { bubbles: true })); } },
   { kind: "multiselect", opaque: true,
     contract: { name: "B", label: "B", type: "filter<string>", control: "multiselect",
                 suggest: { source: "s", dimension: "v" } },
     unshowable: "-Denim", ordinary: "Denim",
     default: "f'Denim'", shows: (el) => el.textContent, expect: "Denim",
     clear: async (el) => {
        el.click();
        await new Promise((r) => setTimeout(r, 0));
        document.querySelector(".check-panel input:checked")?.click();
     } },
   { kind: "range", opaque: true,
     contract: { name: "C", label: "C", type: "filter<number>", rangeMin: 0, rangeMax: 250 },
     unshowable: "<= 100", ordinary: ">= 75",
     default: "f'>= 50'",
     shows: (el) => el.parentElement.querySelector("output").textContent,
     expect: "\u2265 $50",
     clear: (el) => {
        el.value = "0";
        el.dispatchEvent(new window.Event("change", { bubbles: true }));
     } },
   // `date` takes a date literal rather than filter syntax, so the only way it
   // could lie is by displaying nothing for a value that IS in force. The input
   // blanks anything that is not `YYYY-MM-DD`, so the question is what the
   // server does with those, and it rejects them.
   //
   // Measured against a running server for a `date`-typed given: `2023-06-01`
   // is accepted, while `2023-06-01T00:00:00Z` and `2023-06-01T00:00:00` both
   // return 400 `date must match 'YYYY-MM-DD'`, and the page then shows an
   // error on every tile with the value named. Loud, not silent, so there is no
   // opaque concept to conform to.
   //
   // The ISO timestamp form belongs to a `timestamp`-typed given (docs/givens.md
   // §Wire format), and this page draws no widget for one at all: the dispatcher
   // tests `contract.type === "date"`. That is a gap in what the page COVERS,
   // recorded in the PR body, and not a case of this control drawing wrongly.
   { kind: "date", opaque: false,
     contract: { name: "D", label: "D", type: "date" },
     default: "@2023-01-01", shows: (el) => el.value, expect: "2023-01-01",
     clear: (el) => { el.value = ""; el.dispatchEvent(new window.Event("change", { bubbles: true })); } },
];

// The kinds selected by a `control=` tag, as opposed to by shape (`range`
// needs both bounds) or by type (`date`).
const TAGGED = new Set(["select", "multiselect"]);

async function mountWithWidgets(contract, value) {
   const host = document.createElement("div");
   document.body.replaceChildren(host);
   const { widgets } = buildControls(host, {
      modelPath: "m",
      contracts: [contract],
      values: { [contract.name]: value },
      onChange: () => {},
   });
   await new Promise((r) => setTimeout(r, 0));
   await new Promise((r) => setTimeout(r, 0));
   return { el: document.querySelector(`#given-${contract.name}`), widgets };
}

async function mount(contract, value) {
   const host = document.createElement("div");
   document.body.replaceChildren(host);
   buildControls(host, { modelPath: "m", contracts: [contract], values: { [contract.name]: value },
                         onChange: () => {} });
   await new Promise((r) => setTimeout(r, 0));
   await new Promise((r) => setTimeout(r, 0));
   return document.querySelector(`#given-${contract.name}`);
}

test("the table covers every control the dispatcher can build", async () => {
   // Counting `widgets.push(` was wrong in both directions, measured: a comment
   // containing that text failed the test with a cause that had not happened,
   // and widening a condition to `=== "select" || === "radio"` added a control
   // kind while reusing one push, so it passed. Read the dispatch CONDITIONS
   // instead, with comments stripped first.
   const src = (await readFile(SRC, "utf8")).replace(/\/\/[^\n]*/g, "");
   const body = src.slice(
      src.indexOf("export function buildControls"),
      src.indexOf("\nfunction singleSelect"),
   );
   assert.ok(body.includes("widgets.push("), "found the dispatcher");

   // Branch count: one `if (`, then one `} else if (` per further kind.
   const branches = 1 + (body.match(/\} else if \(/g) ?? []).length;
   assert.equal(
      branches,
      KINDS.length,
      "a control kind was added to buildControls without a row in KINDS",
   );

   // And the control-tag values it dispatches on must be exactly the tagged
   // kinds in the table, so widening one condition cannot smuggle a kind in.
   const tagged = new Set(
      [...body.matchAll(/contract\.control === "([^"]+)"/g)].map((m) => m[1]),
   );
   assert.deepEqual(
      [...tagged].sort(),
      KINDS.filter((k) => TAGGED.has(k.kind)).map((k) => k.kind).sort(),
      "buildControls dispatches on a control tag with no row in KINDS",
   );
});

for (const k of KINDS.filter((k) => k.opaque)) {
   test(`${k.kind}: an unshowable filter is shown, flagged and explained`, async () => {
      const el = await mount(k.contract, k.unshowable);
      assert.equal(el.dataset.opaqueFilter, "true", `${k.kind} flag`);
      assert.match(el.title, /written by hand/, `${k.kind} tooltip`);
      const text = (el.parentElement.textContent ?? "") + (el.textContent ?? "");
      assert.ok(text.includes(k.unshowable), `${k.kind} shows the filter as written`);
   });
   test(`${k.kind}: an ordinary value leaves all of that clear`, async () => {
      const el = await mount(k.contract, k.ordinary);
      assert.equal(el.dataset.opaqueFilter, "false", `${k.kind} flag`);
      assert.equal(el.title, "", `${k.kind} tooltip`);
   });
}

// The server re-applies a given's declared default whenever the given is
// absent, so a control that has just been cleared is showing a filtered result
// and must say which. Every control gets this rule, not the one a bug surfaced
// through: it was fixed for the date box first, then had to be fixed again for
// the other three.
for (const k of KINDS) {
   test(`${k.kind}: draws the declared default when the URL carries none`, async () => {
      const el = await mount({ ...k.contract, default: k.default }, "");
      assert.equal(k.shows(el), k.expect);
   });
   test(`${k.kind}: still draws it after the reader clears the control`, async () => {
      const el = await mount({ ...k.contract, default: k.default }, "");
      await k.clear(el);
      assert.equal(
         k.shows(el),
         k.expect,
         "the given is dropped, so the default is what filters",
      );
   });
   test(`${k.kind}: still draws it through set(), which is Reset and Back`, async () => {
      // `set("")` is the path `syncControls` takes after Reset and on popstate,
      // and it is NOT the change path tested above. Pinned for the date box
      // only until this existed, which is one path over from the bug the change
      // path was just fixed for.
      const { el, widgets } = await mountWithWidgets(
         { ...k.contract, default: k.default },
         "",
      );
      widgets[0].set("");
      assert.equal(k.shows(el), k.expect);
   });
}

// The table's `opaque` column is a claim about the code, so the FALSE rows are
// asserted too. Without this, marking a control opaque-capable when it is not
// (or the reverse) is invisible: the loop above simply skips it.
for (const k of KINDS.filter((k) => !k.opaque)) {
   test(`${k.kind}: has no opaque concept, as the table says`, async () => {
      const el = await mount({ ...k.contract, default: k.default }, "");
      assert.equal(
         el.dataset.opaqueFilter,
         undefined,
         "a control with nothing it can fail to draw must not claim otherwise",
      );
      assert.equal(el.title, "");
   });
}
