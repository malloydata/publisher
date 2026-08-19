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
     unshowable: "-Denim", ordinary: "Denim" },
   { kind: "multiselect", opaque: true,
     contract: { name: "B", label: "B", type: "filter<string>", control: "multiselect",
                 suggest: { source: "s", dimension: "v" } },
     unshowable: "-Denim", ordinary: "Denim" },
   { kind: "range", opaque: true,
     contract: { name: "C", label: "C", type: "filter<number>", rangeMin: 0, rangeMax: 250 },
     unshowable: "<= 100", ordinary: ">= 75" },
   // `date` takes a date literal rather than filter syntax, and the server
   // REJECTS anything it cannot parse (measured: 400, and the page shows an
   // error on every tile). There is no filter it can silently draw wrongly, so
   // it has no opaque concept to conform to.
   { kind: "date", opaque: false,
     contract: { name: "D", label: "D", type: "date", default: "@2023-01-01" } },
];

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
   const src = await readFile(SRC, "utf8");
   const branches = src.match(/widgets\.push\(/g) ?? [];
   assert.equal(branches.length, KINDS.length,
      "a control kind was added to buildControls without a row in KINDS");
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
