// The filter controls, driven against a real DOM.
//
// `format.test.mjs` covers the pure encoding module, and that turned out not to
// be coverage of this page at all: a `let` read above its own declaration in
// this file took the page from five controls to one while that suite sat at 17
// pass, 0 fail, and the whole root gate stayed green. Every bug these tests pin
// was found by opening a browser, which is not a thing CI does here.
//
// So the assertions below are deliberately about what the READER sees against
// what is actually in force, because every defect in this file has had the same
// shape: a control that states one filter while the data behind it obeys
// another, drawn perfectly normally.

import { test } from "node:test";
import assert from "node:assert/strict";
import { Window } from "happy-dom";

// controls.js reads `document` and the Publisher browser runtime as globals,
// exactly as the page provides them, so they are installed before it loads.
const window = new Window({ url: "http://localhost/" });
globalThis.window = window;
globalThis.document = window.document;
globalThis.requestAnimationFrame = (fn) => setTimeout(fn, 0);

let options = { rows: [], fail: null };
globalThis.Publisher = {
   context: { environment: "examples", package: "storefront" },
   query: async () => {
      if (options.fail) throw options.fail;
      return options.rows;
   },
};

const { buildControls } = await import("../public/app/controls.js");

const SELECT = {
   name: "CATEGORY",
   label: "Category",
   type: "filter<string>",
   control: "select",
   suggest: { source: "products", dimension: "category" },
};
const MULTI = { ...SELECT, name: "BRAND", label: "Brand", control: "multiselect" };

/** Mount one control and let the options round trip settle. */
async function mount(contract, value, { choices = ["Denim", "Outerwear"], fail = null } = {}) {
   // A deliberately failed load logs through the page's own error path. That is
   // the behaviour under test, so the stack trace is noise rather than signal.
   const realError = console.error;
   if (fail) console.error = () => {};
   options = {
      rows: choices.map((v) => ({ [contract.suggest.dimension]: v })),
      fail,
   };
   const host = document.createElement("div");
   document.body.replaceChildren(host);
   const sent = [];
   const { widgets } = buildControls(host, {
      modelPath: "data_app.malloy",
      contracts: [contract],
      values: { [contract.name]: value },
      onChange: (name, v) => sent.push([name, v]),
   });
   // Two turns: one for the options promise, one for anything it schedules.
   await new Promise((r) => setTimeout(r, 0));
   await new Promise((r) => setTimeout(r, 0));
   console.error = realError;
   return { sent, widgets, el: document.querySelector(`#given-${contract.name}`) };
}

const shown = (select) => select.selectedOptions[0]?.textContent ?? "";

test("ticking a box REPLACES a hand-written filter, it does not extend it", async () => {
   // The bug this file exists for. `-Nike` means everything EXCEPT Nike, which
   // a checkbox list cannot express, so re-encoding it as a literal turns it
   // into a search for those five characters ORed with the pick. The table
   // renders normally either way, which is why nothing caught it.
   const { sent, el } = await mount(MULTI, "-Nike", { choices: ["Levi's", "Nike"] });
   el.click();
   await new Promise((r) => setTimeout(r, 0));
   const boxes = [...document.querySelectorAll(".check-panel input")];
   assert.ok(boxes.length > 0, "the popover has checkboxes");
   assert.equal(
      boxes.filter((b) => b.checked).length,
      0,
      "nothing is pre-ticked while the filter is one the control cannot show",
   );

   boxes[0].click();
   const [, value] = sent.at(-1);
   assert.equal(value, "Levi's", "only the pick is sent");
   assert.ok(!value.includes("Nike"), "the hand-written filter is gone, not OR'd in");
});

test("the panel pre-ticks the values actually in force", async () => {
   // Without this, the "nothing is pre-ticked while opaque" assertion above is
   // satisfied by a control that never pre-ticks anything at all.
   const { el } = await mount(MULTI, "Nike", { choices: ["Levi's", "Nike"] });
   el.click();
   await new Promise((r) => setTimeout(r, 0));
   const ticked = [...document.querySelectorAll(".check-panel input")]
      .filter((b) => b.checked)
      .map((b) => b.closest("label").textContent.trim());
   assert.deepEqual(ticked, ["Nike"]);
});

test("a value in force that the list does not offer still gets a row", async () => {
   // Otherwise the panel reads as "nothing selected" while the button says
   // otherwise, and the next tick ORs the unseen value back in. Measured before
   // the fix: one click on Levi's sent `Ties, Levi's`.
   const { el } = await mount(MULTI, "Ties", { choices: ["Levi's", "Nike"] });
   el.click();
   await new Promise((r) => setTimeout(r, 0));
   const rows = [...document.querySelectorAll(".check-panel input")];
   const labels = rows.map((b) => b.closest("label").textContent.trim());
   assert.deepEqual(labels, ["Levi's", "Nike", "Ties"], "the unlisted value has a row");
   assert.equal(rows[2].checked, true, "and it is ticked, matching the caption");
   assert.equal(
      rows.filter((b) => b.checked).length,
      1,
      "so the panel agrees with the button rather than looking empty",
   );
});

test("a single select drops a pick the encoder refuses, and says All", async () => {
   // The single-select half of the same rule the multiselect is tested for
   // below: display what was SENT, not what was clicked.
   const { sent, el } = await mount(SELECT, "", { choices: ["Nike\t"] });
   el.value = "Nike\t";
   el.dispatchEvent(new window.Event("change", { bubbles: true }));
   assert.equal(sent.at(-1)[1], "", "nothing could be encoded, so nothing is sent");
   assert.equal(shown(el), "All categories", "and the box must not name the value");
});

test("a value already in the list is shown by its own option", async () => {
   // Not by the carried element, which exists only for what the list cannot
   // show. Otherwise every ordinary filter would render through the fallback.
   const { el } = await mount(SELECT, "Denim", { choices: ["Denim", "Outerwear"] });
   assert.equal(shown(el), "Denim");
   assert.equal(el.options.length, 3, "All plus the two choices, and nothing extra");
   assert.equal(el.selectedOptions[0].disabled, false);
});

test("set() follows through to an open popover", async () => {
   // Back with the panel open otherwise repaints the button and leaves the
   // ticks showing the previous selection, so the panel contradicts its own
   // caption and the URL.
   const { widgets, el } = await mount(MULTI, "Nike", { choices: ["Levi's", "Nike"] });
   el.click();
   await new Promise((r) => setTimeout(r, 0));
   assert.equal(document.querySelectorAll(".check-panel input:checked").length, 1);
   widgets[0].set("");
   assert.equal(
      document.querySelectorAll(".check-panel input:checked").length,
      0,
      "the open panel follows the new value",
   );
   assert.equal(el.textContent, "All brands");
});

test("a single select shows a value that is not among its choices", async () => {
   // Reachable from any shared link: the filter is in force server-side, and
   // before this the box rendered blank over filtered data.
   const { el } = await mount(SELECT, "Ties", { choices: ["Denim", "Outerwear"] });
   assert.equal(shown(el), "Ties");
   assert.equal(el.dataset.opaqueFilter, "false", "an ordinary value, just an unlisted one");
   assert.notEqual(el.selectedIndex, -1, "and it is not a blank box");
});

test("a single select shows the filter even when the choices fail to load", async () => {
   const { el } = await mount(SELECT, "Denim", { fail: new Error("boom") });
   assert.equal(shown(el), "Denim", "not 'All categories', which would be a lie");
   assert.match(el.title, /Could not load choices/);
});

test("a failed load and an unshowable filter BOTH keep their explanation", async () => {
   // Two independent authors write this tooltip. Whoever wrote last used to
   // win, so a repaint erased the reason the picker was empty.
   const { el } = await mount(SELECT, "-Denim", { fail: new Error("boom") });
   assert.equal(el.dataset.opaqueFilter, "true");
   assert.match(el.title, /Could not load choices/);
   assert.match(el.title, /written by hand/);
});

test("a single select cannot show a two-value list, and says so", async () => {
   // Representable by the grammar, not by one `<select>`. It used to display
   // the first value only, with no sign the second was also in force.
   const { el } = await mount(SELECT, "Denim, Outerwear");
   assert.equal(shown(el), "Denim, Outerwear");
   assert.equal(el.selectedOptions[0].disabled, true, "and is not re-encodable");
   assert.match(el.title, /written by hand/);
   // `data-opaque-filter` reports whether the GRAMMAR could represent this, so
   // a plain two-value list is false here even though the control cannot show
   // it. Asserted because nothing else in the repo reads this attribute, and an
   // unread attribute drifts.
   assert.equal(el.dataset.opaqueFilter, "false");
});

test("a pick the encoder refuses is not left on display", async () => {
   // A trailing tab does not survive the filter grammar, so `encodeFilterList`
   // drops it and the query goes out unfiltered. The control must follow what
   // was SENT rather than what was clicked.
   const { sent, el } = await mount(MULTI, "", { choices: ["Nike\t"] });
   el.click();
   await new Promise((r) => setTimeout(r, 0));
   const box = document.querySelector(".check-panel input");
   box.click();
   assert.equal(sent.at(-1)[1], "", "nothing is sent, because nothing could be encoded");
   assert.equal(el.textContent, "All brands", "so the button must not name the value");
   assert.equal(
      document.querySelectorAll(".check-panel input:checked").length,
      0,
      "and the box must not stay ticked",
   );
});

test("an ordinary pick still works, in both pickers", async () => {
   const single = await mount(SELECT, "");
   single.el.value = "Denim";
   single.el.dispatchEvent(new window.Event("change", { bubbles: true }));
   assert.deepEqual(single.sent.at(-1), ["CATEGORY", "Denim"]);
   assert.equal(shown(single.el), "Denim");

   const multi = await mount(MULTI, "", { choices: ["Levi's", "Nike"] });
   multi.el.click();
   await new Promise((r) => setTimeout(r, 0));
   document.querySelector(".check-panel input").click();
   assert.deepEqual(multi.sent.at(-1), ["BRAND", "Levi's"]);
   assert.equal(multi.el.textContent, "Levi's");
});
