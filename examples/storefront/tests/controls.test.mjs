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

let options = { rows: [], fail: null, gate: null };
globalThis.Publisher = {
   context: { environment: "examples", package: "storefront" },
   query: async () => {
      // `gate` holds the query open so a test can act inside the round trip the
      // real page has, which is where Reset and Back actually land.
      if (options.gate) await options.gate;
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
      rows: choices.map((v) => ({
         [contract.suggest?.dimension ?? "value"]: v,
      })),
      fail,
      gate: null,
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
   assert.equal(el.textContent, "-Nike", "the button shows the filter as written");
   assert.match(el.title, /written by hand/, "and says it will be replaced");
   assert.equal(el.dataset.opaqueFilter, "true");
   el.click();
   await new Promise((r) => setTimeout(r, 0));
   const boxes = [...document.querySelectorAll(".check-panel input")];
   assert.deepEqual(
      boxes.map((b) => b.closest("label").textContent.trim()),
      ["Levi's", "Nike"],
      // Without this the rows added for unlisted values happily give `-Nike` a
      // row of its own, and clicking it sends `\-Nike`: a literal search for
      // those five characters, which is the bug this whole file exists for.
      "an opaque filter gets NO row, because it is not a value to tick",
   );
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

test("an unlisted row can be unticked like any other", async () => {
   // The row is ordinary, not disabled: a plain value IS one this control can
   // encode, it simply is not in the suggestion list.
   const { sent } = await mount(MULTI, "Ties", { choices: ["Levi's", "Nike"] });
   document.querySelector("#given-BRAND").click();
   await new Promise((r) => setTimeout(r, 0));
   const rows = [...document.querySelectorAll(".check-panel input")];
   assert.equal(rows[2].disabled, false, "not disabled");
   rows[2].click();
   assert.equal(sent.at(-1)[1], "", "unticking it clears the filter");
   assert.deepEqual(
      [...document.querySelectorAll(".check-panel input")].map((b) =>
         b.closest("label").textContent.trim(),
      ),
      ["Levi's", "Nike"],
      "and its row goes with it",
   );
});

test("an unlisted row goes away even when another value stays selected", async () => {
   // The repaint guard compared selections only, so it missed a change to the
   // ROW SET: the row lingered, unticked, after the value behind it was gone.
   const { sent } = await mount(MULTI, "Ties", { choices: ["Levi's", "Nike"] });
   document.querySelector("#given-BRAND").click();
   await new Promise((r) => setTimeout(r, 0));
   document.querySelectorAll(".check-panel input")[0].click(); // + Levi's
   const rows = [...document.querySelectorAll(".check-panel input")];
   rows[rows.length - 1].click(); // - Ties
   assert.equal(sent.at(-1)[1], "Levi's");
   assert.deepEqual(
      [...document.querySelectorAll(".check-panel input")].map((b) =>
         b.closest("label").textContent.trim(),
      ),
      ["Levi's", "Nike"],
   );
});

test("the same value twice does not become two rows or a wrong count", async () => {
   // `suggest { query=… }` is author-written and need not return distinct rows,
   // and `?BRAND=Nike, Nike` is a legal filter. Undeduped, unticking one of two
   // identical rows cleared the filter while the other stayed ticked.
   const dup = await mount(MULTI, "Nike", { choices: ["Nike", "Nike"] });
   dup.el.click();
   await new Promise((r) => setTimeout(r, 0));
   assert.equal(document.querySelectorAll(".check-panel input").length, 1);
   document.querySelector(".check-panel input").click();
   assert.equal(dup.sent.at(-1)[1], "");
   assert.equal(
      document.querySelectorAll(".check-panel input:checked").length,
      0,
      "the panel agrees with the filter it just cleared",
   );

   const twice = await mount(MULTI, "Nike, Nike", { choices: ["Levi's", "Nike"] });
   assert.equal(twice.el.textContent, "Nike", "not '2 selected' over one tick");
});

const RANGE = {
   name: "MIN_SALE",
   label: "Min sale",
   type: "filter<number>",
   rangeMin: 0,
   rangeMax: 250,
};
const DATE = {
   name: "SINCE",
   label: "Since",
   type: "date",
   default: "@2023-01-01",
};

test("a slider cannot sit between whole numbers, and says so", async () => {
   // The input has no `step`, so a browser SNAPS a fractional bound to the
   // nearest whole number and the readout is built from where the handle
   // landed: `>= 75.5` drew "≥ $76", and `>= 0.4` drew "any" over filtered
   // data. Asserted on the arithmetic rather than by driving the element,
   // because happy-dom does NOT snap, so driving it here would prove nothing.
   for (const filter of [">= 75.5", ">= 0.4", ">= 249.9"]) {
      const { el } = await mount(RANGE, filter);
      assert.equal(el.dataset.opaqueFilter, "true", filter);
      assert.equal(
         el.parentElement.querySelector("output").textContent,
         filter,
         `${filter} is shown as written rather than rounded`,
      );
   }
});

test("a slider follows Reset and Back", async () => {
   const { widgets, el } = await mount(RANGE, ">= 75");
   assert.equal(el.parentElement.querySelector("output").textContent, "\u2265 $75");
   widgets[0].set("");
   assert.equal(
      el.parentElement.querySelector("output").textContent,
      "any",
      "Reset must not leave a bound on screen over unfiltered data",
   );
   assert.equal(el.dataset.opaqueFilter, "false");
});

test("dragging away from a filter the slider could not draw clears the warning", async () => {
   const { el } = await mount(RANGE, "<= 100");
   assert.equal(el.dataset.opaqueFilter, "true");
   el.value = "120";
   el.dispatchEvent(new window.Event("input", { bubbles: true }));
   assert.equal(el.dataset.opaqueFilter, "false", "a drag IS a new lower bound");
   assert.equal(el.title, "");
   assert.equal(el.parentElement.querySelector("output").textContent, "\u2265 $120");
   el.value = "0";
   el.dispatchEvent(new window.Event("input", { bubbles: true }));
   assert.equal(
      el.parentElement.querySelector("output").textContent,
      "any",
      "dragging to the floor reads as no bound, not as a bound of zero",
   );
});

test("a bound below the slider's own minimum is not drawn as its minimum", async () => {
   const { el } = await mount(RANGE, ">= -5");
   assert.equal(el.dataset.opaqueFilter, "true");
   assert.equal(el.parentElement.querySelector("output").textContent, ">= -5");
});

test("the date box follows Reset back to the default that then filters", async () => {
   const { widgets, el } = await mount(DATE, "2023-06-01");
   widgets[0].set("");
   assert.equal(el.value, "2023-01-01", "not blank, which would claim no bound");
});

test("every control draws the model's declared default when the URL carries none", async () => {
   // An unset given falls back to its declared default server-side, so a
   // control reading "All" or "any" over that states the one thing that is not
   // true. Only the date box used to get this right.
   const withDefault = (c, d) => ({ ...c, default: d });
   const sel = await mount(withDefault(SELECT, "f'Denim'"), "", { choices: ["Denim"] });
   assert.equal(shown(sel.el), "Denim");
   const multi = await mount(withDefault(MULTI, "f'Nike'"), "", { choices: ["Nike"] });
   assert.equal(multi.el.textContent, "Nike");
   const rng = await mount(withDefault(RANGE, "f'>= 50'"), "");
   assert.equal(rng.el.parentElement.querySelector("output").textContent, "\u2265 $50");
});

test("a slider says so when the filter is not a bound it can draw", async () => {
   // `filter<number>` can say things one slider cannot, and all of these parse
   // cleanly and ARE honoured by the server. Each used to draw "any" with the
   // handle at the left, over data that was filtered.
   for (const filter of ["<= 100", "not >= 100", "> 50", "100"]) {
      const { el } = await mount(RANGE, filter);
      const out = el.parentElement.querySelector("output");
      assert.equal(el.dataset.opaqueFilter, "true", filter);
      assert.equal(out.textContent, filter, `${filter} is shown as written`);
      assert.match(el.title, /written by hand/, filter);
   }
});

test("a bound past the slider's own maximum is not restated as the maximum", async () => {
   // The input clamps out-of-range values, so the readout stated $250 while the
   // filter in force was $500.
   const { el } = await mount(RANGE, ">= 500");
   assert.equal(el.value, "250", "the handle clamps, as inputs do");
   assert.equal(
      el.parentElement.querySelector("output").textContent,
      ">= 500",
      "but the readout must not claim 250",
   );
   assert.equal(el.dataset.opaqueFilter, "true");
});

test("an ordinary bound still draws as a bound", async () => {
   const { el } = await mount(RANGE, ">= 75");
   assert.equal(el.value, "75");
   assert.equal(el.parentElement.querySelector("output").textContent, "\u2265 $75");
   assert.equal(el.dataset.opaqueFilter, "false");
   assert.equal(el.title, "");
});

test("clearing the date box shows the default that then filters", async () => {
   // Clearing fires change with "", the given is dropped, and the model's own
   // default is what filters. The box read blank, which is the one thing that
   // is not true.
   const { sent, el } = await mount(DATE, "2023-06-01");
   el.value = "";
   el.dispatchEvent(new window.Event("change", { bubbles: true }));
   assert.deepEqual(sent.at(-1), ["SINCE", ""], "the URL still loses the given");
   assert.equal(el.value, "2023-01-01", "and the box shows what is in force");
});

test("a single select tracks the value it was last given, not the one it was built with", async () => {
   // Reset landing inside the options round trip: without tracking, the options
   // arrive and restore the filter that is no longer set.
   let release;
   const gate = new Promise((r) => (release = r));
   options = { rows: [{ category: "Denim" }], fail: null, gate };
   const host = document.createElement("div");
   document.body.replaceChildren(host);
   const { widgets } = buildControls(host, {
      modelPath: "data_app.malloy",
      contracts: [SELECT],
      values: { CATEGORY: "Denim" },
      onChange: () => {},
   });
   const el = document.querySelector("#given-CATEGORY");
   widgets[0].set(""); // Reset, while the options query is still open
   release();
   await new Promise((r) => setTimeout(r, 0));
   await new Promise((r) => setTimeout(r, 0));
   assert.equal(shown(el), "All categories", "the late options must not restore it");
});

test("a numeric option column still ticks", async () => {
   // `selected` comes back from the grammar as strings while the column is
   // numbers, so comparing them raw ticks nothing while the button and the URL
   // both say the filter is on.
   const { el } = await mount(MULTI, "10", { choices: [1, 2, 10] });
   assert.equal(el.textContent, "10");
   el.click();
   await new Promise((r) => setTimeout(r, 0));
   const ticked = [...document.querySelectorAll(".check-panel input")]
      .filter((b) => b.checked)
      .map((b) => b.closest("label").textContent.trim());
   assert.deepEqual(ticked, ["10"], "the number ticks despite the type mismatch");
});

test("a multiselect whose choices fail to load says why", async () => {
   // The single select has this; its sibling did not.
   const { el } = await mount(MULTI, "", { fail: new Error("boom") });
   assert.equal(el.dataset.optionsFailed, "true");
   assert.match(el.title, /Could not load choices/);
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
   // One meaning across all four controls: "this control cannot show it", which
   // is what the tooltip beside it says. It used to report whether the GRAMMAR
   // could represent the filter, which disagrees exactly here, since a
   // `<select>` cannot draw a two-value list the grammar is happy with.
   assert.equal(el.dataset.opaqueFilter, "true");
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
