// The control row, built from the model rather than written by hand.
//
// This is the one thing a dashboard gets for free that a hand-authored page has
// to do itself, and it is worth doing properly: the `given:` declarations in
// ../../givens.malloy already say what each filter is called, what widget it
// wants, where its options come from, and what its bounds are. Reading that is
// what lets a control appear here with no edit to this directory.
//
// Adding a filter is three edits, none of them in this directory: declare the
// `given:` in givens.malloy, import it in data_app.malloy, and add a clause
// naming it to `scoped_orders`' `where:`. Skipping the third is the quiet
// failure: the control renders, the value rides the URL, the server accepts it
// because the given IS declared, and not one row is filtered.
//
//   GET /api/v0/…/models/data_app.malloy  ->  { givens: [ { name, type, label,
//     control, suggest, rangeMin, rangeMax, default, description, annotations } ] }
//
// The runtime has no helper for that endpoint, so the page fetches it. Every
// *value* still goes over the wire as a given, never interpolated into a query.

import {
   decodeAtLeast,
   decodeFilterList,
   encodeAtLeast,
   encodeFilterList,
   encodeFilterValue,
   plural,
   readGivenDefault,
} from "./format.js";

/** Fetch the given contracts the page's model declares. */
export async function loadGivenContracts(modelPath) {
   const { environment, package: pkg } = Publisher.context;
   // The model path is ONE encoded segment, slashes included, which is how
   // /sdk/publisher.js addresses it: a model in a subfolder must not become two
   // URL segments.
   const url = `/api/v0/environments/${encodeURIComponent(environment)}/packages/${encodeURIComponent(pkg)}/models/${encodeURIComponent(modelPath)}`;
   const response = await fetch(url, { credentials: "include" });
   if (!response.ok)
      throw new Error(
         `model metadata: ${response.status} ${response.statusText}`,
      );
   const model = await response.json();
   return model.givens ?? [];
}

/**
 * The options a select offers. `suggest` names either a source and one of its
 * dimensions or a model query, and the values come from the model itself, which
 * is why the page never hardcodes a category or a region.
 *
 * The source, query and dimension names are interpolated into a query string
 * here. They come from the model's own `suggest` tag, written by whoever wrote
 * the model, so this is the model author quoting themselves rather than a
 * reader's input reaching the compiler. No value a reader picks or types ever
 * goes through this path: those are all bound as givens.
 */
async function loadOptions(modelPath, contract) {
   const suggest = contract.suggest;
   // The server publishes a `suggest` only when it is runnable, which is a
   // query on its own OR a source-and-dimension pair (see `readGivenControlSpec`
   // in packages/server/src/service/given.ts). Requiring a dimension here would
   // silently drop the query-alone form, leaving a picker with nothing in it and
   // no error to explain why.
   if (!suggest || (!suggest.query && !(suggest.source && suggest.dimension)))
      return [];
   const query = suggest.query
      ? `run: ${suggest.query}`
      : `run: ${suggest.source} -> { group_by: ${suggest.dimension}; order_by: ${suggest.dimension} }`;
   const rows = await Publisher.query(modelPath, query);
   // With `query` alone the first column of each row is the value, which is what
   // the API documents; `dimension` names the column when a query returns more
   // than one.
   const pick = (row) =>
      suggest.dimension ? row[suggest.dimension] : Object.values(row)[0];
   return rows.map(pick).filter((v) => v != null);
}

/**
 * An empty picker and a failed one look identical, and they mean opposite
 * things: "this column has no values" versus "we never found out". Publisher's
 * own SDK draws that distinction explicitly, so the example should not quietly
 * collapse it into a silent empty list.
 */
function optionsFailed(control, contract, error) {
   console.error(`options for ${contract.name}`, error);
   control.title = `Could not load choices: ${error?.message ?? error}`;
   control.dataset.optionsFailed = "true";
}

const isFilter = (contract) => String(contract.type).startsWith("filter<");

/** The one value a single select shows, or "" when the filter is not one it can show. */
const firstValue = (filter) => decodeFilterList(filter)[0] ?? "";

/**
 * A div rather than a label around the whole thing: a label forwards clicks to
 * its control, which would close the multiselect's popover as soon as a checkbox
 * inside it was clicked. The caption is still a real `for=` label.
 */
function labelled(contract, control, description) {
   const wrap = document.createElement("div");
   wrap.className = "control";
   const caption = document.createElement("label");
   caption.className = "control-label";
   caption.htmlFor = `given-${contract.name}`;
   caption.textContent = contract.label ?? contract.name;
   if (description) caption.title = description;
   wrap.append(caption, control);
   return wrap;
}

/**
 * Build the row. `values` maps a given name to its wire value (the same strings
 * the URL and the query body carry); `onChange(name, value)` is called with `""`
 * for "unset", which leaves the model's own default in force.
 */
export function buildControls(host, { modelPath, contracts, values, onChange }) {
   host.replaceChildren();
   const widgets = [];

   for (const contract of contracts) {
      // The server parses `# description="…"` off the declaration and returns
      // it as a field. The page used to pick it out of the raw annotation
      // strings with a regex, which was this page keeping its own copy of an
      // annotation grammar the server already parses.
      const description = contract.description;
      const value = values[contract.name] ?? "";
      const fallback = readGivenDefault(contract.default);
      const args = { modelPath, contract, value, description, onChange };

      if (contract.control === "multiselect") {
         widgets.push(multiSelect(host, args));
      } else if (contract.control === "select") {
         widgets.push(singleSelect(host, args));
      } else if (
         isFilter(contract) &&
         contract.rangeMin !== undefined &&
         contract.rangeMax !== undefined
      ) {
         // Both bounds, per the contract: a slider is what the PAIR signals, so
         // a lone `range_min` or `range_max` is not one (see `Given.rangeMin` in
         // api-doc.yaml). Reading one bound alone would invent the other.
         widgets.push(range(host, args));
      } else if (contract.type === "date") {
         widgets.push(date(host, { ...args, value: value || fallback }));
      }
      // A given with no widget this page knows how to draw is left alone: its
      // model default still applies to every query.
   }

   const reset = document.createElement("button");
   reset.type = "button";
   reset.id = "reset";
   reset.className = "reset";
   reset.textContent = "Reset filters";
   reset.addEventListener("click", () => onChange(null, null));
   host.appendChild(reset);

   return { widgets };
}

function singleSelect(
   host,
   { modelPath, contract, value, description, onChange },
) {
   const select = document.createElement("select");
   select.id = `given-${contract.name}`;
   select.dataset.given = contract.name;
   const all = document.createElement("option");
   all.value = "";
   all.textContent = `All ${plural((contract.label ?? contract.name).toLowerCase())}`;
   select.appendChild(all);
   select.addEventListener("change", () =>
      onChange(contract.name, encodeFilterValue(select.value)),
   );
   host.appendChild(labelled(contract, select, description));

   // `current` rather than the captured `value`: the options arrive a round trip
   // later, and Reset (or a Back) can land inside that window. Restoring the
   // value this widget was BUILT with would then show a filter that is not in
   // force. `set` keeps this in step, so whatever arrives last wins.
   let current = value;
   loadOptions(modelPath, contract)
      .then((options) => {
         for (const option of options) {
            const el = document.createElement("option");
            el.value = el.textContent = option;
            select.appendChild(el);
         }
         select.value = firstValue(current);
      })
      .catch((error) => optionsFailed(select, contract, error));
   return {
      name: contract.name,
      set: (next) => {
         current = next;
         select.value = firstValue(next);
      },
   };
}

/**
 * A checkbox popover, because `control=multiselect` means more than one value
 * and a native multiple select is unusable in a filter row.
 */
function multiSelect(
   host,
   { modelPath, contract, value, description, onChange },
) {
   const button = document.createElement("button");
   button.type = "button";
   button.id = `given-${contract.name}`;
   button.className = "select-like";
   button.dataset.given = contract.name;
   let selected = decodeFilterList(value);
   let options = [];
   let panel = null;

   const caption = () => {
      const noun = plural((contract.label ?? contract.name).toLowerCase());
      if (selected.length === 0) return `All ${noun}`;
      if (selected.length === 1) return selected[0];
      return `${selected.length} selected`;
   };
   const paint = () => {
      button.textContent = caption();
   };
   paint();

   // One call, not a per-value encode and a join: the encoder is what decides
   // which values a filter can carry, and joining its output here would be this
   // page keeping a second opinion about that.
   const commit = () => {
      paint();
      onChange(contract.name, encodeFilterList(selected));
   };

   // Not `{ once: true }`: a pointerdown INSIDE the panel (ticking a checkbox)
   // would spend the listener without closing anything, and the popover would
   // then ignore every later click outside it. The listener is removed when the
   // panel closes instead, which is the event it is actually paired with.
   const onOutsidePointerDown = (event) => {
      if (panel && !panel.contains(event.target) && event.target !== button)
         close();
   };

   const close = () => {
      document.removeEventListener("pointerdown", onOutsidePointerDown);
      panel?.remove();
      panel = null;
   };

   // Rebuilt rather than built once: the options arrive a round trip after the
   // button does, so a reader who opens the popover in that window would get an
   // empty panel that stayed empty until they closed and reopened it.
   const fillPanel = () => {
      if (!panel) return;
      panel.replaceChildren();
      for (const option of options) {
         const row = document.createElement("label");
         row.className = "check-row";
         const box = document.createElement("input");
         box.type = "checkbox";
         box.checked = selected.includes(option);
         box.addEventListener("change", () => {
            selected = box.checked
               ? [...selected, option]
               : selected.filter((v) => v !== option);
            commit();
         });
         const text = document.createElement("span");
         text.textContent = option;
         row.append(box, text);
         panel.appendChild(row);
      }
   };

   button.addEventListener("click", () => {
      if (panel) {
         close();
         return;
      }
      panel = document.createElement("div");
      panel.className = "check-panel";
      fillPanel();
      button.parentElement.appendChild(panel);
      requestAnimationFrame(() => {
         document.addEventListener("pointerdown", onOutsidePointerDown);
      });
   });

   host.appendChild(labelled(contract, button, description));
   loadOptions(modelPath, contract)
      .then((loaded) => {
         options = loaded;
         fillPanel();
      })
      .catch((error) => optionsFailed(button, contract, error));
   return {
      name: contract.name,
      set: (next) => {
         selected = decodeFilterList(next);
         paint();
      },
   };
}

function range(host, { contract, value, description, onChange }) {
   const wrap = document.createElement("span");
   wrap.className = "range";
   const input = document.createElement("input");
   input.type = "range";
   input.id = `given-${contract.name}`;
   input.dataset.given = contract.name;
   input.min = String(contract.rangeMin);
   input.max = String(contract.rangeMax);
   input.value = String(decodeAtLeast(value));
   const readout = document.createElement("output");
   const paint = () => {
      readout.textContent =
         Number(input.value) > 0 ? `≥ $${input.value}` : "any";
   };
   paint();
   input.addEventListener("input", paint);
   // On `change`, not `input`: one query when the drag ends, not one per pixel.
   input.addEventListener("change", () =>
      onChange(contract.name, encodeAtLeast(input.value)),
   );
   wrap.append(input, readout);
   host.appendChild(labelled(contract, wrap, description));
   return {
      name: contract.name,
      set: (next) => {
         input.value = String(decodeAtLeast(next));
         paint();
      },
   };
}

function date(host, { contract, value, description, onChange }) {
   const input = document.createElement("input");
   input.type = "date";
   input.id = `given-${contract.name}`;
   input.dataset.given = contract.name;
   input.value = value;
   input.addEventListener("change", () => onChange(contract.name, input.value));
   host.appendChild(labelled(contract, input, description));
   return {
      name: contract.name,
      set: (next) => {
         input.value = next || readGivenDefault(contract.default);
      },
   };
}
