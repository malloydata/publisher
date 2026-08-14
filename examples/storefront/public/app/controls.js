// The control row, built from the model rather than written by hand.
//
// This is the one thing a dashboard gets for free that a hand-authored page has
// to do itself, and it is worth doing properly: the `given:` declarations in
// ../../givens.malloy already say what each filter is called, what widget it
// wants, where its options come from, and what its bounds are. Reading that
// means adding a given to the model and importing it in data_app.malloy is the
// whole job: a control appears here with no edit to this directory.
//
//   GET /api/v0/…/models/data_app.malloy  ->  { givens: [ { name, type, label,
//     control, suggest, rangeMin, rangeMax, default, annotations } ] }
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
   if (!suggest?.dimension) return [];
   const query = suggest.query
      ? `run: ${suggest.query}`
      : `run: ${suggest.source} -> { group_by: ${suggest.dimension}; order_by: ${suggest.dimension} }`;
   const rows = await Publisher.query(modelPath, query);
   return rows.map((row) => row[suggest.dimension]).filter((v) => v != null);
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

   loadOptions(modelPath, contract).then((options) => {
      for (const option of options) {
         const el = document.createElement("option");
         el.value = el.textContent = option;
         select.appendChild(el);
      }
      select.value = firstValue(value);
   });
   return {
      name: contract.name,
      set: (next) => {
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

   button.addEventListener("click", () => {
      if (panel) {
         close();
         return;
      }
      panel = document.createElement("div");
      panel.className = "check-panel";
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
      button.parentElement.appendChild(panel);
      requestAnimationFrame(() => {
         document.addEventListener("pointerdown", onOutsidePointerDown);
      });
   });

   host.appendChild(labelled(contract, button, description));
   loadOptions(modelPath, contract).then((loaded) => {
      options = loaded;
   });
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
