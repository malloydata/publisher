// The control row, built from the model rather than written by hand.
//
// This is the one thing a dashboard gets for free that a hand-authored page has
// to do itself, and it is worth doing properly: the `given:` declarations in
// ../../givens.malloy already say what each filter is called, what widget it
// wants, where its options come from, and what its bounds are. Reading that means
// adding a given to the model and importing it in data_app.malloy is the whole
// job — a control appears here with no edit to this directory, exactly as it
// appears on a dashboard.
//
//   GET /api/v0/…/models/data_app.malloy  ->  { givens: [ { name, type, label,
//     control, suggest, rangeMin, rangeMax, default, annotations } ] }
//
// The runtime has no helper for that endpoint yet, so the page fetches it. Every
// *value* still goes over the wire as a given, never interpolated into a query.

import {
   atLeast,
   decodeFilterList,
   decodeFilterValue,
   encodeFilterValue,
   plural,
   readAtLeast,
   readGivenDefault,
} from "./format.js";

/** Fetch the given contracts the page's model declares. */
export async function loadGivenContracts(modelPath) {
   const { environment, package: pkg } = Publisher.context;
   const url = `/api/v0/environments/${encodeURIComponent(environment)}/packages/${encodeURIComponent(pkg)}/models/${modelPath}`;
   const response = await fetch(url, { credentials: "include" });
   if (!response.ok)
      throw new Error(`model metadata: ${response.status} ${response.statusText}`);
   const model = await response.json();
   return model.givens ?? [];
}

/** `#(description="…")` is the hint text under a control. */
function readDescription(contract) {
   for (const annotation of contract.annotations ?? []) {
      const match = /#\(description="([^"]*)"\)/.exec(String(annotation));
      if (match) return match[1];
   }
   return undefined;
}

/**
 * The options a select offers. `suggest` names either a source and one of its
 * dimensions or a model query, and the values come from the model itself: this
 * is why the page never hardcodes a category or a region.
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
      const description = readDescription(contract);
      const value = values[contract.name] ?? "";
      const fallback = readGivenDefault(contract.default);

      if (contract.control === "multiselect") {
         widgets.push(
            multiSelect(host, { modelPath, contract, value, description, onChange }),
         );
      } else if (contract.control === "select") {
         widgets.push(
            singleSelect(host, { modelPath, contract, value, description, onChange }),
         );
      } else if (isFilter(contract) && contract.rangeMax !== undefined) {
         widgets.push(range(host, { contract, value, description, onChange }));
      } else if (contract.type === "date") {
         widgets.push(
            date(host, { contract, value: value || fallback, description, onChange }),
         );
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

function singleSelect(host, { modelPath, contract, value, description, onChange }) {
   const select = document.createElement("select");
   select.id = `given-${contract.name}`;
   select.dataset.given = contract.name;
   const all = document.createElement("option");
   all.value = "";
   all.textContent = `All ${plural((contract.label ?? contract.name).toLowerCase())}`;
   select.appendChild(all);
   select.addEventListener("change", () =>
      onChange(contract.name, encodeFilterValue(select.value) ?? ""),
   );
   host.appendChild(labelled(contract, select, description));

   loadOptions(modelPath, contract).then((options) => {
      for (const option of options) {
         const el = document.createElement("option");
         el.value = el.textContent = option;
         select.appendChild(el);
      }
      select.value = decodeFilterValue(value);
   });
   return {
      name: contract.name,
      set: (next) => {
         select.value = decodeFilterValue(next);
      },
   };
}

/**
 * A checkbox popover, because `control=multiselect` means more than one value and
 * a native multiple select is unusable in a filter row. The value it produces is
 * the same comma-separated filter list the Console's chips produce.
 */
function multiSelect(host, { modelPath, contract, value, description, onChange }) {
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

   const commit = () => {
      paint();
      onChange(
         contract.name,
         selected.map((v) => encodeFilterValue(v)).join(", "),
      );
   };

   const close = () => {
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
         document.addEventListener(
            "pointerdown",
            (event) => {
               if (panel && !panel.contains(event.target) && event.target !== button)
                  close();
            },
            { once: true },
         );
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
   input.min = String(contract.rangeMin ?? 0);
   input.max = String(contract.rangeMax ?? 100);
   input.value = String(readAtLeast(value));
   const readout = document.createElement("output");
   const paint = () => {
      readout.textContent = Number(input.value) > 0 ? `≥ $${input.value}` : "any";
   };
   paint();
   input.addEventListener("input", paint);
   // On `change`, not `input`: one query when the drag ends, not one per pixel.
   input.addEventListener("change", () => onChange(contract.name, atLeast(input.value)));
   wrap.append(input, readout);
   host.appendChild(labelled(contract, wrap, description));
   return {
      name: contract.name,
      set: (next) => {
         input.value = String(readAtLeast(next));
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
