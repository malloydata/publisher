// The control row, built from the model rather than written by hand.
//
// This is the one thing a dashboard gets for free that a hand-authored page has
// to do itself, and it is worth doing properly: the `given:` declarations in
// ../../givens.malloy already say what each filter is called, what widget it
// wants, where its options come from, and what its bounds are. Reading that is
// what lets a control appear here with no edit to this directory.
//
// Adding a filter is three edits, none of them in this directory: declare the
// `given:` in givens.malloy WITH a control tag this file draws, import it in
// data_app.malloy, and add a clause naming it to `scoped_orders`' `where:`.
//
// Two of the three fail quietly. Skip the `where:` clause and the control
// renders, the value rides the URL, the server accepts it because the given IS
// declared, and not one row is filtered. Skip the control tag and nothing
// renders at all: see the end of `buildControls`, which leaves a given it has
// no widget for alone. The tags drawn here are `control=select`,
// `control=multiselect`, a `range_min`/`range_max` pair on a `filter<number>`
// given, and type `date`. Note the type: this page sends `>= n` filter syntax,
// so the bounds alone do not draw a slider on a plain `number` given, which is
// narrower than the rule `api-doc.yaml` states for the contract itself.
//
//   GET /api/v0/…/models/data_app.malloy  ->  { givens: [ { name, type, label,
//     control, suggest, rangeMin, rangeMax, default, description, annotations } ] }
//
// The runtime has no helper for that endpoint, so the page fetches it. Every
// *value* still goes over the wire as a given, never interpolated into a query.

import {
   decodeAtLeast,
   encodeAtLeast,
   encodeFilterList,
   encodeFilterValue,
   readFilterForPicker,
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
   // Deduped by string identity, keeping the first occurrence and its original
   // type. A `group_by` cannot repeat a value, but `suggest { query=… }` is a
   // query the model author writes, and two rows for one value put a tick on
   // screen that the caption and the URL both disagree with: measured, with the
   // list `[Nike, Nike]`, unticking one row cleared the filter while the other
   // stayed ticked.
   //
   // First-wins explicitly. `new Map(entries)` fixes the POSITION at the first
   // key but the VALUE at the last, so the mixed-type case this comment is
   // about, `[1, "1"]`, survived as the string and lost the type the sentence
   // above promises. Measured, and not exposed by the `[Nike, Nike]` case,
   // because there both entries are identical.
   const values = rows.map(pick).filter((v) => v != null);
   const first = new Map();
   for (const v of values) {
      const key = String(v);
      if (!first.has(key)) first.set(key, v);
   }
   return [...first.values()];
}

/**
 * An empty picker and a failed one look identical, and they mean opposite
 * things: "this column has no values" versus "we never found out". Publisher's
 * own SDK draws that distinction explicitly, so the example should not quietly
 * collapse it into a silent empty list.
 */
function optionsFailed(control, contract, error) {
   console.error(`options for ${contract.name}`, error);
   control.dataset.optionsFailed = "true";
   control.dataset.optionsError = `Could not load choices: ${error?.message ?? error}`;
   composeTitle(control);
}

/**
 * The tooltip has two independent authors: the failed-choices message, written
 * once, and the control's own help, rewritten on every repaint. Whoever wrote
 * last used to win, so a repaint erased the explanation of an empty picker.
 * Both are stored and the title is composed from them here, so neither can
 * silently delete the other.
 */
function composeTitle(control, help) {
   if (help !== undefined) {
      if (help) control.dataset.controlHelp = help;
      else delete control.dataset.controlHelp;
   }
   control.title = [control.dataset.optionsError, control.dataset.controlHelp]
      .filter(Boolean)
      .join(" ");
}

const OPAQUE_HELP =
   "This filter was written by hand and this control cannot show it. Changing the selection replaces it.";

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
      // The server parses `# description="…"` off the declaration and returns
      // it as a field. The page used to pick it out of the raw annotation
      // strings with a regex, which was this page keeping its own copy of an
      // annotation grammar the server already parses.
      const description = contract.description;
      const value = values[contract.name] ?? "";
      // `value || fallback`, for every control rather than just the date box.
      // An unset given falls back to its DECLARED default server-side (see
      // docs/givens.md), so a control drawing "All" or "any" over that is
      // stating the one thing that is not true. Latent while every filter given
      // here defaults to `f''`, and live the moment a reader gives one a real
      // default, as `SINCE` does in givens.malloy.
      const fallback = readGivenDefault(contract.default);
      const args = {
         modelPath,
         contract,
         value: value || fallback,
         fallback,
         description,
         onChange,
      };

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
         widgets.push(date(host, args));
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
   { modelPath, contract, value, fallback, description, onChange },
) {
   const select = document.createElement("select");
   select.id = `given-${contract.name}`;
   select.dataset.given = contract.name;
   const all = document.createElement("option");
   all.value = "";
   all.textContent = `All ${plural((contract.label ?? contract.name).toLowerCase())}`;
   select.appendChild(all);
   host.appendChild(labelled(contract, select, description));

   // The control has to show the filter actually in force, and the option list
   // is not always able to. It is empty until the choices arrive a round trip
   // later, it stays empty if that query fails, the value may not be among the
   // choices, and some filters are things one `<select>` cannot express at all:
   // `-Outerwear` (everything EXCEPT Outerwear), or the two-value list
   // `Denim, Outerwear`. Measured, each of those rendered either "All
   // categories" or a blank box while the data behind it stayed filtered, which
   // is the same lie the rest of this page exists to remove.
   //
   // So one option element carries whatever the list cannot. It stays
   // selectable while it holds a plain single value, because picking it again
   // is a no-op and the change handler can encode it. It is disabled when it
   // holds a filter this control could not re-encode without changing its
   // meaning, and then the tooltip says choosing something replaces it.
   const carried = document.createElement("option");
   const showFilter = (filter) => {
      const { values, opaque } = readFilterForPicker(filter);
      const plain = !opaque && values.length === 1;
      const listed =
         plain &&
         [...select.options].some((o) => o !== carried && o.value === values[0]);
      if (!filter || listed) {
         carried.remove();
         select.value = plain ? values[0] : "";
      } else {
         carried.disabled = !plain;
         carried.value = plain ? values[0] : filter;
         carried.textContent = values.length ? values.join(", ") : filter;
         if (!carried.isConnected) select.appendChild(carried);
         select.value = carried.value;
      }
      // "This control cannot show the filter", NOT "the grammar could not
      // represent it". The two disagree for a plain two-value list, which a
      // `<select>` cannot draw even though the grammar is perfectly happy, and
      // the flag is meant to match the tooltip beside it. All three controls
      // that can receive a filter answer the same question now; the date box
      // takes a date literal and so has nothing it can fail to draw.
      const undrawable = !!filter && !plain;
      select.dataset.opaqueFilter = String(undrawable);
      composeTitle(select, undrawable ? OPAQUE_HELP : "");
   };

   // `current` rather than the captured `value`: the options arrive a round trip
   // later, and Reset (or a Back) can land inside that window. Restoring the
   // value this widget was BUILT with would then show a filter that is not in
   // force. `set` keeps this in step, so whatever arrives last wins.
   let current = value;
   showFilter(value);

   // Registered here rather than beside the element because it needs `current`
   // and `showFilter`. It has to clear its own opaque state: the app does not
   // call `set` on the widget that raised the change, so nothing else will, and
   // the control would go on showing the hand-written filter it just replaced.
   // Updating `current` matters for the same reason, since options can arrive
   // after this and would otherwise restore the filter that is no longer set.
   select.addEventListener("change", () => {
      // Re-read the display from `current` rather than trusting the click: the
      // encoder DROPS a value it cannot carry, so the control must show what
      // was sent. A value with a stray tab encodes to "" (All), and without
      // this the box went on naming that value over unfiltered data.
      // Send first, then draw what is in FORCE. Choosing "All" sends `""`, the
      // given is dropped, and the server then re-applies the model's declared
      // default, so a box reading "All categories" over that is the same lie
      // this control was fixed for. The date box already worked this way.
      const sent = encodeFilterValue(select.value);
      onChange(contract.name, sent);
      current = sent || fallback;
      showFilter(current);
   });
   loadOptions(modelPath, contract)
      .then((options) => {
         for (const option of options) {
            const el = document.createElement("option");
            el.value = el.textContent = option;
            select.appendChild(el);
         }
         showFilter(current);
      })
      .catch((error) => optionsFailed(select, contract, error));
   return {
      name: contract.name,
      set: (next) => {
         current = next || fallback;
         showFilter(current);
      },
   };
}

/**
 * A checkbox popover, because `control=multiselect` means more than one value
 * and a native multiple select is unusable in a filter row.
 */
function multiSelect(
   host,
   { modelPath, contract, value, fallback, description, onChange },
) {
   const button = document.createElement("button");
   button.type = "button";
   button.id = `given-${contract.name}`;
   button.className = "select-like";
   button.dataset.given = contract.name;
   // Both answers in one call: the values, and whether this control can
   // represent them. `readFilterForPicker` in format.js says why they have to
   // travel together, and what `opaque` costs if a caller ignores it.
   // Deduped for the same reason the option list is: `?BRAND=Nike, Nike` is a
   // legal filter, and undeduped it captioned "2 selected" over a single ticked
   // box.
   const readSelection = (filter) => {
      const { values, opaque } = readFilterForPicker(filter);
      return { values: [...new Set(values)], opaque };
   };

   let { values: initial, opaque: initiallyOpaque } = readSelection(value);
   let opaque = initiallyOpaque;

   let selected = initial;
   let options = [];
   let panel = null;
   let renderedRows = [];

   /**
    * The rows the panel should be showing: the suggested options, plus any
    * value in force the list does not offer. `keys` is the same list as
    * strings, so `commit` can tell whether a repaint would change anything
    * without touching the DOM. One definition, because a second opinion about
    * which rows belong is exactly how the panel and the button drift apart.
    */
   const panelRows = () => {
      const offered = options.map(String);
      const unlisted = opaque
         ? []
         : selected.filter((v) => !offered.includes(v));
      return { rows: [...options, ...unlisted], keys: [...offered, ...unlisted] };
   };

   const caption = () => {
      const noun = plural((contract.label ?? contract.name).toLowerCase());
      if (opaque) return selected[0] ?? `All ${noun}`;
      if (selected.length === 0) return `All ${noun}`;
      if (selected.length === 1) return selected[0];
      return `${selected.length} selected`;
   };
   const paint = () => {
      button.textContent = caption();
      // Say so rather than silently converting on the next click. Through
      // `composeTitle`, because a failed options query writes to the same
      // attribute and whichever ran last used to erase the other.
      composeTitle(button, opaque ? OPAQUE_HELP : "");
      button.dataset.opaqueFilter = String(opaque);
   };
   paint();

   // One call, not a per-value encode and a join: the encoder is what decides
   // which values a filter can carry, and joining its output here would be this
   // page keeping a second opinion about that.
   const commit = () => {
      const encoded = encodeFilterList(selected);
      // Re-read the selection from what was actually ENCODED, not from what was
      // ticked. The encoder drops a value it cannot carry, so a caption counting
      // the dropped ones, and a box left ticked for one, state a filter that is
      // not in force.
      const ticked = selected;
      // `encoded || fallback`, and `opaque` re-derived rather than assumed:
      // unticking the last box sends `""`, so the server re-applies the declared
      // default and the panel must show that rather than "All brands". A default
      // that is itself hand-written comes back opaque. Measured, with a default
      // of `f'-Nike'` the button returns to `-Nike` with its warning intact.
      ({ values: selected, opaque } = readSelection(encoded || fallback));
      paint();
      // Repaint only when it would change something. `fillPanel` replaces every
      // row, so doing it on each tick destroys the checkbox the reader is
      // standing on: measured, focus fell to `<body>`, which makes ticking a
      // second brand by keyboard impossible without tabbing back through the
      // whole control row, and the panel scrolls, so a tick in its lower half
      // jumped the list back to the top under the cursor.
      //
      // Two ways it changes. The encoder may have dropped a value, so a box is
      // ticked for a filter that is not in force. Or the ROW SET may differ:
      // unticking a value the suggestion list does not offer removes its row,
      // and comparing selections alone missed that, leaving the row on screen.
      const wanted = panelRows().keys;
      const changed =
         ticked.length !== selected.length ||
         selected.some((v, i) => v !== ticked[i]) ||
         wanted.length !== renderedRows.length ||
         wanted.some((k, i) => k !== renderedRows[i]);
      // Mutual with `fillPanel`, whose checkboxes call this: one has to come
      // second, and both only ever run from a click.
      // eslint-disable-next-line no-use-before-define
      if (changed) fillPanel();
      onChange(contract.name, encoded);
   };

   // Not `{ once: true }`: a pointerdown INSIDE the panel (ticking a checkbox)
   // would spend the listener without closing anything, and the popover would
   // then ignore every later click outside it. The listener is removed when the
   // panel closes instead, which is the event it is actually paired with.
   const onOutsidePointerDown = (event) => {
      // `close` and this handler reference each other, so one has to come
      // second. Safe because both are only ever called from an event, long
      // after both exist.
      if (panel && !panel.contains(event.target) && event.target !== button) {
         // eslint-disable-next-line no-use-before-define
         close();
      }
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
      // A value in force that the suggestion list does not offer still needs a
      // row. Without one the panel reads as "nothing selected" while the button
      // says otherwise, and the next tick ORs the unseen value back in: measured,
      // `?BRAND=Ties` with Ties absent from the list sent `Ties, Levi's` from a
      // single click on Levi's. It also made the picker's behaviour turn on
      // something invisible, since `?BRAND=-Ties` replaced while `?BRAND=Ties`
      // extended, from identical-looking panels.
      //
      // These rows are ordinary, not disabled: a plain value IS one this control
      // can encode, it simply is not in the list, so it can be unticked like any
      // other. An opaque filter gets no row, because it has no value to tick.
      const { rows, keys } = panelRows();
      renderedRows = keys;
      for (const option of rows) {
         const row = document.createElement("label");
         row.className = "check-row";
         const box = document.createElement("input");
         box.type = "checkbox";
         // `String(option)`: `selected` comes back from the filter grammar as
         // strings, while `options` holds whatever the column is, so comparing
         // a number column raw ticks nothing while the button and the URL both
         // say the filter is on.
         //
         // Not an invitation to reuse this on a `filter<number>` given, mind.
         // This control encodes through the STRING grammar only, and the two
         // are not interchangeable: measured, `-5` string-encodes to `\-5`,
         // which the number parser rejects outright. Positive integers happen
         // to survive, so a copy works until the first negative value.
         box.checked = !opaque && selected.includes(String(option));
         box.addEventListener("change", () => {
            const value = String(option);
            // Replace, do not extend. While `opaque`, `selected` holds the
            // hand-written filter as one entry, so appending to it would
            // re-encode `-Nike` as a literal and OR it with the pick, which is
            // the exact hazard this branch exists to prevent. Dropping it here
            // is what makes the tooltip's "changing the selection replaces it"
            // true.
            const base = opaque ? [] : selected;
            selected = box.checked
               ? [...base, value]
               : base.filter((v) => v !== value);
            opaque = false;
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
         ({ values: selected, opaque } = readSelection(next || fallback));
         paint();
         // An open panel has to follow too. Back with the popover open
         // otherwise repaints the button and leaves the ticks showing the
         // previous selection, so the panel contradicts its own caption and
         // the URL. Reset escapes this only because its pointerdown closes
         // the panel first.
         fillPanel();
      },
   };
}

function range(host, { contract, value, fallback, description, onChange }) {
   const wrap = document.createElement("span");
   wrap.className = "range";
   const input = document.createElement("input");
   input.type = "range";
   input.id = `given-${contract.name}`;
   input.dataset.given = contract.name;
   input.min = String(contract.rangeMin);
   input.max = String(contract.rangeMax);
   const readout = document.createElement("output");

   // A slider can express one thing, a lower bound, and `filter<number>` can
   // express many. `<= 100`, `not >= 100`, `> 50` and a bare `100` all parse
   // cleanly and are all honoured by the server, and every one of them decoded
   // to 0 here and drew "any": the slider parked at the left over data that IS
   // filtered. A bound past `range_max` is worse, because the input clamps it
   // and the readout then states a number that is not the one in force: `>= 500`
   // drew "≥ $250".
   //
   // Same rule as the two pickers, and the same words: show that something is
   // in force, say the control cannot draw it, and do not pretend otherwise.
   const showFilter = (filter) => {
      const text = String(filter ?? "").trim();
      const bound = decodeAtLeast(filter);
      const unshowable =
         text !== "" &&
         (bound === 0 ||
            bound < contract.rangeMin ||
            bound > contract.rangeMax ||
            // The handle only sits on whole numbers, because the input has no
            // `step`. A browser SNAPS anything else, and the readout is built
            // from where the handle landed, so `>= 75.5` drew "≥ $76" and
            // `>= 0.4` drew "any" over data that is filtered. Worth knowing:
            // happy-dom does not snap, so the suite cannot see this by driving
            // the element. It is arithmetic here on purpose, which is why it
            // can be tested at all.
            !Number.isInteger(bound));
      input.value = String(bound);
      input.dataset.opaqueFilter = String(unshowable);
      composeTitle(input, unshowable ? OPAQUE_HELP : "");
      // From `bound`, NOT from `input.value`. The element clamps what it is
      // given, so with `range_min=10` and nothing in force this wrote 0, read
      // back 10, and stated "≥ $10" over unfiltered data: a number the reader
      // never chose and the server never applied. Reading back a value you
      // just wrote is how the clamp gets laundered into a fact.
      readout.textContent = unshowable ? text : bound > 0 ? `≥ $${bound}` : "any";
   };
   showFilter(value);
   // Dragging clears the unshowable state, because a drag IS a new lower bound.
   input.addEventListener("input", () => {
      input.dataset.opaqueFilter = "false";
      composeTitle(input, "");
      // Here `input.value` IS the truth: the reader just dragged the handle.
      readout.textContent =
         Number(input.value) > 0 ? `≥ $${input.value}` : "any";
   });
   // On `change`, not `input`: one query when the drag ends, not one per pixel.
   // Send, then redraw from what is in force: dragging to the floor sends `""`
   // and the server re-applies the declared default, which is not "any".
   input.addEventListener("change", () => {
      const sent = encodeAtLeast(input.value);
      onChange(contract.name, sent);
      showFilter(sent || fallback);
   });
   wrap.append(input, readout);
   host.appendChild(labelled(contract, wrap, description));
   return {
      name: contract.name,
      set: (next) => {
         showFilter(next || fallback);
      },
   };
}

function date(host, { contract, value, description, onChange }) {
   const input = document.createElement("input");
   input.type = "date";
   input.id = `given-${contract.name}`;
   input.dataset.given = contract.name;
   input.value = value;
   // Repaint after sending, not before. Clearing the box (the browser's own
   // clear affordance, or select-all and delete) fires `change` with `""`, the
   // given is dropped, and the model's own default is then what filters. The
   // box read blank over data with a lower bound: "no bound" is exactly what it
   // was not. `set` already did this, which is why Reset was right and this
   // path was not. Order matters: send `""` so the URL stays clean, then draw
   // what is actually in force.
   input.addEventListener("change", () => {
      onChange(contract.name, input.value);
      input.value = input.value || readGivenDefault(contract.default);
   });
   host.appendChild(labelled(contract, input, description));
   return {
      name: contract.name,
      set: (next) => {
         input.value = next || readGivenDefault(contract.default);
      },
   };
}
