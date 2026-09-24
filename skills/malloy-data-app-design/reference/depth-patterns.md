<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->
# Depth patterns

> The mechanics behind the depth plan in `SKILL.md` §4. Read when building drill-down, cross-filtering, linkable state, or an entity drawer. Each pattern names the bug you get by skipping a part, because every one of these fails quietly rather than loudly.

## Shared state with a subscriber bus

Any app with a control that more than one tile obeys (a period selector, a unit toggle, a cross-filter) needs one place to hold that state and one way to tell tiles it changed. Without it, each tile grows its own copy and they drift out of sync.

```js
const state = { month: null, period: "m", unit: "k" };
const subs = [];
const emit = () => { subs.forEach((fn) => fn()); syncControls(); writeHash(); };

// Each tile registers its own redraw and never reads another tile's state.
subs.push(() => redrawRevenue());
```

Three rules keep this from turning into a tangle:

- **Tiles subscribe; they do not push.** A tile reads `state` and redraws. Only a control calls `emit()`.
- **`emit()` also writes the URL.** State that is not in the URL is state a user cannot share (see below).
- **A tile that is expensive to redraw should check whether its slice of state actually changed.** A unit toggle does not need a refetch, only a reformat, and refetching on every keystroke of a filter is how an app becomes slow.

## Linkable state

Put page, period, and filters in the URL hash and read them on load. This is the single cheapest thing that gets an app adopted: a view someone can paste into Slack beats one the recipient has to re-navigate.

```js
// #pnl?m=2026-06&p=q&u=eur
function writeHash() {
  if (!state.month) return;
  const p = new URLSearchParams({ m: state.month });
  if (state.period !== "m") p.set("p", state.period);   // omit defaults: shorter, and
  if (state.unit === "eur") p.set("u", "eur");          // the default can change later
  history.replaceState(null, "", `#${currentPage}?${p}`);
}
```

Use `replaceState`, not `pushState`, for filter changes: every tweak of a dropdown becoming a back-button step makes the back button useless. Reserve `pushState` for navigation the user would expect to undo, like opening a different page.

**Validate on read.** A hash is user-editable, so treat it as untrusted input: check a page id against the known list and a period against the known set, and fall back to the default rather than trusting it into a query.

## Drill-down: the model side

A drill target is a **parameterised source that extends the spine**, so every inherited measure and view comes back already filtered to that entity. The drill panel then names a *view* and recomputes nothing.

```malloy
##! experimental.parameters

source: product_detail(pid::number is 0) is products extend {
  where: product_id = pid
  view: profile is { ... }
}
```

The flag is per-file and required: without it the parameter list is a parse error, and a `.malloy` file that fails to parse fails the **whole package load**, so every tile goes blank rather than just the drill.

Two traps worth knowing before you build several of these:

- **A persisted rollup is still read through a wrapper.** Extending or wrapping a persisted source keeps the persist annotation and still reads the built table, so a parameterised detail source over a rollup does not silently recompute it. The pattern is safe, and a slow drill is almost never the wrapper: look first at a missing filter or a grain that is not actually aggregated.
- **An inherited view can be meaningless under a parameter.** A source scoped to one entity inherits views that only make sense across many: a sentiment split on a single-sentiment theme collapses to 0% or 100%, a share-of-total becomes 100%. Declare purpose-built views on the detail source for those, and say in the source's doc comment which inherited views not to use.

### When you cannot change the model

Often you can't: the model is owned by another team, synced from somewhere else, or the brief scopes you to the app. The parameterised source is still the right answer to *propose* - say so, because it is a small model change that removes a whole class of drift - but you may have to ship without it.

Falling back to query strings built in the app is legitimate. What makes it safe is keeping the drill queries under the same discipline as the tile queries, not treating them as one-off strings:

- **Put them in the same module** as the tile queries, next to the tile they drill from. A drill query that lives in the render code is the drift the pattern warns about.
- **Share one filter builder and one definition of every derived measure.** If a rate is pooled in a tile, the same helper must pool it in the drawer. Two spellings of the same measure is how a drawer quietly disagrees with the row that opened it.
- **Assert it.** A unit test over the query strings - every one filters to the entity, every rate uses the shared helper - costs a few lines and catches the drift that code review will not.

Write down that you did this and why. A reader who later sees hand-built drill queries should find the reason, not assume the pattern was unknown.

## Drill-down: the panel side

An entity drawer has five parts. Skipping any one produces a bug that only shows up in use, not in a screenshot.

```js
const drill = { entity: null, charts: [], returnFocus: null };

function openDrill(entity) {
  drill.entity = entity;
  drill.returnFocus = document.activeElement;   // 1. remember where focus came from
  panel.hidden = false;
  requestAnimationFrame(() => {
    panel.classList.add("is-open");
    document.addEventListener("keydown", onTab);  // 2. trap Tab inside the panel
    renderDrill();
  });
}

function renderDrill() {
  const entity = drill.entity;
  loadRows(entity).then((rows) => {
    if (drill.entity !== entity) return;   // 3. stale-response guard
    paint(rows);
  });
}

function closeDrill() {
  drill.entity = null;
  panel.classList.remove("is-open");
  document.removeEventListener("keydown", onTab);
  drill.charts.forEach((c) => c.destroy());     // 4. tear charts down
  drill.charts = [];
  drill.returnFocus?.focus();                   // 5. give focus back
}
```

What each part prevents:

1. **Return focus** - without it, closing the drawer drops a keyboard user at the top of the document, losing their place in a long table.
2. **Focus trap** - without it, Tab walks out of the open drawer into the page behind it, which is invisible to a mouse user and completely breaks a keyboard one. Close on Escape too.
3. **Stale-response guard** - the real bug. Click entity A, then quickly click entity B: A's slower query resolves last and paints A's numbers under B's title. It is intermittent, looks like bad data rather than a race, and is close to impossible to diagnose from a screenshot. Compare the entity you are about to paint against the one currently open, and drop the response if they differ.
4. **Chart teardown** - most chart libraries keep a registry and attach resize listeners; reopening a drawer twenty times without destroying leaks memory and can leave ghost tooltips from the old instance.
5. Also re-render the drawer when the global filter changes underneath it, or it silently shows numbers for a period the rest of the page has moved off.

## Cross-filtering (shared scope)

Clicking a category filters the other tiles. Express scope as a `where:` refinement on the **same named view** each tile already runs, so a filtered tile and an unfiltered one still share one definition.

```js
function scopeWhere(ignore) {
  const parts = Object.entries(SCOPE)
    .filter(([k]) => !ignore?.includes(k))
    .map(([k, v]) => `${k} = ${lit(v.value)}`);
  return parts.length ? ` + { where: ${parts.join(" and ")} }` : "";
}
```

Three requirements, in order of how badly they bite:

- **Every active filter is visible and removable.** A chip per filter, with an X, and a clear-all. A filtered dashboard with no visible chips silently lies to the next person who looks at it - they read filtered numbers as totals. This is the one non-negotiable part of cross-filtering.
- **A tile ignores its own dimension.** The tile you clicked to set `category = X` should not then filter itself to X, or it collapses to a single bar. That is what the `ignore` argument is for.
- **Escape interpolated values correctly, or avoid interpolating.** Malloy escapes with a backslash, not by doubling quotes - `''` closes the string and opens a new one, so a value like `Land O'Lakes` produces a malformed query. Prefer binding the value as a typed runtime parameter (a `given:`, declared behind `##! experimental.givens`, and passed by the runtime rather than pasted into the string) over building query text at all; where you must interpolate, constrain values to a known set and backslash-escape quotes and backslashes.

## Comparison

Collect entities into a tray, then compare them side by side. Back it with a source parameterised on the **pair**, returning one row per entity, so both sides use identical definitions rather than two separately-written queries that might not agree.

```malloy
##! experimental.parameters

source: head_to_head(a::number is 0, b::number is 0) is products extend {
  where: product_id = a or product_id = b
  view: scorecard is { group_by: product_id, ... }
}
```

Persist the tray across navigation (session storage is enough), or a user who found three interesting entities on three different pages cannot actually compare them - which is the whole point of collecting.

## Provenance

Let a tile show the query behind its number, pulled from the tile's own definition rather than retyped. It costs one chip per tile and it is what makes a number defensible to someone who did not build the app. It also pays off when handing context to an agent: the exact query string the tile ran is far more useful than the name of the source it came from.
