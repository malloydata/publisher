<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The App Frame Template

> For an app with views, filters or an entity to look up, start from this skill's `assets/data-app/`: a page, a frame
> module, a query and address module, a stylesheet, the d3 chart module and two check scripts. They come from apps
> built and shipped with this skill. Copy them, keep the frame, and write the views for your app. The example views
> they ship with (NYC rodent complaints by ZIP code) show the shape; replace them.

## What is in `assets/data-app/`

| File | What it is | Copy into `public/` |
|---|---|---|
| `index.html` | The page: header with name, views and search; scope bar; main; footer | Once, then it is the app's own |
| `app.js` | The frame above `// ---- Views`, then example views (marked ADAPT) | Once, then it is the app's own |
| `scope.js` | Every query the app runs and its address; no DOM | Once, then it is the app's own |
| `styles.css` | The design system: tokens, frame, cards, states, phone layout, looks | Once, then it is the app's own |
| `charts.js`, `charts.css` | The d3 chart module (`reference/charts.md`) | Whole; never edit, copy again to take a fix |
| `themes.css` | Ten colour themes, each light and dark (below) | Whole; never edit, copy again to take new themes |
| `vendor/d3.min.js`, `vendor/LICENSE-d3.txt` | d3 v7.9.0 and its ISC license | Whole |
| `vendor/topojson-client.min.js`, `vendor/LICENSE-topojson-client.txt` | topojson-client 3.1.0 and its ISC license, for boundary maps only | Only with a map (`reference/maps.md`) |
| `palette.py` | Checks the colour tokens (below) | Never: run it from here |
| `themes.py` | Writes and checks `themes.css`; where a theme is added or changed | Never: run it from here |
| `fonts.py` | Writes `fonts.css`, the faces as base64 text (below) | Never: run it from here |

Run the copy from the package's own directory, the one holding `publisher.json`. The first line finds where the
skill is installed: in this directory or one above it (a workspace's `.claude/skills` or `.agents/skills`), else in
your home directory. A shell variable does not outlive its command, so every snippet below that uses `$T` starts
with the same line. A file already in `public/` is the app's own and stays:

```sh
T=$(d=$PWD; while :; do for s in "$d/.claude/skills" "$d/.agents/skills"; do [ -d "$s/malloy-html-data-apps/assets/data-app" ] && { echo "$s/malloy-html-data-apps/assets/data-app"; exit; }; done; [ "$d" = / ] && break; d=$(dirname "$d"); done; for s in "$HOME/.claude/skills" "$HOME/.agents/skills"; do [ -d "$s/malloy-html-data-apps/assets/data-app" ] && { echo "$s/malloy-html-data-apps/assets/data-app"; exit; }; done)
test -f publisher.json || { echo "run this from the package directory, the one with publisher.json"; exit 1; }
test -n "$T" || { echo "assets/data-app is not on disk: build from the minimal scaffold in SKILL.md"; exit 1; }
mkdir -p public/vendor
for f in index.html app.js scope.js styles.css; do [ -e "public/$f" ] || cp "$T/$f" public/; done
cp "$T/charts.js" "$T/charts.css" "$T/themes.css" public/ && cp "$T/vendor/d3.min.js" "$T/vendor/LICENSE-d3.txt" public/vendor/
chmod -R u+w public
```

When the skill reached you only as text (MCP prompts), `assets/` is not on disk: say so, and build from the minimal
scaffold in `SKILL.md` and `reference/runtime.md`.

## The model the frame needs

An app is only as deep as its model. Before writing the views:

- **Record grain.** The main source is one row per record, with a `primary_key` when the data has one. Lookup
  tables are joined and decoded into readable dimensions.
- **Every number from the model.** Rates are measures whose doc names the numerator and denominator; subsets are
  filtered measures (`count() { where: ... }`), never arithmetic in JavaScript.
- **The headline question is a source.** When the question has a scope of its own (passenger cars, one state), it
  is a source that extends the main one with that `where:`, exported first. Its `kpis` view is the app's headline,
  run bare, so the page, an agent and an analyst asking the model get the same numbers. `scope.js` names it
  `HEADLINE`.
- **A date in one place.** A date the story turns on (a policy's end) is written once, in the dimension that splits
  the periods. A story around a date compares like for like: each month beside the same month a year earlier
  (`calculate: prior is lag(m, 12)`).
- **Thresholds carry their evidence.** A minimum count for a change between groups is a documented field, and a
  card that applies it says how many groups it left out.
- **One view per card**, named for what it shows; a `kpis` view with every headline measure; a `records` view of
  the fields a reader would check, newest first.
- **Codes are strings.** ZIP codes and FIPS codes are string dimensions, because the address holds strings.

Query every view and look at the rows before building on them.

## Plan from what the data shows

The job and the question come first (`reference/design.md`). Then look for what stands out, with queries: the
biggest movers, a break in a trend, a share that flipped, an entity far from its peers. Check each against its
denominator and threshold; a fall from five to two is not a finding. Write the plan down before building: the lead
finding with its number; each card's finding, two or three forms that could show it and the one picked; the one
chart form native to the field (a rose of slope aspect, an hour-by-weekday grid, an epidemic curve) and why; and
what was left out. A card whose finding turns out weak is cut, not padded.

## The views

Each view has its own address, so every view and filter can be shared and Back works:

- **Overview** (`#/`): the question, the answer in a sentence or two built from the rows, headline numbers from
  bare views of the headline source, the chart for the lead finding, and links into the app.
- **Explore** (`#/explore`): the scope bar, then sections named for what the plan found, six to ten cards. Every
  card with a category or time axis filters the app on click; a second click clears it. A click filters in place;
  only a name drawn as a link opens a page.
- **An entity's page** (`#/<entity>/<id>`): its numbers against the whole, its trend, its latest records. Reached
  from search, every ranked list and the records.
- **Compare** (`#/compare?ids=a,b`): up to four entities side by side.
- **Records** (`#/records`): the rows in scope, 50 more at a time.
- **About the data** (`#/about`): sources and licenses with attribution, what was reshaped, caveats, the date of the
  latest record, read from a query and never typed.

This is the Explorer archetype (`reference/archetypes.md`) built as views at their own addresses rather than
drawers, which is what works in a frame as tall as its content (`reference/embedding.md`).

## What `app.js` holds

Everything above `// ---- Views` is the frame. Keep it as it is:

- **Queries**: `query()` sends one request per distinct text, at most four in flight, cached for the session, and
  evicts a failed one.
- **The address**: `go()`, `setScope()` and `toggleScope()`. A view change pushes history; a filter replaces it.
- **Compare**: `picked`, `inCompare()`, `toggleCompare()` and `goCompare()`. The header's Compare link carries the
  picks and counts them ("Compare · 2"). Every ranked list of entities passes `inCompare` and
  `onCompare: toggleCompare`, so its + adds there. Never declare these names again in the views: the module stops
  with a SyntaxError and the page is blank. An app with nothing to compare drops the link, the +, "Add to compare"
  and `compare` from `VIEWS` in `scope.js`, and keeps the `compare()` function, which the frame names.
- **`tile()`**: a card that loads as it nears the screen, with loading, empty and error states, a finding sentence,
  a method note and its working (each query, what it counts, its rows and a CSV of them). Its loading state keeps
  240 px; pass `reserve` with the chart's height for a card that is shorter or taller. It does not remember settled heights
  between visits, which `reference/performance.md` describes. When the scope changes on the
  same view, every card is drawn again in place: the chart stays, dimmed, until the rows arrive, then marks move
  and numbers count.
- **`paint()` and `keepInView()`**: a new scope never takes a card out of the page, and the card the reader clicked
  keeps its place. That holds standalone; in a content-height frame the host scrolls and `scrollBy` does nothing,
  so the page may move. A part of a view that follows the address but is not a card, such as Compare's picks, is marked
  `data-redraw="<name>"` and swapped in place.
- **`body[data-ready="true"]`**: set once every card on the view has drawn or failed, and nothing marked
  `data-pending` is waiting. A line drawn from a query outside a card, like the Overview's answer, carries
  `data-pending` until it is drawn. A screenshot waits on it.
- **`selfCheck()`**: open the app at `?check=1#/` and a banner says whether every number in the answer and the
  headline card is a value bare views of the model return, and that no placeholder (`undefined`, `NaN`) is on the
  page.
- **d3** loads beside `boot()`'s first queries rather than ahead of them; nothing draws before it is in.

Below it, `boot()` (its distinct-value lists and the "Data through" line are marked ADAPT) and the views. The
example's Overview, Explore, entity page and Compare are worked; its Records and About are headings to fill.

## `scope.js`: queries and the address

It builds every query and the address and holds no DOM, so test it with `node` before any page exists: import it
with a sample `known`, parse a few hand-written addresses (one with a quote in a value, one with an unknown id), and
run two or three of the strings it builds against the model.

- `known` holds what distinct-value queries returned at load: each pick key's values, the `[min, max]` of the
  period, the entity's ids. Nothing from the address or a click reaches a query unless it is in `known`. Drop nulls
  from those lists: a null becomes a blank option that matches "nothing selected".
- The address is the route, then the scope: `#/explore?borough=BROOKLYN&from=2023`, `#/zip/10035?from=2023`.
  Defaults are left out of it.
- A scope key on the entity's own field (a `state` pick when the entity is a state) goes in `ENTITY_KEYS`. The
  entity's page and Compare choose the entity themselves, so they leave it out, through `S.skipFor(route.view)`.

## Look and colour

`styles.css` is one frame with nine choices, each an attribute on `<body>`; with none, a choice takes its first
option. Choose a look from the data and its reader, then change one or two choices so the app is its own.

| Attribute | Options | What changes |
|---|---|---|
| `data-masthead` | `band`, `light`, `bar` | A dark band; a light masthead; one slim light row |
| `data-face` | `sans`, `serif` | The face of titles, section heads and headline numbers |
| `data-filters` | `selects`, `sentence` | Labelled selects, or a sentence ("Showing … in …") |
| `data-numbers` | `panel`, `rules` | Headline numbers on a tinted panel, or between rules |
| `data-sections` | `plain`, `numbered` | Section heads plain, or numbered under a heavy rule |
| `data-cards` | `caps`, `headline` | Card titles in small capitals, or a headline |
| `data-buttons` | `pills`, `links`, `square` | Buttons and selects |
| `data-footer` | `panel`, `colophon` | The footer |
| `data-lede` | `plain`, `dropcap` | The lede |

Three starting looks: **Band** (no attributes); **Masthead** (`data-masthead="light" data-face="serif"
data-filters="sentence" data-numbers="rules" data-sections="numbered" data-cards="headline" data-buttons="links"
data-footer="colophon" data-lede="dropcap"`), for a subject read slowly; **Ledger** (`data-masthead="bar"
data-numbers="rules" data-sections="numbered" data-buttons="square"`), for numbers read side by side.

Colour comes from a theme, chosen from the subject. `themes.css` holds ten, each a complete palette for a light
page and a dark one: the page tone, the accent, six categories, the ramp, and good and bad news. Every one passes
the palette check in both modes. Set it on `<html>`, `<html data-theme="lagoon">`.

| Theme | Page | Accent | Suits |
|---|---|---|---|
| `ember` | warm paper | rust | heat, fire, energy, prices, the cost of living |
| `harbor` | cool grey | navy | water, shipping, weather, cities, transport |
| `meadow` | warm paper | forest green | nature, farming, food, birds, land, health |
| `dusk` | cool grey | indigo | space, night, sleep, technology, science, culture |
| `coral` | white | coral, with teal | oceans, beaches, tourism, people and places |
| `ink` | white | newspaper red | politics, elections, courts, news, history |
| `desert` | sand | gold | drought, mining, the West, dry land |
| `orchard` | warm paper | plum | wine, fruit, fashion, the arts, retail |
| `lagoon` | cool grey | teal | climate, coasts, public health, ecology |
| `graphite` | cool grey | electric blue | technology, finance, markets, infrastructure |

- **Choose from the subject, not from habit.** Where several apps are seen side by side, as in a gallery, don't
  repeat the theme of the apps before it unless the subject calls for that theme alone.
- **Light and dark.** With no `data-mode`, the reader's system setting decides; `data-mode="dark"` or `"light"`
  fixes it, and `?mode=dark` in the address previews it, as `?theme=` previews another theme. The frame draws every
  chart again when either changes, since charts read their colours when they draw. Shoot the app in both modes.
- **Meanings are the app's; colours are the theme's.** List what each category means in a comment at the top of
  `styles.css` (`/* --c1 as paid, --c2 in today's dollars, --c6 a year ago */`) and keep it the same on every view.
  Only `--c1` may be the accent. Fewer categories are better.
- **The tokens:** `--accent` for the lead number, links and the selection; `--c1` to `--c6` for categories;
  `--c-bar` for a count with no category; `--seq-0` to `--seq-5` for the ramp, light to dark on a light page and dark
  to light on a dark one; `--up` and `--down` for bad news and good news, not up and down (a falling price is good for
  a buyer; keep a change neutral when its meaning is unclear).
- **Overriding a colour** is for a subject with its own (a party, a client's brand): set the token on
  `:root[data-theme="<theme>"]` and on `:root[data-theme="<theme>"][data-mode="dark"]` in `styles.css`, never in
  `themes.css`, and check both modes. `themes.css` is written by `themes.py`; a new or changed theme goes there.

The app is not the data's publisher: never use their logo, their name as a brand, their palette or their fonts.
Credit them as a source in About. A client's own identity is different (`reference/token-contract.md`).

Check the colours in both modes after any override, until each prints `palette ok`:

```sh
T=$(d=$PWD; while :; do for s in "$d/.claude/skills" "$d/.agents/skills"; do [ -d "$s/malloy-html-data-apps/assets/data-app" ] && { echo "$s/malloy-html-data-apps/assets/data-app"; exit; }; done; [ "$d" = / ] && break; d=$(dirname "$d"); done; for s in "$HOME/.claude/skills" "$HOME/.agents/skills"; do [ -d "$s/malloy-html-data-apps/assets/data-app" ] && { echo "$s/malloy-html-data-apps/assets/data-app"; exit; }; done)
python3 -I "$T/palette.py" public/styles.css public/themes.css --theme lagoon --mode light
python3 -I "$T/palette.py" public/styles.css public/themes.css --theme lagoon --mode dark
```

It checks text contrast on the page and panel, marks at 3:1, categories far enough apart in colour, `--up` and
`--down` apart in lightness, and a ramp that steps evenly in the mode's direction; each failure exits 1. Categories
too close under red-green colour blindness are a note, not a failure, since no six colours separate fully for every
reader: label those two directly wherever both appear.

## Fonts

A sans for everything that is not prose, a serif for ledes, findings and method notes. The faces ship inside
`public/fonts.css` as base64 text, so the app needs no font host and the package holds only text files. Every face
is under the SIL Open Font License, which travels with it:

```sh
T=$(d=$PWD; while :; do for s in "$d/.claude/skills" "$d/.agents/skills"; do [ -d "$s/malloy-html-data-apps/assets/data-app" ] && { echo "$s/malloy-html-data-apps/assets/data-app"; exit; }; done; [ "$d" = / ] && break; d=$(dirname "$d"); done; for s in "$HOME/.claude/skills" "$HOME/.agents/skills"; do [ -d "$s/malloy-html-data-apps/assets/data-app" ] && { echo "$s/malloy-html-data-apps/assets/data-app"; exit; }; done)
python3 -I "$T/fonts.py" public figtree source-serif-4   # add inter or fraunces when the look uses one
```

It fetches each pinned file, checks its sum, and writes `public/fonts.css` and the licenses to
`public/vendor/fonts/`. Load `fonts.css` before `styles.css`. The stacks fall back to system faces, so if the font
host does not answer, ship without `fonts.css`, remove its `<link>` from `index.html` so the page does not ask for a missing file, and say so; never keep a file whose sum differs.

## Before you build on it

- Load each view from its address alone: `#/`, `#/explore`, an entity's page, `#/about`. Each shows data and the
  console has no errors.
- Press the + on a ranked list: the Compare link counts it and opens Compare with it.
- Change a filter on Explore, open an entity, go back: the filter is still set and the address says so.
- After editing a module, check it as a module: `node --input-type=module --check < public/app.js`. A plain
  `node --check` on a `.js` file misses a name declared twice.

## Before you call it done

On top of `SKILL.md`'s recipe:

- `?check=1#/` shows the passing banner.
- The fonts loaded: `[...document.fonts].some((f) => f.family === 'Source Serif 4 Variable' && f.status === 'loaded')`
  is true. `document.fonts.check` is true even when the face is missing.
- Shoot every view, Explore with each scope control set, Explore after a click, and an entity with a scope, at 1280
  and 390 px wide, in light mode and again with `?mode=dark`. Cards load as they near the screen, so shoot with a viewport taller than the view and wait for
  `body[data-ready="true"]`, which the frame sets once every card on the view has drawn or failed
  (`reference/verification-harness.md`). No label overlaps or runs off its chart.
- Every card has a finding and a method note, and its working opens with a definition for every query.
- Every scope control changes every card that should respond; a card that ignores a key says so.
- A nav link opened from low on a view lands at the top of the next view.
- Search finds an entity by part of its name and opens its page; Back returns to where the reader was.
