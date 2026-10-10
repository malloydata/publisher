<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The Publisher Console

> What this is: a tour of the **Publisher Console**, the server's built-in web UI — how the core
> constructs (environments, packages, models, sources, views, notebooks, data apps) surface in it,
> and how to navigate them. Zero code required. It's served at **http://localhost:4000** whenever
> the server is running.

The Console is the default, no-code way to explore what a Publisher deployment serves. It's built
from the [SDK](embedded-data-apps.md) but you don't need to know that — just open it and browse.
(The Console is Publisher's own UI. It is not an [HTML data app](html-data-apps.md), which is a
custom page _you_ author inside a package; see [choosing-a-surface.md](choosing-a-surface.md) for
how the three in-package surfaces relate.)

## The resource hierarchy

Everything in Publisher nests the same way, and the Console mirrors it:

```
Environment            e.g. "examples"
└── Package            e.g. "storefront"  (a versioned bundle of models + data)
    ├── Model          a .malloy file: sources, views, measures, dimensions
    │   ├── Source     a queryable entity (a table or a join graph)
    │   └── View       a saved, reusable query on a source
    ├── Notebook       a notebooks/*.malloy file (a legacy .malloynb is read, not authored): a one-column layout of text and query tiles
    ├── Dashboard      a dashboards/*.malloy file: filter controls + a tiled grid
    └── Data Apps      an in-package HTML data app (the package's public/ dir)
```

The [REST and MCP APIs](api-overview.md) expose this exact hierarchy; the Console is a view onto it.

## Navigating

- **Left sidebar** — **Home**, then an **Environments** list. Pick an environment to see its
  packages; pick a package to see its models, notebooks, dashboards, and data apps.
- **Breadcrumbs** across the top track where you are: `environment › package › file`. They are the
  way back up; pages carry no separate "Back to" link.
- **Sidebar footer** holds the things about the Console rather than the data in it: **Theme** (the
  visualization theme editor), the light/dark **mode toggle** when the deployment allows it (see
  [theming.md](theming.md)), and links to the Malloy docs, these Publisher docs, and the live
  **Publisher API** explorer (see [api-overview.md](api-overview.md)).

![The Publisher Console showing the storefront package under the examples environment](screenshots/console.png)

## Two URL shapes

You'll see two path styles, and they're not interchangeable:

- **Console routes** are short — `/{environment}/{package}/{file}`, e.g.
  `http://localhost:4000/examples/storefront/storefront.malloy`. Use these to link to something
  inside the Console (a model, a dashboard, a package).
- **Resource paths** are fully qualified — `/environments/{environment}/packages/{package}/...`.
  This is the canonical form the [REST and MCP APIs](api-overview.md) use, and it's also how an
  in-package HTML data app is served, e.g.
  `http://localhost:4000/environments/examples/packages/html-data-app/`.

When in doubt, the fully-qualified `/environments/.../packages/...` form always works; the short form
is a Console convenience.

## What you can do in the Console

- **Browse a package** — one section each for **Artifacts**, **Data Apps**, **Semantic Models**,
  **Package Data** and **Materializations**, in that order, plus the package's `README.malloynb`
  rendered underneath. Artifacts lists dashboards and notebooks together, each row tagged with its
  kind, and carries the **New** button on its heading row. Data Apps is hidden when the package has
  none. Artifacts is hidden when empty too, unless creating is offered: then it shows a "No
  artifacts yet" row with a **New artifact** button. Every kind has its own icon and its own color,
  so a row's type reads before its name does.
  The Materializations section lists the package's build runs and carries the three controls that
  change them: **Scope**, **Schedule** and **Add materialization**.
  An artifact is listed by its title, or by its file name without folder or extension when it has
  none; the folder path appears beside it only to tell apart two files with the same name, and a
  `.malloynb` keeps its path. A notebook's title comes from its opening markdown heading unless a
  `## title="…"` or a `#" ` doc comment overrides it.
- **Build a dashboard by dragging** — every dashboard page has an **Edit** button in the header,
  beside the breadcrumbs, that turns it into a grid you rearrange directly: drag a tile's card to
  move it, drag its right edge to set its width (it snaps to whole columns), and set its chart and
  drill from its **⋯** menu; add filters with **+ Filter** on the filter row under the description.
  Titles, descriptions and text tiles are edited where they are shown: click one (a small pencil
  follows the text) and type. A **text tile** holds markdown (a heading, a paragraph, a list) and is
  added from the same dialog as a query tile. The classic dashboard-building feel, over a file you
  can still read and review. **Save** writes the `dashboards/*.malloy` back into the package and the
  builder stays open; in the builder the header button reads **View** and returns to the read-only
  page, asking first if edits are unsaved
  ([dashboards.md](dashboards.md#editing-in-the-console)).
- **Create a dashboard or notebook** — the package page's **New** menu takes a type (Dashboard or Notebook), a
  model, a source and its view (one select), and a title, writes the file into the package (it never overwrites an existing one) and opens it in
  its editor. A host with its own record creates it there instead. It is offered when the server takes
  writes (the file goes into the package) or when a host keeps the record and can store; a workspace
  that keeps drafts in the browser beside a writable server still gets it, and writes to the package.
  It is not offered when neither route exists, on a record that cannot store, or on a pinned version
  of a package. An empty Dashboards or Notebooks section carries its own **New dashboard** or
  **New notebook** that opens the same window on that kind. Below 600px wide the Console hides
  **Edit** and the **New** menu, and an editor opened by URL asks for **Edit anyway** first.
- **Edit a notebook** — a notebook is a one-column dashboard, and it opens in the same builder as a
  dashboard (a tagged `notebooks/*.malloy`; a legacy `.malloynb` is read-only). Its prose is text
  tiles and its queries are query tiles, in file order. Everything is click-to-edit: click the
  title, the description, a text tile or a tile's heading and type. In a one-line field Enter keeps
  the edit and Escape puts the old text back; in a text tile, Escape, **Done** or Cmd/Ctrl+Enter
  keeps it, **Cancel** drops it, and Cmd/Ctrl+S saves from inside the field. Use the **+** on a
  tile to insert a tile between two tiles, or **Add tile** at the end; each query tile has
  a **Viz type** picker listing all eight choices (From the view, Table, Line, Bar, Big value,
  Scatter, Shape map and Segment map). A choice the view cannot render stays in the list, greyed,
  with its reason beside it: Big value needs a view with only totals (no group by), and a map needs
  a view that already carries a map chart. The picker is disabled, with the reason, for a tile whose
  chart line the builder does not model (such as `# bar_chart { size=spark }`), and that line is left
  alone. Tiles are dragged into a new order.
  A document stays the kind it was created as. Adding a tile offers every source the package
  publishes; when the file cannot already see the chosen source, the builder adds a named import
  for it (into that model's existing `import { … }` line when there is one).
  A cell-format notebook (the older shape, with `run:` and `(markdown)` cells) opens already
  converted to this layout and unsaved. Nothing is written until you save, and the first **Save**
  asks before it rewrites the file in the tile layout: the builder cannot take that back, though
  the file's history in your repository can. **Cancel** writes nothing.
  **Save** (or Cmd/Ctrl+S) writes at once, with no review step, and the builder stays open; the
  button reads **Saved**, greyed, until the next edit, and its tooltip says where it writes (the
  package file, this browser, or where the host app keeps it). **View** in the header leaves, and
  asks first when edits are unsaved. On a server that does not take writes there is no Save. A save whose text
  declares a real `#(authorize)` or `#(access_filter)` gate outside prose is refused with a 400, so
  the builder opens such a file but cannot save it from the Console; gates live in the model file.
- **Explore, no code** — open a source in the [Explorer](explorer.md), the visual query builder;
  every action generates valid Malloy, and you can view the Malloy and SQL behind any result.
- **Read a notebook** — a `.malloynb` in a package renders its markdown and runs its query cells
  inline, including `# dashboard` views (KPI tiles + nested charts). The format is deprecated and
  the bundled examples no longer ship one, but the viewer stays for packages that have them.

![The storefront business-overview dashboard rendered inline in a notebook](screenshots/storefront-dashboard.png)

- **Tune parameters live** — when a model declares [givens](givens.md), a dashboard over it shows a
  control row and a notebook a **Filters panel**; change a control and every tile or cell re-runs.
  Try `http://localhost:4000/examples/governed-analytics`.

![A notebook's Parameters panel, generated automatically from the model's givens](screenshots/givens-parameters-panel.png)

- **Open a data app** — a package's [HTML data apps](html-data-apps.md) render inside the Console
  (and can be opened standalone). Try
  `http://localhost:4000/environments/examples/packages/html-data-app/`.
- **Edit the theme** — operators can iterate colors/fonts live at `/settings/theme` (see
  [theming.md](theming.md)).

## Where to go next

- Discover data with an AI agent instead of clicking: [ai-agents.md](ai-agents.md).
- Build a custom UI: [html-data-apps.md](html-data-apps.md).
- Drive it programmatically: [api-overview.md](api-overview.md).
