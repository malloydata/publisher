# The Publisher Console

> What this is: a tour of the **Publisher Console**, the server's built-in web UI — how the core
> constructs (environments, packages, models, sources, views, notebooks, data apps) surface in it,
> and how to navigate them. Zero code required. It's served at **http://localhost:4000** whenever
> the server is running.

The Console is the default, no-code way to explore what a Publisher deployment serves. It's built
from the [SDK](react-data-apps.md) but you don't need to know that — just open it and browse.
(The Console is Publisher's own UI; it is not an [HTML data app](html-data-apps.md), which is a
custom page _you_ author inside a package — see
[choosing-a-surface.md](choosing-a-surface.md) for how the surfaces relate.)

## The resource hierarchy

Everything in Publisher nests the same way, and the Console mirrors it:

```
Environment            e.g. "examples"
└── Package            e.g. "storefront"  (a versioned bundle of models + data)
    ├── Model          a .malloy file: sources, views, measures, dimensions
    │   ├── Source     a queryable entity (a table or a join graph)
    │   └── View       a saved, reusable query on a source
    ├── Notebook       a .malloynb file: markdown + live query cells
    ├── Dashboard      a dashboards/*.malloy file: filter controls + a tiled grid
    └── Data Apps      an in-package HTML data app (the package's public/ dir)
```

The [REST and MCP APIs](api-overview.md) expose this exact hierarchy; the Console is a view onto it.

## Navigating

- **Left sidebar** — **Home**, then an **Environments** list, and a **Settings** section
  (Visualization theme). Pick an environment to see its packages; pick a package to see its models,
  notebooks, dashboards, and data apps.
- **Breadcrumbs** across the top track where you are: `environment › package › file`.
- **Theme toggle** (top-right) switches light/dark when the deployment allows it (see
  [theming.md](theming.md)).
- **Footer links** jump to the Malloy docs, these Publisher docs, and the live **Publisher API**
  explorer (see [api-overview.md](api-overview.md)).

![The Publisher Console showing the storefront package under the examples environment](screenshots/console.png)

## Two URL shapes

You'll see two path styles, and they're not interchangeable:

- **Console routes** are short — `/{environment}/{package}/{file}`, e.g.
  `http://localhost:4000/examples/storefront/storefront.malloynb`. Use these to link to something
  inside the Console (a notebook, a package).
- **Resource paths** are fully qualified — `/environments/{environment}/packages/{package}/...`.
  This is the canonical form the [REST and MCP APIs](api-overview.md) use, and it's also how an
  in-package HTML data app is served, e.g.
  `http://localhost:4000/environments/examples/packages/storefront/`.

When in doubt, the fully-qualified `/environments/.../packages/...` form always works; the short form
is a Console convenience.

## What you can do in the Console

- **Browse a package** — its dashboards, notebooks, data apps, models, data files,
  materializations, and README, one section each, at a glance. Every kind has its own icon and its
  own color, so a row's type reads before its name does. Notebooks and dashboards are listed by
  title with the filename beside them; a notebook's title comes from its opening markdown heading
  unless a `## title="…"` or a `#" ` doc comment overrides it.
- **Explore, no code** — open a source in the [Explorer](explorer.md), the visual query builder;
  every action generates valid Malloy, and you can view the Malloy and SQL behind any result.
- **Read a notebook** — a `.malloynb` renders its markdown and runs its query cells inline, including
  `# dashboard` views (KPI tiles + nested charts). Try
  `http://localhost:4000/examples/storefront/storefront.malloynb`.

![The storefront business-overview dashboard rendered inline in a notebook](screenshots/storefront-dashboard.png)

- **Tune parameters live** — when a notebook imports [givens](givens.md), it shows a **Parameters
  panel** above the cells; change a control and every cell re-runs, with the values in the URL so a
  filtered read is a link. The storefront notebook above has one, and
  `http://localhost:4000/examples/governed-analytics/orders.malloynb` is the smaller example.

![A notebook's Parameters panel, generated automatically from the model's givens](screenshots/givens-parameters-panel.png)

- **Open a data app** — a package's [HTML data apps](html-data-apps.md) render inside the Console (and
  can be opened standalone). Try `http://localhost:4000/environments/examples/packages/storefront/`.
- **Edit the theme** — operators can iterate colors/fonts live at `/settings/theme` (see
  [theming.md](theming.md)).

## Where to go next

- Discover data with an AI agent instead of clicking: [ai-agents.md](ai-agents.md).
- Build a custom UI: [html-data-apps.md](html-data-apps.md).
- Drive it programmatically: [api-overview.md](api-overview.md).
