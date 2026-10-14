<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Theming

> What this is: how to control Publisher's light/dark theme and chart palette. For the full public
> walkthrough (editor UI and per-chart annotation examples), see
> [Theming Publisher](https://docs.malloydata.dev/documentation/user_guides/publishing/theming).

Publisher renders charts, tables, and dashboard tiles with a light/dark theme. You can change it in
three places, from broadest to narrowest:

1. The config file `publisher.config.json`, at the instance level.
2. The in-app Theme Editor at `/settings/theme`, which lets an operator iterate against the live UI.
3. Per-chart `# theme.*` annotations inside a `.malloy` file, for one-off styling on a single view.

These cascade: defaults → instance config → editor → per-chart annotation. Each layer only overrides
the keys it sets. (A per-environment layer is reserved but not yet applied.)

The Theme Editor writes to a runtime store (DuckDB, persisted alongside other server state). It's
blocked when `publisher.config.json` has `frozenConfig: true`, the same way every other runtime
mutation is.

## Theme object shape

```jsonc
{
  "theme": {
    "defaultMode": "light",
    "allowUserToggle": true,
    "palette": {
      "series": ["#14b3cb", "#e47404", "#1474a4"],
      "background": { "light": "#ffffff", "dark": "#1e293b" },
      "tableHeader": { "light": "#5d626b", "dark": "#cbd5e1" },
      "tableHeaderBackground": { "light": "#f5fafc", "dark": "#1e293b" },
      "tableBody": { "light": "#727883", "dark": "#e2e8f0" },
      "tile": { "light": "#f5fafc", "dark": "#0f172a" },
      "tileTitle": { "light": "#5d626b", "dark": "#94a3b8" },
      "mapColor": { "light": "#14b3cb", "dark": "#14b3cb" },
      "border": { "light": "#e2e8f0", "dark": "#334155" },
      "cardBorder": { "light": "#cbd5e1", "dark": "#475569" },
      "axis": { "light": "#cbd5e1", "dark": "#475569" },
      "gridline": { "light": "#cbd5e1", "dark": "#475569" },
      "chartText": { "light": "#0f172a", "dark": "#e2e8f0" },
      "value": { "light": "#0f172a", "dark": "#f1f5f9" },
    },
    "font": { "family": "Inter, sans-serif", "size": 12 },
  },
}
```

`defaultMode` accepts `"light"`, `"dark"`, or `"auto"`. With `"auto"` the viewer's OS preference
(`prefers-color-scheme`) wins until they override it from the mode toggle in the sidebar. Setting
`allowUserToggle: false` hides the toggle and locks viewers into `defaultMode`.

`palette.series` and `font` are shared across modes; the rest of the palette keys take an explicit
`{ light, dark }` pair. `mapColor` is the saturated end of the choropleth gradient on `# shape_map`
and `# segment_map` visualizations.

The chrome around the data has its own keys, so a host can match its own hairlines and text without
overriding the renderer's CSS. The values in the example above are the built-in defaults.

| Key          | Paints                                                               |
| ------------ | -------------------------------------------------------------------- |
| `border`     | Table gridlines and row rules                                        |
| `cardBorder` | A dashboard card's edge, and the rule under a pinned table header    |
| `axis`       | Chart axis domain and tick lines                                     |
| `gridline`   | Chart gridlines                                                      |
| `chartText`  | Chart text: axis labels and titles, legends, chart and header titles |
| `value`      | The big-value (KPI) number on a dashboard tile                       |

A colour set for `light` only is not carried into dark for these keys: dark keeps its default.

## Embedding: matching a host's design system

An app that embeds the report reader or builder through the SDK (as Credible does) styles its chrome
in three layers. Each reaches something the others do not, so a host uses all three.

1. **The host's MUI theme** (its `ThemeProvider`): text and surface colours, hover and selection
   tints, and the library parts the builder is made of, such as text fields, buttons, checkboxes,
   switches and menus. The builder reads `palette.text`, `palette.background`, `palette.action` and
   `palette.warning`, so a host palette carries through.
2. **The instance theme** above (`ServerProvider`'s `theme`, layered over the server's): the data's
   colours, the font, the accent (the first series colour) used for selection and focus, and
   `cardBorder`, the one edge every card, filter button and outlined control is drawn in.
3. **`ui`: the chrome's design tokens**, passed to `ServerProvider` (or `ThemeProvider`). Any subset;
   the rest keep the SDK's values. These belong to the host app, not the server, so they are a prop
   rather than part of the theme object above.

```tsx
<ServerProvider
   ui={{
      radius: { control: "4px", tile: "6px", menu: "6px", dialog: "8px" },
      size: { control: 36 },
      type: {
         control: { fontSize: 14, fontWeight: 500 },
         pageTitle: { fontSize: 28, fontWeight: 700 },
      },
      font: { mono: "JetBrains Mono, monospace" },
      shadow: { menu: "0 4px 16px rgba(0, 0, 0, 0.16)" },
   }}
>
```

| Token                       | Default                           | What draws with it                                                                |
| --------------------------- | --------------------------------- | --------------------------------------------------------------------------------- |
| `radius.control`            | `6px`                             | Buttons, segmented controls, filter buttons, list rows                            |
| `radius.tile`               | `8px`                             | Tile cards, popovers, the add-tile preview, kind cards                            |
| `radius.menu`               | `10px`                            | The Add menu, a tile's ⋯ menu, the save-status menu                               |
| `radius.dialog`             | `12px`                            | Every builder window                                                              |
| `size.control`, `size.chip` | `32`, `24` (px)                   | Control and chip heights                                                          |
| `type.pageTitle`            | 24 / 600                          | The report's title                                                                |
| `type.tileTitle`            | 14 / 600                          | A tile's title                                                                    |
| `type.prose`                | 15, line height 1.6               | Text tiles and the description                                                    |
| `type.control`              | 13 / 500                          | Buttons, menu items, filter buttons, list rows                                    |
| `type.caption`              | 12                                | Subtitles, hints, secondary lines                                                 |
| `type.label`                | 12 / 600                          | Small headings in a window ("Chart", "Preview"); its weight is the builder's bold |
| `type.code`                 | 11.5, `font.mono`                 | Malloy names: views, fields, sources                                              |
| `font.mono`                 | `ui-monospace, SFMono-Regular, …` | The face Malloy names are set in                                                  |
| `shadow.menu`               | a soft 24px drop                  | Under menus                                                                       |

What no token reaches is selectable by a **stable class name**, exported as `reportClasses`, for a
global stylesheet or a styled wrapper:

| Class                               | Part                                                                                                                    |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `publisher-report`                  | A report page, read or edited                                                                                           |
| `publisher-report-header`, `-title` | The title and description, and the title alone                                                                          |
| `publisher-filter-row`              | The row of filter buttons                                                                                               |
| `publisher-filter`                  | One filter's button (`data-set`, `data-problem`)                                                                        |
| `publisher-filter-popover`          | The popover a filter opens                                                                                              |
| `publisher-tile`                    | A tile's card (`data-tile-kind="query"` or `"text"`)                                                                    |
| `publisher-tile-heading`            | A tile's title and subtitle                                                                                             |
| `publisher-drill`                   | A clickable cell                                                                                                        |
| `publisher-builder`                 | The builder (`data-saves-to`)                                                                                           |
| `publisher-builder-bar`             | The builder's controls as one bar (`BuilderBar`)                                                                        |
| `publisher-save-status`             | The bar's save status                                                                                                   |
| `publisher-builder-tile`            | A tile in the builder: its selection, handle and menu                                                                   |
| `publisher-width-handle`            | The grip that sets a tile's width                                                                                       |
| `publisher-menu`                    | The builder's menus                                                                                                     |
| `publisher-dialog`                  | Every builder window, with `publisher-add-tile-dialog`, `publisher-filter-dialog` or `publisher-clickable-cells-dialog` |

These names are API: they are kept across releases, and one that goes away is said so in the release
notes. MUI's own class names (`.MuiButton-root` and the like) are not, and change between versions.
