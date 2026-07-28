# Theming

> What this is: how to control Publisher's light/dark theme and chart palette. For the full public
> walkthrough (editor UI and per-chart annotation examples), see
> [Theming Publisher](https://docs.malloydata.dev/documentation/user_guides/publishing/theming).

Publisher renders charts, tables, and dashboard tiles with a light/dark theme. You can change it in
three places, from broadest to narrowest:

1. The config file `publisher.config.json`, at the instance level.
2. The in-app Theme Editor at `/settings/theme`, which lets an operator iterate against the live UI.
3. `theme.*` annotations inside a `.malloy` file: `##` at the top of a file styles **every** view in
   it, `#` above one view styles that view alone.

These cascade: defaults → instance config → editor → model annotation → source annotation → view
annotation. Each layer only overrides the keys it sets. (A per-environment layer is reserved but not
yet applied.)

A model-level `## theme.palette.series` is worth using deliberately, because it beats both the config
and the editor and the UI gives no sign that it did: to a viewer, changing the instance palette
appears to do nothing for that model's charts, and the model's own charts stop matching everything
around them — a choropleth reading `mapColor`, a tile's chrome, a second model's charts on the same
page. Reach for it when a model's colors carry meaning of their own (a status field that must be red
and green everywhere), not to make one model look nice.

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
      "background":            { "light": "#ffffff", "dark": "#1e293b" },
      "tableHeader":           { "light": "#5d626b", "dark": "#cbd5e1" },
      "tableHeaderBackground": { "light": "#f5fafc", "dark": "#1e293b" },
      "tableBody":             { "light": "#727883", "dark": "#e2e8f0" },
      "tile":                  { "light": "#f5fafc", "dark": "#0f172a" },
      "tileTitle":             { "light": "#5d626b", "dark": "#94a3b8" },
      "mapColor":              { "light": "#14b3cb", "dark": "#14b3cb" }
    },
    "font": { "family": "Inter, sans-serif", "size": 12 }
  }
}
```

`defaultMode` accepts `"light"`, `"dark"`, or `"auto"`. With `"auto"` the viewer's OS preference
(`prefers-color-scheme`) wins until they override it from the header toggle. Setting
`allowUserToggle: false` hides the toggle and locks viewers into `defaultMode`.

`palette.series` and `font` are shared across modes; the rest of the palette keys take an explicit
`{ light, dark }` pair. `mapColor` is the saturated end of the choropleth gradient on `# shape_map`
and `# segment_map` visualizations.
