import type { ResolvedTheme } from "./types";

/**
 * Build the CSS variable map consumed by the renderer's compiled styles.
 * Set as inline `style` on the element that wraps `<malloy-render>` so
 * each rendered viz inherits the active theme without touching global
 * stylesheets.
 *
 * We emit TWO namespaces for the same values:
 *
 *   --malloy-render--*  : final names used by the renderer's compiled CSS
 *                         rules (e.g. `color: var(--malloy-render--table-
 *                         header-color)`). Setting these works for code
 *                         paths that don't have a theme-shadowing layer.
 *
 *   --malloy-theme--*   : escape hatch. The renderer's internal `Ti()`
 *                         helper resolves theme keys to
 *                         `var(--malloy-theme--<kebab-key>)` when no
 *                         Malloy `# theme.*` annotation is present, then
 *                         writes its own `--malloy-render--*` inline
 *                         style on a deeper element. That inner write
 *                         shadows our outer `--malloy-render--*` value,
 *                         so for keys the renderer re-emits we have to
 *                         set the `--malloy-theme--*` source so the
 *                         shadowed `var(...)` resolves to our colour.
 *
 * Names are derived from the renderer source — search the bundle for
 * `Ti(` in `dist/module/index.mjs` to confirm the kebab mapping.
 */
export function buildTableCssVars(
   theme: ResolvedTheme,
): Record<string, string> {
   const border =
      theme.mode === "dark" ? "1px solid #334155" : "1px solid #e5e7eb";
   const pinnedBorder =
      theme.mode === "dark" ? "1px solid #475569" : "1px solid #daedf3";
   // The dashboard tile title and dimension-name text colour come from
   // theme.tileTitle. The value text below them stays a step heavier
   // for hierarchy; pick a contrast variant per mode.
   const valueColor = theme.mode === "dark" ? "#f1f5f9" : "#1f2937";
   const fontSize = `${theme.font.size}px`;

   return {
      // --malloy-render--* set: direct consumers and our own override
      // rules in injectRendererOverrides. `background` and
      // `table-background` are intentionally omitted: the operator's
      // colour should stay scoped to the viz surfaces (charts via Vega,
      // tables via the renderer's `theme.tableBackground` prop). The
      // dashboard root and row-headers fall through to the renderer's
      // own neutral default so the chrome between tiles doesn't take
      // on the operator's accent colour.
      "--malloy-render--font-family": theme.font.family,
      "--malloy-render--table-header-color": theme.tableHeader,
      "--malloy-render--table-body-color": theme.tableBody,
      "--malloy-render--table-border": border,
      "--malloy-render--table-pinned-background": theme.tile,
      "--malloy-render--table-pinned-border": pinnedBorder,
      "--malloy-render--table-font-size": fontSize,
      // label-color drives the dashboard tile title (e.g. "by_month"
      // above a chart). The renderer doesn't expose this in its theme
      // prop yet, so the value lands here for the
      // injectRendererOverrides CSS rule to pick up.
      "--malloy-render--label-color": theme.tileTitle,
      "--malloy-render--value-color": valueColor,

      // --malloy-theme--* set: source for the renderer's internal
      // `var(--malloy-theme--<key>)` fallbacks. These are what the
      // renderer writes through when no `# theme.*` annotation is
      // present, and they win over our outer --malloy-render--* set
      // for the table/dashboard styling that goes through that path.
      // Same omission of `background`: don't paint the dashboard root.
      "--malloy-theme--font-family": theme.font.family,
      "--malloy-theme--table-header-color": theme.tableHeader,
      "--malloy-theme--table-body-color": theme.tableBody,
      "--malloy-theme--table-border": border,
      "--malloy-theme--table-pinned-background": theme.tile,
      "--malloy-theme--table-pinned-border": pinnedBorder,
      "--malloy-theme--table-font-size": fontSize,
   };
}
