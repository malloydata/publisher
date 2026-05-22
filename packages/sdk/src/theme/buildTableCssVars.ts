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
   const body = theme.mode === "dark" ? "#e2e8f0" : "#727883";
   const border =
      theme.mode === "dark" ? "1px solid #334155" : "1px solid #e5e7eb";
   // Tiles sit ON the chart background. In dark mode the chart background
   // is slate (#1e293b); tiles use the page-outer dark (#0f172a) so they
   // read as recessed cards instead of fighting the container.
   const pinnedBg = theme.mode === "dark" ? "#0f172a" : "#f5fafc";
   const pinnedBorder =
      theme.mode === "dark" ? "1px solid #475569" : "1px solid #daedf3";
   // Dashboard tiles split text into a small "label" (field name) and a
   // big "value" (the number). The label is secondary, the value is the
   // primary read. Pick contrast accordingly.
   const labelColor = theme.mode === "dark" ? "#94a3b8" : "#5d626b";
   const valueColor = theme.mode === "dark" ? "#f1f5f9" : "#1f2937";
   const fontSize = `${theme.font.size}px`;

   return {
      // --malloy-render--* set: direct consumers and our own override rules.
      "--malloy-render--background": theme.background,
      "--malloy-render--font-family": theme.font.family,
      "--malloy-render--table-background": theme.background,
      "--malloy-render--table-header-color": theme.tableHeader,
      "--malloy-render--table-body-color": body,
      "--malloy-render--table-border": border,
      "--malloy-render--table-pinned-background": pinnedBg,
      "--malloy-render--table-pinned-border": pinnedBorder,
      "--malloy-render--table-font-size": fontSize,
      "--malloy-render--label-color": labelColor,
      "--malloy-render--value-color": valueColor,

      // --malloy-theme--* set: source for the renderer's internal
      // `var(--malloy-theme--<key>)` fallbacks. These are what the
      // renderer's QWe() writes through when no `# theme.*` annotation
      // is present, and they win over our outer --malloy-render--* set
      // for the table/dashboard styling that goes through that path.
      "--malloy-theme--background": theme.background,
      "--malloy-theme--font-family": theme.font.family,
      "--malloy-theme--table-background": theme.background,
      "--malloy-theme--table-header-color": theme.tableHeader,
      "--malloy-theme--table-body-color": body,
      "--malloy-theme--table-border": border,
      "--malloy-theme--table-pinned-background": pinnedBg,
      "--malloy-theme--table-pinned-border": pinnedBorder,
      "--malloy-theme--table-font-size": fontSize,
   };
}
