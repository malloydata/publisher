import type { ResolvedTheme } from "./types";

/**
 * Build the CSS variable map applied to the wrapper element around
 * `<malloy-render>`. The values here power two things only:
 *
 * 1. The dashboard tile chrome rules in
 *    {@link PUBLISHER_RENDERER_OVERRIDES_CSS} (`.dashboard-item`,
 *    `.dashboard-item-title`, `.dashboard-item-value`, ...). The
 *    renderer doesn't expose those surfaces through its `theme` prop,
 *    so we paint them ourselves via the var cascade.
 *
 * 2. Renderer-internal rules that read the same `--malloy-render--*`
 *    names. The renderer's own `theme` prop write (see
 *    `buildMalloyExplicitTheme`) usually sets these inline on a deeper
 *    element first, but we set them on the outer wrapper so the var
 *    has a value even when the prop is partial.
 *
 * Single namespace by design: the `--malloy-theme--*` shadow set we
 * used to emit was a workaround for the renderer's
 * `var(--malloy-theme--<key>)` fallback lookup, which only fires when
 * no explicit `theme` prop is given. With the prop wired we no longer
 * compete with that lookup, so the shadow namespace just bloated
 * every wrapper element's inline style.
 */
export function buildTableCssVars(
   theme: ResolvedTheme,
): Record<string, string> {
   return {
      "--malloy-render--font-family": theme.font.family,
      "--malloy-render--table-font-size": `${theme.font.size}px`,
      "--malloy-render--table-header-color": theme.tableHeader,
      "--malloy-render--table-body-color": theme.tableBody,
      "--malloy-render--table-border": theme.border,
      "--malloy-render--table-pinned-background": theme.tile,
      "--malloy-render--table-pinned-border": theme.pinnedBorder,
      // Drives the dashboard tile title (e.g. "by_month" above a chart)
      // and the dimension-name text via injectRendererOverrides.
      "--malloy-render--label-color": theme.tileTitle,
      // The numeric value rendered under each tile title. Not editable
      // in v1; computed from the active mode for readable contrast.
      "--malloy-render--value-color": theme.valueColor,
   };
}
