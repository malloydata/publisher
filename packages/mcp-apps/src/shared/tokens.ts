// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Visual tokens for the widget chrome.
 *
 * Deliberately neutral. This bundle ships in the open-source Publisher, so it
 * carries no product palette and no licensed font: greys plus the platform UI
 * font stack, which reads as native in whichever chat client hosts the iframe.
 *
 * Publisher's own web UI has a richer theme layer in `@malloy-publisher/sdk`
 * (`buildVegaThemeOverride`, `buildMalloyExplicitTheme`, and `# theme.*`
 * annotation support). It is NOT imported here: `sdk/src/theme/defaults.ts`
 * reaches `components/styles.ts`, which imports `@mui/material`, and MUI has no
 * business in a chat widget bundle. Matching that theme, so a chart looks the
 * same in chat as in the Publisher UI, is worth doing separately.
 *
 * Chart colours are not set here at all. The renderer's own defaults apply, so
 * the charts look like Malloy's charts.
 *
 * LIGHT ONLY, and worth stating rather than discovering: these are fixed hex
 * values with no `prefers-color-scheme` variant, so the card is a white panel
 * with dark text even in a client running a dark theme. It will look like a
 * light patch in a dark conversation. Not addressed here for the same reason as
 * `# theme.*` parity: doing it properly means reading the host's own theme, which
 * the MCP Apps spec exposes (`McpUiHostStylesSchema` carries `--color-*` and
 * `--font-*` variables), so the honest fix is to consume those rather than to
 * guess with a media query the host may not reflect.
 */

export const FONT_SANS =
   'system-ui, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif';

export const FONT_MONO =
   'ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace';

export const tokens = {
   background: "#FFFFFF",
   surface: "#F7F7F7",
   border: "#E2E2E2",
   borderStrong: "#D0D0D0",
   text: "#333333",
   textMuted: "#6B6B6B",
   textFaint: "#9A9A9A",
   warningBackground: "#FDF6E3",
   warningBorder: "#E0CE9A",
   warningText: "#7A6320",
} as const;
