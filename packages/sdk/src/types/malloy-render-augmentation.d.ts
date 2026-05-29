// Type-augmentation stopgap for @malloydata/render.
//
// The Publisher SDK consumes a `theme` prop on `MalloyRenderer` and a
// `MalloyExplicitTheme` type that ship in `monty/explicit-theme-prop`
// on `malloydata/malloy` and have not yet been published to npm. In
// local dev the renderer is consumed via `bun link` against a checkout
// that already has these exports, so this file is a no-op locally;
// its only job is to satisfy CI's `tsc --noEmit` step against the
// currently-published renderer (which lacks them).
//
// At runtime an older renderer silently ignores the unknown `theme`
// option, so the failure mode is graceful degradation (chart canvas
// won't follow `theme.background` / `mapColor` until the new renderer
// publishes; table chrome still themes via the CSS-var path).
//
// REMOVE THIS FILE once the renderer PR merges and @malloydata/render
// publishes a version that exports MalloyExplicitTheme natively, and
// bump the dependency in packages/sdk/package.json to that version.

import "@malloydata/render";

declare module "@malloydata/render" {
   export interface MalloyExplicitTheme {
      tableRowHeight?: string;
      tableBodyColor?: string;
      tableFontSize?: string;
      tableHeaderColor?: string;
      tableHeaderWeight?: string;
      tableBodyWeight?: string;
      tableBorder?: string;
      tableBackground?: string;
      tableGutterSize?: string;
      tablePinnedBackground?: string;
      tablePinnedBorder?: string;
      fontFamily?: string;
      background?: string;
      mapColor?: string;
   }

   interface MalloyRendererOptions {
      theme?: MalloyExplicitTheme;
   }
}
