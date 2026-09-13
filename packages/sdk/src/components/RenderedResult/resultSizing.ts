// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every height decision a rendered result takes, in one module.
 *
 * Three things used to be tangled together and spread across four files: how
 * tall a result should paint, whether it is the kind of result that has a
 * height of its own at all, and which DOM node that height can be read off.
 * They are separated here so the first two are pure and testable, and the third
 * is the single place this package touches the renderer's DOM.
 *
 * The renderer exposes no size API — `MalloyViz` offers `onReady` and
 * `getMetadata()` and nothing that reports a height — so a result that sizes
 * itself has to be measured. What CAN come from the public metadata is which
 * kind of result it is: `getMetadata().getRootField().renderAs()`. Keying off
 * that instead of the rendered class names is what keeps the DOM contact down
 * to one bounded walk.
 */

/**
 * How a rendered root arrives at its height.
 *
 * - `content`: it has a height of its own and the container should shrink to
 *   it — a table, a `# dashboard` grid, a `# big_value`, a list, and the maps,
 *   whose height comes from their aspect ratio.
 * - `container`: it fills whatever box it is handed and has no height to
 *   report. The plotted charts, and only those. Measuring one tells you what
 *   you already gave it, minus the inset it drew itself at.
 */
export type ResultSizing = "content" | "container";

/**
 * Roots that fill their box, named in the renderer's `renderAs()` vocabulary.
 *
 * That vocabulary has two sources and they do not agree, which is the trap
 * here. A field rendered by a plugin reports the PLUGIN's name, and the
 * renderer's default registry names the bar and line plugins `"bar"` and
 * `"line"` — not `"bar_chart"` and `"line_chart"`, which are the tag spellings
 * and appear nowhere in what `renderAs()` returns. Measured: a `# line_chart`
 * tile reports `"line"`. The tag-derived fallback in the renderer, used when no
 * plugin claims the field, collapses both to `"chart"` instead, so that name
 * has to be here too.
 *
 * Measured, because "it is a chart" is not the test — filling the box is. A bar
 * chart reports its box minus an 8px inset at every size: 392 in a 400px tile,
 * 692 in a 700px notebook cell. That inset is the ratchet, and it is why these
 * are never measured. `scatter_chart` is here by analogy with the other plotted
 * charts rather than by measurement; it has the same axes and the same insets.
 *
 * The maps are NOT here, which is the trap in the other direction. A
 * `# shape_map` reports 365 in a 400px tile and 365 in a 700px cell — the same
 * number, so its height comes from its aspect ratio and not from its box. It
 * belongs with the content-sized roots, and putting it here made a map that
 * needs 365px sit in 700px of cell.
 */
const CONTAINER_SIZED_ROOTS: ReadonlySet<string> = new Set([
   "bar",
   "line",
   "chart",
   "scatter_chart",
]);

/**
 * Roots known to have a height of their own, in the same vocabulary.
 *
 * Kept as an explicit list rather than "everything that is not a chart" so a
 * root this SDK has never heard of — a host's own render plugin — can be told
 * apart from both and given the conservative treatment in
 * {@link remeasuresAfterReady}.
 */
const CONTENT_SIZED_ROOTS: ReadonlySet<string> = new Set([
   "table",
   "dashboard",
   "shape_map",
   "segment_map",
   "list",
   "list_detail",
   "big_value",
   "cell",
   "link",
   "image",
]);

/**
 * Which sizing rule a root follows.
 *
 * Unknown names — a host's render plugin, a renderer newer than this SDK — are
 * treated as content-sized, which is the behavior every root had before this
 * classification existed: measure once and use what comes back.
 */
export function resultSizing(renderAs: string | undefined): ResultSizing {
   if (!renderAs) return "content";
   return CONTAINER_SIZED_ROOTS.has(renderAs) ? "container" : "content";
}

/**
 * Whether the result keeps being measured after the first height lands.
 *
 * It has to be, and the acute case is a table. The renderer signals `onReady`
 * as soon as it knows it can paint, and for a table that is immediately — a
 * chart waits for a non-zero parent size, a table does not — so a single
 * measurement taken there lands before the table's virtualized grid has laid
 * out, and reads a height the table never keeps. A `# big_value` row loses the
 * same race more quietly: it reports the box it was handed, so the panel simply
 * keeps the cap and the tile carries a band of empty space under its numbers.
 *
 * Re-measuring a content-sized root TERMINATES, because its height does not
 * depend on the box it is in. `.malloy-table.root` is
 * `height: fit-content; max-height: 100%` in the renderer's own CSS and the
 * wrappers between it and the container add no padding, so shrinking the
 * container to the table's content height is the exact point at which the cap
 * stops binding and the next measurement reports the same number.
 *
 * This used to be tables only, and the reason was the one root this is NOT safe
 * for: a chart draws itself inset from the box it is given, so feeding its
 * height back as the new container height would ratchet the container down by
 * that inset every pass and never converge. Charts are classified
 * container-sized now and are never measured at all, so the hazard is out of
 * the measured set and the narrow exemption it forced can go with it.
 *
 * A root this SDK does not recognise stays on the one-shot path regardless. It
 * could be a host's own fill-the-box viz, and one measurement is what it got
 * before any of this existed.
 */
export function remeasuresAfterReady(renderAs: string | undefined): boolean {
   return renderAs !== undefined && CONTENT_SIZED_ROOTS.has(renderAs);
}

/**
 * How far below the stage the node carrying a root's content height sits.
 *
 * This is the one renderer-DOM assumption left in the SDK, so it is stated
 * rather than inlined. The stage is our own node and is box-sized, so it is
 * never the answer: measuring it would floor every result at the height the
 * container already has and nothing could ever shrink. Its first child is the
 * renderer's outer wrapper, also box-sized, for the same reason.
 *
 * Depth 2 — the wrapper's child — is the root render node, and for every root
 * but one its `scrollHeight` is the content height.
 *
 * A `# dashboard` grid is the exception: its own wrapper is constrained, and
 * the grid one level further down is what reports the height. That used to be
 * spelled as a class-name check on the rendered DOM
 * (`classList.contains("malloy-dashboard")`); it is keyed off `renderAs()` now,
 * which is metadata the renderer publishes rather than markup it happens to
 * emit.
 */
export function contentNodeDepth(renderAs: string | undefined): number {
   return renderAs === "dashboard" ? 3 : 2;
}

/**
 * The node under `stage` that carries the result's content height: `depth`
 * first children down, or null if the renderer has not built that deep yet.
 *
 * This is the node measured AND the node watched for changes, so the two can
 * never drift apart.
 *
 * Null rather than the deepest node available, which is the tempting fallback
 * and is actively wrong. A DOM shallower than `depth` almost always means the
 * renderer is still building, and the shallow nodes are its box-sized wrappers:
 * answering with one reports the height the panel already has, which reads as a
 * successful measurement, stops the retry, and leaves the observer watching a
 * node whose size can never change. That is a `# big_value` tile stuck at its
 * 400px cap with 264px of white under its numbers. Reporting nothing instead
 * leaves the panel at its first-paint height and lets the caller measure again
 * when the renderer mutates the DOM into its final shape.
 */
export function contentNode(
   stage: HTMLElement,
   depth: number,
): HTMLElement | null {
   let node: HTMLElement | null = stage.firstElementChild as HTMLElement | null;
   // Depth 1 is `node` itself; walk the remainder.
   for (let level = 1; level < depth && node; level++) {
      node = node.firstElementChild as HTMLElement | null;
   }
   return node;
}

/**
 * Content height of the rendered result under `stage`, or 0 if the renderer
 * has not built its final shape yet. See {@link contentNode}.
 *
 * `scrollHeight` is the CONTENT height, so this assumes the box adds no chrome
 * of its own. True today: `.malloy-table.root` has no border, and a horizontal
 * scrollbar costs no layout where scrollbars overlay. A wide table clipped by a
 * few pixels on a platform that draws classic scrollbars would be this
 * assumption breaking, and the fix is `+ (offsetHeight - clientHeight)`.
 */
export function measureContentHeight(
   stage: HTMLElement,
   depth: number,
): number {
   const node = contentNode(stage, depth);
   if (!node) return 0;
   return node.scrollHeight || node.offsetHeight || 0;
}

/**
 * Per-tile cap for a dashboard's composite form. A tile is one panel among
 * several, so capping them keeps the grid even instead of letting one long
 * table set the height of its whole row.
 */
export const TILE_MAX_HEIGHT = 400;

/**
 * Cap for a notebook cell result that lays itself out: a table, mostly. Tall
 * enough for a screenful of rows, after which the cell scrolls rather than the
 * page turning into one long table.
 */
export const NOTEBOOK_CELL_MAX_HEIGHT = 700;

/** Cap for a query result shown inside a model page's cell. */
export const MODEL_CELL_MAX_HEIGHT = 600;

/** Cap for the results dialog, which gets most of a tall viewport. */
export const RESULTS_DIALOG_MAX_HEIGHT = 800;

/**
 * Height for a container-sized root whose caller set no cap.
 *
 * A chart has no height of its own, so with nothing to measure and no cap to
 * obey, something has to choose. This is that choice, and it is the same number
 * a dashboard tile gets, so an uncapped chart reads like a tall tile rather
 * than a page-height band.
 *
 * This is what closed the stretch: the single-query dashboard form used to ask
 * for a 20000px "no cap" height, which a bare `# bar_chart` painted at, measured
 * back off itself, and kept — 1992px for a two-row chart against 227px for the
 * same query under a `# dashboard` grid. The cap is `undefined` now and a chart
 * is never measured, so neither half of that can happen.
 */
export const UNCAPPED_CONTAINER_HEIGHT = 400;

/**
 * Height to paint at before a content-sized result has been measured.
 *
 * A cap is also the first paint's height, because the measured height starts
 * out equal to it. Harmless while every caller passes something viewport-sized
 * (400 to 800); a caller that passes no cap needs a number that is not "as tall
 * as you like".
 */
export const INITIAL_RENDER_HEIGHT = 2000;

/** First-paint height, before anything is known about the result. */
export function initialResultHeight(maxHeight: number | undefined): number {
   return maxHeight === undefined
      ? INITIAL_RENDER_HEIGHT
      : Math.min(maxHeight, INITIAL_RENDER_HEIGHT);
}

/**
 * The height a result panel should paint at, given what is known so far.
 *
 * `maxHeight` is a CAP and `undefined` means uncapped — it is not a height
 * request, which is why "no cap" is expressed by leaving it out rather than by
 * passing a number big enough to mean the same thing.
 */
export function resolveResultHeight({
   sizing,
   contentHeight,
   maxHeight,
}: {
   sizing: ResultSizing | undefined;
   contentHeight: number | undefined;
   maxHeight: number | undefined;
}): number {
   if (sizing === "container") {
      return maxHeight ?? UNCAPPED_CONTAINER_HEIGHT;
   }
   if (contentHeight === undefined || contentHeight <= 0) {
      return initialResultHeight(maxHeight);
   }
   return maxHeight === undefined
      ? contentHeight
      : Math.min(maxHeight, contentHeight);
}
