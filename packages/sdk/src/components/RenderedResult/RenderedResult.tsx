// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, useTheme } from "@mui/material";
import React, {
   Suspense,
   useCallback,
   useEffect,
   useLayoutEffect,
   useMemo,
   useRef,
} from "react";
import { buildMalloyExplicitTheme } from "../../theme/buildMalloyExplicitTheme";
import {
   buildTableCssVars,
   DASHBOARD_CARD_PADDING_PX,
   type DashboardCardGeometry,
} from "../../theme/buildTableCssVars";
import { buildVegaThemeOverride } from "../../theme/buildVegaThemeOverride";
import { readChartAnnotations } from "../../theme/readChartAnnotations";
import { resolveTheme } from "../../theme/resolveTheme";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { ResolvedTheme } from "../../theme/types";
import {
   DRILL_CELL_CLASS,
   moveDrillStop,
   drillableFieldNames,
   markDrillableCells,
   type DrillMetadataSource,
} from "../drill/markDrillableCells";
import type { DrillClickPayload } from "../drill/resolveDrill";
import type { DrillBinding } from "../drill/useDrill";

type MalloyRenderElement = HTMLElement & Record<string, unknown>;

/**
 * Loose alias for the {@link MalloyViz} instance returned by createViz().
 * Avoids pinning the SDK to a specific class import; we only call methods
 * declared on the renderer's public type. Keep in sync with
 * `@malloydata/render` if more methods are needed.
 */
interface MalloyVizHandle extends DrillMetadataSource {
   setResult: (result: unknown) => void;
   render: (element: HTMLElement) => void;
   remove: () => void;
   // Fires once the chart has painted (or immediately if it already has). We
   // use it to swap a freshly-rendered chart in only when it's ready, so the
   // previously-painted chart never blanks out mid-render.
   onReady: (callback: () => void) => void;
}

declare global {
   // eslint-disable-next-line @typescript-eslint/no-namespace
   namespace JSX {
      interface IntrinsicElements {
         "malloy-render": React.DetailedHTMLProps<
            React.HTMLAttributes<HTMLElement>,
            MalloyRenderElement
         >;
      }
   }
}

interface RenderedResultProps {
   result: string;
   height?: number;
   isFillElement?: (boolean) => void;
   onSizeChange?: (height: number) => void;
   /**
    * Makes `# drill` cells clickable, and makes them look it. From `useDrill`
    * on the host surface, which decides where a click may go; omitted where
    * nothing can act on one, and then the result renders inert.
    */
   drill?: DrillBinding;
}

const createRenderer = async (
   theme: ResolvedTheme,
   onClick?: DrillBinding["onClick"],
): Promise<MalloyVizHandle> => {
   if (typeof window === "undefined") {
      throw new Error("MalloyRenderer can only be used in browser environment");
   }

   const { MalloyRenderer } = await import("@malloydata/render");
   const renderer = new MalloyRenderer({
      onClick,
      vegaConfigOverride: buildVegaThemeOverride(theme),
      // Pass the explicit theme so table chrome and dashboard tiles
      // pick up the operator's colours directly. Without this the
      // renderer's own inline style writes `--malloy-render--*` values
      // sourced from its built-in defaults and shadows whatever we set
      // on the outer wrapper.
      theme: buildMalloyExplicitTheme(theme),
      onError: (error) => {
         console.error("Error rendering visualization:", typeof error, error);
      },
   });
   return renderer.createViz() as MalloyVizHandle;
};

// Warm the renderer chunk as soon as this module loads so the first chart
// paint doesn't have to wait on the dynamic import resolving (the async
// import is what widened the clear-then-repaint gap into a visible flicker).
if (typeof window !== "undefined") {
   void import("@malloydata/render");
}

/**
 * Pull a per-chart Theme override out of a parsed Malloy result by reading
 * `# theme.*` annotations on the model and the query. Returns `undefined`
 * if no theme annotations are present, the shape is unexpected, or the
 * annotation parser is unavailable at runtime.
 *
 * Three failure modes are kept distinct so a malformed annotation doesn't
 * masquerade as a missing dependency:
 *
 * 1. No `theme.*` annotations in the result — return undefined silently.
 * 2. `@malloydata/malloy-tag` not resolvable at runtime — return undefined
 *    silently. SDK consumers may bundle without it; the chart still renders.
 * 3. Parser threw on a malformed annotation — return undefined AND log a
 *    `console.warn` with the offending lines so authors see the typo.
 */
async function extractChartThemeOverride(parsed: unknown) {
   if (!parsed || typeof parsed !== "object") return undefined;
   const r = parsed as {
      annotations?: Array<{ value?: string }>;
      model_annotations?: Array<{ value?: string }>;
      source_annotations?: Array<{ value?: string }>;
   };
   const lines = [
      ...(Array.isArray(r.model_annotations) ? r.model_annotations : []),
      ...(Array.isArray(r.source_annotations) ? r.source_annotations : []),
      ...(Array.isArray(r.annotations) ? r.annotations : []),
   ]
      .map((a) => (typeof a?.value === "string" ? a.value : undefined))
      .filter((s): s is string => Boolean(s));
   if (lines.length === 0) return undefined;

   let parseAnnotation: typeof import("@malloydata/malloy-tag").parseAnnotation;
   try {
      ({ parseAnnotation } = await import("@malloydata/malloy-tag"));
   } catch {
      // Missing peer dep is an acceptable fallback. Charts render with the
      // shell theme only.
      return undefined;
   }

   try {
      const { tag } = parseAnnotation(lines);
      return readChartAnnotations(tag);
   } catch (error) {
      console.warn(
         "Failed to parse # theme.* annotations; chart will render with the shell theme.",
         { error, lines },
      );
      return undefined;
   }
}

function applyTableCssVars(
   element: HTMLElement,
   theme: ResolvedTheme,
   card: DashboardCardGeometry,
): void {
   const vars = buildTableCssVars(theme, card);
   for (const [key, value] of Object.entries(vars)) {
      element.style.setProperty(key, value);
   }
}

/**
 * CSS overrides for the renderer's own hardcoded colours.
 *
 * These are appended as a `<style>` in `document.head` by
 * `injectRendererOverrides` below, which is the mechanism actually in use and
 * is described where it happens. An earlier version of this block said the
 * renderer puts its DOM in a Shadow Root and that we register through
 * `MalloyViz.addStylesheet()`. Neither is true of this code: nothing here calls
 * that API, and were the boundary real a `document.head` stylesheet could not
 * reach `.malloy-dashboard` at all, which is precisely what these rules do and
 * what the theming tests check. Selectors are written at higher specificity
 * than the renderer's own so they win regardless of stylesheet order.
 */
const PUBLISHER_RENDERER_OVERRIDES_CSS = `
/* Card geometry, through the renderer's OWN theme vars rather than over the top
   of them. It declares --malloy-theme--dashboard-* on .malloy-render itself and
   passes each through to the --malloy-render--* the card CSS reads, so a value
   inherited from our wrapper loses to that declaration however early it is set:
   this has to be a rule on the same element, at higher specificity. Doubling the
   class does that, and then no !important is needed — which matters, because
   !important here also lands on .dashboard-item-borderless and repaints a card
   the author asked to have no card. */
.malloy-render.malloy-render {
   --malloy-theme--dashboard-card-radius: var(--publisher-dashboard-card-radius);
   --malloy-theme--dashboard-card-padding: var(--publisher-dashboard-card-padding);
}
/* dashboard.css hardcodes background: #f7f9fc on .malloy-dashboard
   and .dashboard-row-header, which would paint light grey in dark
   mode and also bleed an operator-picked palette.background across
   the panel chrome in light. Both surfaces use
   --malloy-render--background here, which buildMalloyExplicitTheme
   wires to dashboardRoot: a mode-keyed neutral that stays decoupled
   from palette.background (the chart canvas) on purpose. Selectors
   are duplicated at higher specificity to beat the renderer s own
   scoped rules. */
.malloy-render .malloy-dashboard,
.malloy-render.malloy-render .malloy-dashboard,
div.malloy-render .malloy-dashboard {
   /* --publisher-dashboard-root is set on the outer Publisher wrapper
      (see buildTableCssVars) and stays decoupled from
      --malloy-render--background, which gets shadowed inside the
      renderer when a theme.background annotation is present. The
      dedicated var means the panel between tiles always paints the
      operator neutral dashboardRoot regardless of annotations. */
   background: var(--publisher-dashboard-root) !important;
   background-color: var(--publisher-dashboard-root) !important;
   color: var(--malloy-render--table-body-color) !important;
}
.malloy-render .malloy-dashboard .dashboard-row,
.malloy-render .malloy-dashboard .dashboard-row-body,
.malloy-render .malloy-dashboard .dashboard-row-header,
.malloy-render.malloy-render .malloy-dashboard .dashboard-row-header,
div.malloy-render .malloy-dashboard .dashboard-row-header {
   /* Belt + suspenders: dashboard.css hardcodes .dashboard-row-header
      to #f7f9fc, and row / row-body have no explicit background but
      get caught in any annotation-driven inline writes. Force all of
      them to the neutral panel colour. */
   background: var(--publisher-dashboard-root) !important;
   background-color: var(--publisher-dashboard-root) !important;
}
.malloy-render .malloy-dashboard .dashboard-item:not(.dashboard-item-borderless) {
   /* Tile padding around each chart / table. Uses our custom
      --malloy-render--tile-background so the operator can theme the
      tile separately from the table header row (which paints from
      --malloy-render--table-pinned-background below).

      A flat bordered card rather than the renderer's shadow ring, which is what
      makes it match the composite tile's Paper. :not(borderless) because
      # borderless asks for no card at all, and an !important background and
      border drew one anyway. */
   background: var(--malloy-render--tile-background) !important;
   color: var(--malloy-render--table-body-color) !important;
   box-shadow: none !important;
   border: var(--malloy-render--table-border) !important;
}
.malloy-render .malloy-dashboard .dashboard-row-header-separator {
   background: var(--malloy-render--table-border) !important;
}
.malloy-render .malloy-table .th.column-cell {
   /* Non-pinned tables have no header background in the renderer's
      own CSS (only pinned scrolled tables paint the pinned-header
      with this var). Force the same value here so every table has a
      visible header band reflecting the operator's choice. */
   background: var(--malloy-render--table-pinned-background) !important;
}
.malloy-render .dashboard-item-title,
.malloy-render .dashboard-dimension-name {
   color: var(--malloy-render--label-color) !important;
}
.malloy-render .dashboard-item-value,
.malloy-render .dashboard-item-value-measure,
.malloy-render .dashboard-dimension-value {
   color: var(--malloy-render--value-color) !important;
}
.malloy-render .malloy-table,
.malloy-render .malloy-list {
   color: var(--malloy-render--table-body-color) !important;
}
.malloy-render .column-cell.th,
.malloy-render .cell-content.header {
   color: var(--malloy-render--table-header-color) !important;
}
/* A cell whose column declares a "# drill" this surface can honor, marked
   by markDrillableCells. Ordinary text with a pointer cursor until hovered,
   then it reads as a link: the same affordance Malloyyo gives the same tag,
   and the only thing that tells a reader a cell navigates. Kept off the
   resting state deliberately: a column painted blue competes with the data.
   (No backticks in here: this is inside a template literal.) */
.malloy-render .${DRILL_CELL_CLASS} {
   cursor: pointer;
}
.malloy-render .${DRILL_CELL_CLASS}:hover > .cell-content {
   color: var(--publisher-drill-link) !important;
   text-decoration: underline;
}
/* Keyboard focus reads the same as hover, plus a ring: a hover colour is not
   something a keyboard user can produce, and a focus ring is not something a
   mouse user sees. focus-visible rather than focus, so a click does not leave a
   ring behind it. (No backticks in here: this is inside a template literal.) */
.malloy-render .${DRILL_CELL_CLASS}:focus-visible {
   outline: 2px solid var(--publisher-drill-link);
   outline-offset: -2px;
}
.malloy-render .${DRILL_CELL_CLASS}:focus-visible > .cell-content {
   color: var(--publisher-drill-link) !important;
   text-decoration: underline;
}
`;

/**
 * Append the overrides as a `<style>` in `document.head`. The renderer
 * adds its own styles the same way (`MalloyViz.addStylesheet` just calls
 * `document.head.appendChild(<style>)`), so this puts our rules in the
 * same cascade. Our selectors are written at higher specificity than the
 * renderer's nested `.malloy-dashboard .dashboard-item`, with !important,
 * so they win whether they land before or after the renderer's stylesheet.
 *
 * Idempotent: the style element is keyed by id, so re-mounts skip
 * re-injection.
 */
const PUBLISHER_RENDERER_OVERRIDE_ID = "publisher-malloy-renderer-overrides";
function injectRendererOverrides(): void {
   if (typeof document === "undefined") return;
   if (document.getElementById(PUBLISHER_RENDERER_OVERRIDE_ID)) return;
   const style = document.createElement("style");
   style.id = PUBLISHER_RENDERER_OVERRIDE_ID;
   style.textContent = PUBLISHER_RENDERER_OVERRIDES_CSS;
   document.head.appendChild(style);
}

function RenderedResultInner({
   result,
   height: inputHeight,
   drill,
   onSizeChange,
}: RenderedResultProps) {
   const ref = useRef<HTMLDivElement>(null);
   // The renderer binds its click handler at construction, so a changing
   // `drill` identity would otherwise re-run the render effect and rebuild the
   // chart: undoing the no-flicker swap below every time the parent
   // re-renders. Read the latest binding through a ref instead, and keep
   // `drill` out of the effect's dependencies, so callers can pass an inline
   // one safely.
   const drillRef = useRef(drill);
   drillRef.current = drill;
   const handleDrillClick = useCallback((payload: DrillClickPayload) => {
      drillRef.current?.onClick(payload);
   }, []);
   // Whether any drill is wired at all, which is what the effect reacts to: a
   // surface that gains or loses the capability has to (re)mark its cells, but
   // a new inline binding with the same capability must not rebuild anything.
   //
   // KNOWN LIMITATION, and the narrower alternative is worse. `canDrill` decides
   // WHICH cells get marked and is read once per render pass, so a predicate
   // that changes while the result stays put leaves the previous pass's marks in
   // place. Depending on the predicate's identity instead was tried and reverted:
   // `canDrill` chains back to the notebook's parameters, so every control edit
   // tore down and rebuilt every chart, which is the flicker this component was
   // fixed for. The stale case needs a host whose predicate resolves after first
   // paint against an unchanged result; it cannot happen in the notebook, where
   // anything that changes the declared givens also rebuilds the cells. Fixing
   // it properly means re-marking without re-rendering, which is a change to the
   // marking pass rather than to this dependency list.
   const drillable = drill !== undefined;
   const hasMeasuredRef = useRef(false);
   // The chart currently painted into the container, held across renders. A
   // new render paints into a fresh offscreen stage and only swaps in (and
   // disposes this one) once it's ready, so the container never blanks between
   // charts. That up-front clear + cleanup dispose was the flicker.
   const liveRef = useRef<{ viz: MalloyVizHandle; node: HTMLElement } | null>(
      null,
   );
   // Bumped on every render-effect run and on unmount. A slow async render that
   // resolves after a newer one has started (or after unmount) checks this and
   // bails, so overlapping renders can't leave two charts or leak a viz.
   const renderGenRef = useRef(0);
   const { theme: baseTheme, layers, mode } = usePublisherTheme();
   // The host app's card radius, so a dashboard card reads as a card of the app
   // it is embedded in rather than as the renderer's 8px on a 4px page. Both
   // cards take it from here; see DashboardTile, which is the other one.
   const cardRadius = useTheme().shape.borderRadius;
   // Memoized because it is a dependency of the render effect below, and a fresh
   // object each render would re-render the chart on every render.
   const cardGeometry = useMemo<DashboardCardGeometry>(
      () => ({
         radius: `${cardRadius}px`,
         padding: `${DASHBOARD_CARD_PADDING_PX}px`,
      }),
      [cardRadius],
   );

   // Dispose the last live viz on unmount only. Deliberately NOT done in the
   // render effect's cleanup: a re-run must keep the old chart until the new
   // one has painted, and the new render disposes it during the swap.
   useEffect(() => {
      return () => {
         renderGenRef.current += 1;
         liveRef.current?.viz.remove();
         liveRef.current = null;
      };
   }, []);

   useLayoutEffect(() => {
      if (!ref.current || !result) return;

      injectRendererOverrides();

      const element = ref.current;
      const myGen = (renderGenRef.current += 1);
      let cancelled = false;
      const isCurrent = () => !cancelled && myGen === renderGenRef.current;
      // Created inside the async body; hoisted so cleanup can tear them down
      // if this render is superseded before it swaps itself into `liveRef`.
      let stage: HTMLDivElement | undefined;
      let viz: MalloyVizHandle | undefined;
      let drillObserver: MutationObserver | null = null;
      let drillFrame = 0;
      let drillKeydown: ((event: KeyboardEvent) => void) | null = null;
      let drillKeyup: ((event: KeyboardEvent) => void) | null = null;
      let drillFocusIn: ((event: FocusEvent) => void) | null = null;
      let observer: MutationObserver | null = null;
      let measureTimeout: NodeJS.Timeout | null = null;
      // Safety net so a render that never signals ready (an async renderer
      // error that only reaches onError) can't leave a previous chart showing
      // stale data forever; see the setTimeout below.
      let readyFallback: ReturnType<typeof setTimeout> | null = null;

      hasMeasuredRef.current = false;

      // Measure the rendered chart's natural height off `root` (the stage that
      // wraps the renderer output) and report it up. Same grandchild/dashboard
      // HACK as before, just anchored on the stage wrapper.
      const measureRenderedSize = (root: HTMLElement) => {
         if (hasMeasuredRef.current || cancelled || !root.firstElementChild)
            return;
         const child = root.firstElementChild as HTMLElement;
         const grandchild = child.firstElementChild as HTMLElement;
         if (!grandchild) return;
         const greatgrandchild = grandchild.firstElementChild as HTMLElement;
         let renderedHeight =
            grandchild.scrollHeight || grandchild.offsetHeight || 0;

         // HACK - malloy dashboards height are determined by the greatgrandchild.
         if (
            greatgrandchild &&
            grandchild.classList.contains("malloy-dashboard")
         ) {
            renderedHeight =
               greatgrandchild.scrollHeight ||
               greatgrandchild.offsetHeight ||
               0;
         }

         if (renderedHeight > 0) {
            hasMeasuredRef.current = true;
            if (onSizeChange) {
               onSizeChange(renderedHeight);
            }
         }
      };

      (async () => {
         let parsed: unknown;
         try {
            parsed = JSON.parse(result);
         } catch (error) {
            console.error("Error parsing visualization result:", error);
            return;
         }

         const perChart = await extractChartThemeOverride(parsed);
         const effectiveTheme = perChart
            ? resolveTheme([...layers, perChart], mode)
            : baseTheme;

         if (!isCurrent()) return;

         try {
            viz = await createRenderer(effectiveTheme, handleDrillClick);
         } catch (error) {
            console.error("Failed to create renderer:", error);
            return;
         }
         if (!isCurrent()) {
            // Superseded during the dynamic import / construction.
            viz.remove();
            viz = undefined;
            return;
         }

         // Render into a fresh stage appended to the container. While a
         // previous chart is still on screen, keep the stage laid out (so the
         // fill chart actually measures a size and `onReady` fires) but hidden
         // and overlaid; reveal it and drop the old chart only once painted.
         const previous = liveRef.current;
         stage = document.createElement("div");
         stage.style.width = "100%";
         stage.style.height = "100%";
         // Theme the chart's table/dashboard chrome on the stage itself, not on
         // the shared container: during a swap the outgoing chart is still a
         // child of the container, so writing the new per-chart CSS vars there
         // would repaint the old chart's chrome to the new theme before it is
         // swapped out. Scoping the vars to this stage keeps each chart stable.
         applyTableCssVars(stage, effectiveTheme, cardGeometry);
         if (previous) {
            element.style.position = "relative";
            stage.style.position = "absolute";
            stage.style.inset = "0";
            stage.style.visibility = "hidden";
         }
         element.appendChild(stage);

         // Fallback measurement if `onReady` never fires: settle on DOM
         // mutations, then measure once.
         const stageNode = stage;
         observer = new MutationObserver(() => {
            if (measureTimeout) clearTimeout(measureTimeout);
            measureTimeout = setTimeout(() => {
               measureRenderedSize(stageNode);
               observer?.disconnect();
            }, 100);
         });
         observer.observe(stage, {
            childList: true,
            subtree: true,
            attributes: true,
         });

         const activeViz = viz;
         let promoted = false;
         const promote = () => {
            if (promoted || !isCurrent()) return;
            promoted = true;
            if (readyFallback) {
               clearTimeout(readyFallback);
               readyFallback = null;
            }
            // The new chart has painted; drop the outgoing one now.
            if (previous) {
               previous.viz.remove();
               if (previous.node.parentNode === element) {
                  element.removeChild(previous.node);
               }
            }
            stageNode.style.position = "";
            stageNode.style.inset = "";
            stageNode.style.visibility = "";
            element.style.position = "";
            liveRef.current = { viz: activeViz, node: stageNode };
            measureRenderedSize(stageNode);
         };

         try {
            // The renderer accepts a Malloy Result; we don't import that type
            // in the SDK to avoid pinning to the malloy core types here.
            viz.setResult(parsed);
            viz.render(stage);

            // Mark the cells a `# drill` makes clickable, so they read as
            // links. The names come from the result's field metadata, which is
            // complete as soon as it is set, but the DOM is not: a
            // `# dashboard` result builds its cards over later frames, so a
            // one-shot pass right after render() would miss every table that
            // appears after it. Hence the observer, which re-marks (batched to
            // a frame) as cards arrive.
            const binding = drillRef.current;
            // Isolated from the render above. The chart has already rendered by
            // this point, and a throw in here would reach the same catch and
            // replace a working result with an error message: the affordance
            // would be taking the chart down with it. `drillableFieldNames`
            // already guards its own metadata read on the same principle.
            try {
               if (binding) {
                  const names = drillableFieldNames(viz, binding.canDrill);
                  // Guarded INSIDE, not just at the first call. The try
                  // below wraps this function's definition, not the frames the
                  // observer schedules it on, so a throw on a re-mark escaped
                  // to the window and the isolation only ever covered the
                  // synchronous pass.
                  const mark = () => {
                     try {
                        markDrillableCells(stageNode, names);
                     } catch (markError) {
                        console.warn("Drill affordance skipped:", markError);
                     }
                  };
                  mark();
                  // Enter and Space fire the drill for a focused cell. Delegated
                  // on the stage rather than bound per cell, because the
                  // renderer rebuilds cells as a dashboard's cards arrive and a
                  // per-cell listener would be lost with them; this one outlives
                  // every re-mark.
                  //
                  // It synthesises a CLICK rather than calling the drill handler
                  // directly, so the payload comes from the renderer's own click
                  // path. Building one here would mean re-deriving which field
                  // and value a cell represents, which is exactly the mapping
                  // this module has no access to and the reason the affordance
                  // is matched on rendered text in the first place.
                  //
                  // The click goes to the cell's INNER `.cell-content`, not to
                  // the cell. Measured, because dispatching on the cell looked
                  // right and silently did nothing: a real mouse click lands on
                  // `.cell-content` and the renderer reads the event target to
                  // identify the field, so a click on the outer cell reaches the
                  // handler with a target it does not recognise and is dropped
                  // without a sound. Falls back to the cell for a shape that has
                  // no inner content node.
                  //
                  // Enter fires on keydown and Space on keyUP, which is the
                  // native button rule and is load-bearing here rather than
                  // pedantry. Firing Space on keydown opened the drill menu and
                  // then the SAME keypress's keyup landed on the now-focused
                  // first menu item and chose it, so a Space drill navigated
                  // straight past the menu it had just opened. Measured: the
                  // menu was visible while the key was held and gone, with the
                  // URL already changed, once it was released.
                  //
                  // Dispatched with the cell's own coordinates rather than
                  // through `HTMLElement.click()`, which reports `clientX/Y` as
                  // 0. A drill offering two destinations anchors its menu on
                  // those numbers (`useDrill` holds no ref into the renderer's
                  // DOM), so a keyboard-opened menu appeared in the corner of
                  // the viewport instead of beside the cell it came from.
                  const fire = (cell: Element) => {
                     const content =
                        cell.querySelector<HTMLElement>(".cell-content");
                     const target = content ?? (cell as HTMLElement);
                     const rect = target.getBoundingClientRect();
                     target.dispatchEvent(
                        new MouseEvent("click", {
                           bubbles: true,
                           cancelable: true,
                           view: target.ownerDocument.defaultView,
                           clientX: Math.round(rect.left + rect.width / 2),
                           clientY: Math.round(rect.bottom),
                        }),
                     );
                  };
                  const drillCell = (event: KeyboardEvent) =>
                     (
                        event.target as HTMLElement | null
                     )?.closest?.<HTMLElement>(`.${DRILL_CELL_CLASS}`) ?? null;
                  // Move the single tab stop within a drillable column.
                  // `markDrillableCells` gives a column one stop rather than one
                  // per cell, so these keys are how a reader reaches the other
                  // rows; without them the affordance would only ever fire on
                  // whichever row held the stop. Clamped rather than wrapping,
                  // which is what a table's rows read as.
                  const rove = (
                     cell: HTMLElement,
                     to: number | "first" | "last",
                  ) => {
                     // The move lives in `markDrillableCells` beside the column
                     // rule, because it has to hold the same invariant the
                     // marking does: exactly one stop per column. Doing it here
                     // by clearing the focused cell assumed that cell HELD the
                     // stop, which a click-focused `tabindex="-1"` cell does not.
                     moveDrillStop(cell, to)?.focus();
                  };
                  drillKeydown = (event: KeyboardEvent) => {
                     const cell = drillCell(event) as HTMLElement | null;
                     if (!cell) return;
                     const roveKey =
                        event.key === "ArrowDown"
                           ? 1
                           : event.key === "ArrowUp"
                             ? -1
                             : event.key === "Home"
                               ? ("first" as const)
                               : event.key === "End"
                                 ? ("last" as const)
                                 : undefined;
                     if (roveKey !== undefined) {
                        // Prevented whether or not the move happened, so the
                        // first and last rows do not scroll the page out from
                        // under a reader who is still inside the column.
                        event.preventDefault();
                        rove(cell, roveKey);
                        return;
                     }
                     if (event.key === "Enter") {
                        // Otherwise Enter submits an enclosing form on an
                        // embedding host.
                        event.preventDefault();
                        fire(cell);
                     } else if (event.key === " ") {
                        // Held Space scrolls the page; the activation itself
                        // waits for keyup.
                        event.preventDefault();
                     }
                  };
                  drillKeyup = (event: KeyboardEvent) => {
                     if (event.key !== " ") return;
                     const cell = drillCell(event);
                     if (!cell) return;
                     event.preventDefault();
                     fire(cell);
                  };
                  // Focus and the tab stop have to agree however focus got
                  // there. The arrow handler moves the stop, but a
                  // `tabindex="-1"` cell is still CLICK-focusable, so without
                  // this a reader who clicked row 5, tabbed away and tabbed back
                  // landed on row 0. That is the same asymmetry the arrow path
                  // already fixes, for the one way of arriving that does not go
                  // through it. `moveDrillStop(cell, 0)` resolves to the cell
                  // itself, so it just makes that cell the column's stop.
                  drillFocusIn = (event: FocusEvent) => {
                     const cell = (
                        event.target as HTMLElement | null
                     )?.closest?.<HTMLElement>(`.${DRILL_CELL_CLASS}`);
                     if (cell) moveDrillStop(cell, 0);
                  };
                  stageNode.addEventListener("keydown", drillKeydown);
                  stageNode.addEventListener("keyup", drillKeyup);
                  stageNode.addEventListener("focusin", drillFocusIn);
                  if (
                     names.size > 0 &&
                     typeof MutationObserver !== "undefined"
                  ) {
                     drillObserver = new MutationObserver(() => {
                        cancelAnimationFrame(drillFrame);
                        drillFrame = requestAnimationFrame(mark);
                     });
                     // childList only: marking writes classes, and observing
                     // attributes would have it retrigger itself every frame.
                     drillObserver.observe(stageNode, {
                        childList: true,
                        subtree: true,
                     });
                  }
               }
            } catch (drillError) {
               // No affordance, and the result stands. Clicks still resolve,
               // because the renderer routes those itself.
               console.warn("Drill affordance skipped:", drillError);
            }

            viz.onReady(promote);
            // If onReady never fires (an async render error that only reaches
            // the renderer's onError), force the swap after a bounded wait so
            // the outcome becomes visible instead of the previous chart
            // lingering with stale data indefinitely.
            readyFallback = setTimeout(promote, 10000);
         } catch (error) {
            console.error("Error rendering visualization:", error);
            observer?.disconnect();
            drillObserver?.disconnect();
            viz.remove();
            viz = undefined;
            if (stageNode.parentNode === element) {
               element.removeChild(stageNode);
            }
         }
      })();

      return () => {
         cancelled = true;
         observer?.disconnect();
         if (measureTimeout) clearTimeout(measureTimeout);
         if (readyFallback) clearTimeout(readyFallback);
         // If this render built a stage but never swapped it into `liveRef`
         // (superseded, or unmounted mid-render), tear it down so it can't
         // leak a viz or leave an orphan node behind.
         if (stage && liveRef.current?.node !== stage) {
            // The keyboard listeners go with the stage, and ONLY with the
            // stage. Removing them unconditionally tied their lifetime to the
            // effect RUN instead, while the stage's is tied to `liveRef`: a
            // re-run tore them off the chart that is still on screen, which
            // keeps its `tabindex` and `role="button"` and so still says Enter
            // and Space do something. Usually that window closes when the new
            // render promotes, but a render that never gets that far (a parse
            // failure, a renderer that throws) leaves the visible chart
            // permanently keyboard-dead while still advertising otherwise.
            // Left on a promoted stage they are collected with the node when
            // `promote` removes it, so nothing leaks by keeping them.
            if (drillKeydown)
               stage.removeEventListener("keydown", drillKeydown);
            if (drillKeyup) stage.removeEventListener("keyup", drillKeyup);
            if (drillFocusIn)
               stage.removeEventListener("focusin", drillFocusIn);
            // The re-marker is scoped the same way, and for the same reason.
            // Disconnecting it unconditionally tied it to the effect RUN: a
            // re-run while a `# dashboard` was still building its cards stopped
            // the LIVE stage being re-marked, so cards arriving after that point
            // never became drillable. Like the listeners, it is collected with
            // the node once a promoted stage is dropped.
            drillObserver?.disconnect();
            cancelAnimationFrame(drillFrame);
            viz?.remove();
            if (stage.parentNode === element) {
               element.removeChild(stage);
            }
            // This render may have set the container position:relative for its
            // overlay stage but never promoted; reset it so no stray relative
            // lingers on the container.
            element.style.position = "";
         }
      };
   }, [
      result,
      handleDrillClick,
      drillable,
      onSizeChange,
      baseTheme,
      layers,
      mode,
      cardGeometry,
   ]);

   // Malloy renderer requires explicit pixel height to render visualizations
   return (
      <div
         ref={ref}
         style={{
            width: "100%",
            height: inputHeight ? `${inputHeight}px` : "400px",
         }}
      />
   );
}

export default function RenderedResult(props: RenderedResultProps) {
   if (typeof window === "undefined") {
      return (
         <Box
            sx={{
               width: "100%",
               height: props.height ? `${props.height}px` : "100%",
               display: "flex",
               alignItems: "center",
               justifyContent: "center",
               color: "text.secondary",
            }}
         >
            Loading...
         </Box>
      );
   }

   return (
      <Suspense
         fallback={
            <Box
               sx={{
                  width: "100%",
                  height: props.height ? `${props.height}px` : "100%",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  color: "text.secondary",
               }}
            >
               Loading visualization...
            </Box>
         }
      >
         <RenderedResultInner {...props} />
      </Suspense>
   );
}
