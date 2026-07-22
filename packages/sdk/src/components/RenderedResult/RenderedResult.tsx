import { Box } from "@mui/material";
import React, { Suspense, useEffect, useLayoutEffect, useRef } from "react";
import { buildMalloyExplicitTheme } from "../../theme/buildMalloyExplicitTheme";
import { buildTableCssVars } from "../../theme/buildTableCssVars";
import { buildVegaThemeOverride } from "../../theme/buildVegaThemeOverride";
import { readChartAnnotations } from "../../theme/readChartAnnotations";
import { resolveTheme } from "../../theme/resolveTheme";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { ResolvedTheme } from "../../theme/types";

type MalloyRenderElement = HTMLElement & Record<string, unknown>;

/**
 * Loose alias for the {@link MalloyViz} instance returned by createViz().
 * Avoids pinning the SDK to a specific class import; we only call methods
 * declared on the renderer's public type. Keep in sync with
 * `@malloydata/render` if more methods are needed.
 */
interface MalloyVizHandle {
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
   onDrill?: (element: unknown) => void;
}

const createRenderer = async (
   theme: ResolvedTheme,
   onDrill?: (element: unknown) => void,
): Promise<MalloyVizHandle> => {
   if (typeof window === "undefined") {
      throw new Error("MalloyRenderer can only be used in browser environment");
   }

   const { MalloyRenderer } = await import("@malloydata/render");
   const renderer = new MalloyRenderer({
      onClick: onDrill,
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

function applyTableCssVars(element: HTMLElement, theme: ResolvedTheme): void {
   const vars = buildTableCssVars(theme);
   for (const [key, value] of Object.entries(vars)) {
      element.style.setProperty(key, value);
   }
}

/**
 * The renderer puts its DOM inside a Shadow Root (attachShadow), so a
 * `<style>` tag in `document.head` cannot reach `.dashboard-item` or any
 * of the renderer's other internal classes — Shadow DOM blocks selector
 * matching across the boundary. CSS variables DO cross the boundary
 * (that's why `--malloy-render--background` already paints the top-level
 * `.malloy-render` container), but selectors don't.
 *
 * The renderer's own API for "add a stylesheet that takes effect inside
 * the shadow root" is `MalloyViz.addStylesheet()`. We register our
 * overrides through that path once, at module load, before any viz is
 * constructed. The renderer dedupes by identity, so calling it multiple
 * times in dev (HMR) is safe.
 */
const PUBLISHER_RENDERER_OVERRIDES_CSS = `
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
.malloy-render .malloy-dashboard .dashboard-item {
   /* Tile padding around each chart / table. Uses our custom
      --malloy-render--tile-background so the operator can theme the
      tile separately from the table header row (which paints from
      --malloy-render--table-pinned-background below). */
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
   onDrill,
   onSizeChange,
}: RenderedResultProps) {
   const ref = useRef<HTMLDivElement>(null);
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
            viz = await createRenderer(effectiveTheme, onDrill);
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
         applyTableCssVars(stage, effectiveTheme);
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
            viz.onReady(promote);
            // If onReady never fires (an async render error that only reaches
            // the renderer's onError), force the swap after a bounded wait so
            // the outcome becomes visible instead of the previous chart
            // lingering with stale data indefinitely.
            readyFallback = setTimeout(promote, 10000);
         } catch (error) {
            console.error("Error rendering visualization:", error);
            observer?.disconnect();
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
   }, [result, onDrill, onSizeChange, baseTheme, layers, mode]);

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
