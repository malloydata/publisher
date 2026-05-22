import { Box } from "@mui/material";
import React, { Suspense, useLayoutEffect, useRef } from "react";
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
      onError: (error) => {
         console.error("Error rendering visualization:", typeof error, error);
      },
   });
   return renderer.createViz() as MalloyVizHandle;
};

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
.malloy-render .malloy-dashboard {
   background: var(--malloy-render--background) !important;
   color: var(--malloy-render--table-body-color) !important;
}
.malloy-render .malloy-dashboard .dashboard-item {
   background: var(--malloy-render--table-pinned-background) !important;
   color: var(--malloy-render--table-body-color) !important;
   box-shadow: none !important;
   border: var(--malloy-render--table-border) !important;
}
.malloy-render .malloy-dashboard .dashboard-row-header {
   background: var(--malloy-render--background) !important;
}
.malloy-render .malloy-dashboard .dashboard-row-header-separator {
   background: var(--malloy-render--table-border) !important;
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
   const { theme: baseTheme, layers, mode } = usePublisherTheme();

   useLayoutEffect(() => {
      if (!ref.current || !result) return;

      injectRendererOverrides();

      let isMounted = true;
      // Track the active viz instance so the cleanup can dispose it even
      // when unmount races with the async setup (see bug #3 from the code
      // review: viz constructed but never `remove()`'d on rapid navigation).
      let viz: MalloyVizHandle | undefined;
      const element = ref.current;

      while (element.firstChild) {
         element.removeChild(element.firstChild);
      }

      // Apply the shell theme's CSS variables immediately so the table
      // chrome looks right during the async setup window. We re-apply once
      // the per-chart override resolves below, so per-chart annotations
      // affect both Vega and table styles.
      applyTableCssVars(element, baseTheme);

      hasMeasuredRef.current = false;

      let observer: MutationObserver | null = null;
      let measureTimeout: NodeJS.Timeout | null = null;

      const measureRenderedSize = () => {
         if (hasMeasuredRef.current || !isMounted || !element.firstElementChild)
            return;
         const child = element.firstElementChild as HTMLElement;
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

         // Per-chart annotation may have shifted table CSS vars; re-apply.
         if (isMounted) applyTableCssVars(element, effectiveTheme);

         try {
            const created = await createRenderer(effectiveTheme, onDrill);
            if (!isMounted) {
               // Unmounted during the dynamic import / construction. Tear
               // down the viz we just made; the effect's cleanup won't see
               // it because `viz` was unset.
               created.remove();
               return;
            }
            viz = created;

            observer = new MutationObserver(() => {
               if (measureTimeout) clearTimeout(measureTimeout);
               measureTimeout = setTimeout(() => {
                  measureRenderedSize();
                  observer?.disconnect();
               }, 100);
            });

            observer.observe(element, {
               childList: true,
               subtree: true,
               attributes: true,
            });

            // The renderer accepts a Malloy Result; we don't import that type
            // in the SDK to avoid pinning to the malloy core types here.
            viz.setResult(parsed);
            viz.render(element);
         } catch (error) {
            console.error("Error rendering visualization:", error);
            observer?.disconnect();
            viz?.remove();
            viz = undefined;
         }
      })();

      return () => {
         isMounted = false;
         observer?.disconnect();
         if (measureTimeout) clearTimeout(measureTimeout);
         viz?.remove();
         viz = undefined;
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
