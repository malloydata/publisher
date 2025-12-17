import React, { Suspense, useLayoutEffect, useRef } from "react";

type MalloyRenderElement = HTMLElement & Record<string, unknown>;

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

// Simple dynamic import function
const createRenderer = async (onDrill?: (element: unknown) => void) => {
   // Only import when we're in a browser environment
   if (typeof window === "undefined") {
      throw new Error("MalloyRenderer can only be used in browser environment");
   }

   const { MalloyRenderer } = await import("@malloydata/render");
   const renderer = new MalloyRenderer({
      onClick: onDrill,
      onError: (error) => {
         console.error("Error rendering visualization:", typeof error, error);
      },
   });
   return renderer.createViz();
};

// Inner component that actually renders the visualization
function RenderedResultInner({
   result,
   height,
   onDrill,
   onSizeChange,
}: RenderedResultProps) {
   const ref = useRef<HTMLDivElement>(null);

   // Render the visualization once the component mounts
   useLayoutEffect(() => {
      if (!ref.current || !result) return;

      let isMounted = true;
      const element = ref.current;

      // Clear previous content
      while (element.firstChild) {
         element.removeChild(element.firstChild);
      }

      // Set up observer to measure size after render completes
      let observer: MutationObserver | null = null;
      let measureTimeout: NodeJS.Timeout | null = null;

      const measureRenderedSize = () => {
         if (!isMounted || !element.firstElementChild) return;

         // It's the grandchild that is the actual visualization.
         const child = element.firstElementChild as HTMLElement;
         const grandchild = child.firstElementChild as HTMLElement;
         if (!grandchild) return;
         const renderedHeight =
            grandchild.scrollHeight || grandchild.offsetHeight || 0;

         if (renderedHeight > 0 && onSizeChange) {
            onSizeChange(renderedHeight);
         }
      };

      createRenderer(onDrill)
         .then((viz) => {
            if (!isMounted) return;

            // Set up mutation observer to detect when rendering is complete
            observer = new MutationObserver(() => {
               // Debounce - wait for mutations to settle
               if (measureTimeout) clearTimeout(measureTimeout);
               measureTimeout = setTimeout(() => {
                  measureRenderedSize();
                  // Disconnect after measuring to prevent infinite loops
                  observer?.disconnect();
               }, 100);
            });

            observer.observe(element, {
               childList: true,
               subtree: true,
               attributes: true,
            });

            try {
               viz.setResult(JSON.parse(result));
               viz.render(element);
            } catch (error) {
               console.error("Error rendering visualization:", error);
               observer?.disconnect();
            }
         })
         .catch((error) => {
            console.error("Failed to create renderer:", error);
         });

      return () => {
         isMounted = false;
         observer?.disconnect();
         if (measureTimeout) clearTimeout(measureTimeout);
      };
   }, [result, onDrill, onSizeChange]);

   // Always use fixed height - no measurement, no resizing
   return (
      <div
         ref={ref}
         style={{
            width: "100%",
            height: height ? `${height}px` : "100%",
         }}
      />
   );
}

// Main component with error boundary and fallback
export default function RenderedResult(props: RenderedResultProps) {
   // Show loading state if we're in server-side rendering
   if (typeof window === "undefined") {
      return (
         <div
            style={{
               width: "100%",
               height: props.height ? `${props.height}px` : "100%",
               display: "flex",
               alignItems: "center",
               justifyContent: "center",
               color: "#666",
            }}
         >
            Loading...
         </div>
      );
   }

   return (
      <Suspense
         fallback={
            <div
               style={{
                  width: "100%",
                  height: props.height ? `${props.height}px` : "100%",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  color: "#666",
               }}
            >
               Loading visualization...
            </div>
         }
      >
         <RenderedResultInner {...props} />
      </Suspense>
   );
}
