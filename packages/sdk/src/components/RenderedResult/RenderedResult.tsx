import React, {
   Suspense,
   useEffect,
   useLayoutEffect,
   useRef,
   useState,
} from "react";

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

function RenderResultSimple({ result, onDrill }: RenderedResultProps) {
   const ref = useRef<HTMLDivElement>(null);

   useLayoutEffect(() => {
      if (!ref.current || !result) return;
      const element = ref.current;

      createRenderer(onDrill).then((viz) => {
         viz.setResult(JSON.parse(result));
         viz.render(element);
      });
   }, [result, onDrill]);

   return <div ref={ref} style={{ width: "100%", height: "100%" }} />;
}
// Inner component that actually renders the visualization
function RenderedResultInner({
   result,
   height,
   isFillElement,
   onSizeChange,
   onDrill,
}: RenderedResultProps) {
   const ref = useRef<HTMLDivElement>(null);
   const [isRendered, setIsRendered] = useState(false);

   // Render the visualization once the component mounts
   useLayoutEffect(() => {
      if (!ref.current || !result) return;

      let isMounted = true;
      const element = ref.current;

      // Clear previous content
      while (element.firstChild) {
         element.removeChild(element.firstChild);
      }

      createRenderer(onDrill)
         .then((viz) => {
            if (!isMounted) return;

            // Set up a mutation observer to detect when content is added
            const observer = new MutationObserver((mutations) => {
               for (const mutation of mutations) {
                  if (
                     mutation.type === "childList" &&
                     mutation.addedNodes.length > 0
                  ) {
                     const hasContent = Array.from(mutation.addedNodes).some(
                        (node) => node.nodeType === Node.ELEMENT_NODE,
                     );
                     if (hasContent) {
                        observer.disconnect();
                        setTimeout(() => {
                           if (isMounted) {
                              setIsRendered(true);
                           }
                        }, 50);
                        break;
                     }
                  }
               }
            });

            observer.observe(element, {
               childList: true,
               subtree: true,
               characterData: true,
            });

            try {
               viz.setResult(JSON.parse(result));
               viz.render(element);
            } catch (error) {
               console.error("Error rendering visualization:", error);
               observer.disconnect();
               if (isMounted) {
                  setIsRendered(true);
               }
            }
         })
         .catch((error) => {
            console.error("Failed to create renderer:", error);
            if (isMounted) {
               setIsRendered(true);
            }
         });

      return () => {
         isMounted = false;
      };
   }, [result, onDrill]);

   // Set up size measurement using scrollHeight instead of ResizeObserver
   useEffect(() => {
      if (!ref.current || !isRendered) return;
      const element = ref.current;

      // Function to measure and report size
      const measureSize = () => {
         if (element) {
            const measuredHeight = element.offsetHeight;
            if (measuredHeight > 0) {
               if (onSizeChange) {
                  onSizeChange(measuredHeight);
               }
            } else if (isFillElement && element.firstChild) {
               // HACK- we If there's a child and it's height is 0, then we're in a fill element
               // We use the callback `isFillElement` to notify the parent that we're in a fill element
               // the parent should then set height for this element, otherwise it will have size 0.
               const child = element.firstChild as HTMLElement;
               const childHeight = child.offsetHeight;
               if (childHeight == 0) {
                  isFillElement(true);
               } else {
                  isFillElement(false);
               }
            }
         }
      };

      // Initial measurement after a brief delay to let content render
      const timeoutId = setTimeout(measureSize, 100);

      let observer: MutationObserver | null = null;
      // Also measure when the malloy result changes
      observer = new MutationObserver(measureSize);
      observer.observe(element, {
         childList: true,
         subtree: true,
         attributes: true,
      });

      // Cleanup
      return () => {
         clearTimeout(timeoutId);
         observer?.disconnect();
      };
   }, [onSizeChange, result, isFillElement, isRendered]);

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
         {props.onSizeChange ? (
            <RenderedResultInner {...props} />
         ) : (
            <RenderResultSimple {...props} />
         )}
      </Suspense>
   );
}
