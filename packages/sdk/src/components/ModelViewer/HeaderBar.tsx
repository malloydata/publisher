// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Collapse } from "@mui/material";
import { useEffect, useId, useRef, useState, type ReactNode } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { AccessChips, AccessFacts } from "./AccessChips";
import { HeaderCardContext } from "./headerCard";
import { OffSurfaceChip, PublicSurfaceChip } from "./offSurface";
import type { RowLayer } from "./rowLayers";
import { isRestricted, isRowFiltered, type SourceAccess } from "./sourceAccess";
import { FILE_VIEW_TOP_GAP, PANE_CARD_SX, PANE_INSET } from "./surface";
import { fontSizes, MONO_FONT_FAMILY } from "./theme";

/** The last defined value, kept while a collapsing card closes with its contents in it. */
function useHeld<T>(value: T | undefined): T | undefined {
   const held = useRef(value);
   if (value !== undefined) held.current = value;
   return held.current;
}

/**
 * The model page's header: the source's name as the page's heading, and beside
 * it the source picker ("2 sources ▾", {@link import("./SourcesNav").SourcesNav}),
 * with a host's actions on the right. Then the source's status row (who can
 * query it, whether the package exposes the file, and what a host adds), the
 * model's and the source's docs, and the quiet meta line ("Source · 33 fields ·
 * from …").
 */
export function HeaderBar({
   /** The heading: the source's name, or on a model with only queries the model's name. */
   title,
   /** Beside the heading: the source picker. */
   titleAdornments,
   /** What the title is, leading the meta line: "Source · 43 fields". */
   kindLabel,
   /** The meta line's count, after the kind: "33 fields", or "2 queries" on a model with only queries. */
   meta,
   /** The source's own `#(doc)`, printed under its name. */
   doc,
   /** Where the source comes from, at the end of the meta line. */
   provenance,
   /** Controls on the heading line's right. */
   actions,
   /** A host's chips on the status row, after the viewer's own. */
   status,
   /** The title's heading level: the page's h1 where the viewer is the page,
    *  a level down where it is a panel on another page. */
   headingLevel = "h1",
   /** The model file's own description, ahead of the source's. */
   modelDoc,
   /** The source's `#(authorize)` / `#(access_filter)` declarations: chips on
    *  the status row, and their facts in the card the chips open. */
   access,
   /** Whether the reader can change the model, so the card also shows the
    *  rules' Malloy (an editor's vocabulary) beside their words. */
   showRules = false,
   /** False for a model's own header (no sources, only queries): it has no status row. */
   isSource = true,
   /** The file is one the package's `index.malloy` leaves out: its Private chip leads the status row. */
   offSurface,
   /** The file is the package's root `index.malloy`, whose sources are the package's public
    *  ones: its Public chip sits in the status row where a hidden file's Private does. */
   publicSurface = false,
}: {
   title: ReactNode;
   titleAdornments?: ReactNode;
   kindLabel?: string;
   meta?: ReactNode;
   doc?: string;
   provenance?: ReactNode;
   actions?: ReactNode;
   status?: ReactNode;
   headingLevel?: "h1" | "h2";
   modelDoc?: string;
   access?: SourceAccess;
   showRules?: boolean;
   isSource?: boolean;
   offSurface?: boolean;
   publicSurface?: boolean;
}) {
   const hasAccess = isRestricted(access) || isRowFiltered(access);
   // The card under the header holds who can query the source, opened from the
   // Restricted / Row filtered chip that names it, never from the name.
   const layers: RowLayer[] = hasAccess ? ["access"] : [];
   const [openLayer, setOpenLayer] = useState<RowLayer | undefined>(undefined);
   const shownLayer: RowLayer | undefined =
      openLayer !== undefined && layers.includes(openLayer)
         ? openLayer
         : undefined;
   const cardLayer = useHeld(shownLayer);
   const cardId = useId();
   const headerCard = {
      openLayer: shownLayer,
      layers,
      toggle: (layer: RowLayer) =>
         setOpenLayer((current) => (current === layer ? undefined : layer)),
      cardId,
   };
   const hasStatus = hasAccess || offSurface || publicSurface || !!status;
   // The host's page title, a step smaller where the viewer is a section of a page.
   const pageTitle = usePublisherTheme().theme.ui.type.pageTitle;
   const titleSize =
      typeof pageTitle.fontSize === "number"
         ? `${pageTitle.fontSize}px`
         : pageTitle.fontSize;

   return (
      <Box>
         <Box
            data-testid="model-viewer-source-heading"
            sx={{
               display: "flex",
               alignItems: "center",
               flexWrap: "wrap",
               gap: 1.5,
               pt: FILE_VIEW_TOP_GAP,
               pb: 0.5,
            }}
         >
            <Box
               component={headingLevel}
               sx={{
                  m: 0,
                  fontSize:
                     headingLevel === "h1"
                        ? titleSize
                        : `calc(${titleSize} * 0.8)`,
                  fontWeight: pageTitle.fontWeight ?? 600,
                  lineHeight: 1.25,
                  color: "text.primary",
                  fontFamily: MONO_FONT_FAMILY,
                  minWidth: 0,
                  overflowWrap: "anywhere",
               }}
            >
               {title}
            </Box>
            {titleAdornments}
            {actions && (
               <Box
                  sx={{
                     ml: "auto",
                     display: "flex",
                     alignItems: "center",
                     gap: 1,
                  }}
               >
                  {actions}
               </Box>
            )}
         </Box>
         {!isSource && offSurface && (
            <Box sx={{ display: "flex", alignItems: "center", pt: 0.5, pb: 1 }}>
               <OffSurfaceChip />
            </Box>
         )}
         {isSource && hasStatus && (
            // The source's state, all on one row under its name.
            <HeaderCardContext.Provider value={headerCard}>
               <Box
                  data-testid="model-viewer-status-row"
                  sx={{
                     display: "flex",
                     alignItems: "center",
                     flexWrap: "wrap",
                     gap: 1.5,
                     pt: 0.5,
                     pb: 1,
                  }}
               >
                  <AccessChips access={access} />
                  {offSurface && <OffSurfaceChip />}
                  {publicSurface && <PublicSurfaceChip />}
                  {status}
               </Box>
            </HeaderCardContext.Provider>
         )}
         {/* The model's own description, ahead of the source's: it describes the
             file every source here belongs to. */}
         {modelDoc && (
            <SourceDoc
               key={`model:${modelDoc}`}
               doc={modelDoc}
               label="model description"
               testId="model-viewer-model-doc"
            />
         )}
         {/* Keyed by the doc: another source's or query's starts clamped, measured afresh. */}
         {doc && <SourceDoc key={doc} doc={doc} />}
         {/* One quiet line under the name: what it is, how many fields, and where it comes from. */}
         {(kindLabel !== undefined || meta !== undefined || provenance) && (
            <Box
               // Addressable because every join row in the pane below carries
               // its own "N fields" too, so a text match for the count is
               // ambiguous on any source with joins.
               data-testid="model-viewer-source-meta"
               sx={{
                  display: "flex",
                  alignItems: "center",
                  gap: 0.75,
                  minWidth: 0,
                  pb: 2,
                  fontSize: fontSizes.body,
                  color: "text.secondary",
                  whiteSpace: "nowrap",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
               }}
            >
               {kindLabel !== undefined && (
                  <Box component="span" data-testid="model-viewer-source-kind">
                     {kindLabel}
                  </Box>
               )}
               {kindLabel !== undefined &&
                  (meta !== undefined || provenance) && (
                     <Box component="span" aria-hidden>
                        ·
                     </Box>
                  )}
               {meta !== undefined && <Box component="span">{meta}</Box>}
               {meta !== undefined && provenance && (
                  <Box component="span" aria-hidden>
                     ·
                  </Box>
               )}
               {provenance && (
                  <Box
                     component="span"
                     sx={{
                        minWidth: 0,
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                     }}
                  >
                     {provenance}
                  </Box>
               )}
            </Box>
         )}
         <Collapse in={shownLayer !== undefined} timeout="auto" unmountOnExit>
            <Box
               id={cardId}
               data-testid="model-viewer-source-detail"
               data-layer={cardLayer ?? undefined}
               sx={{ ...PANE_CARD_SX, p: PANE_INSET, mb: 2 }}
            >
               {cardLayer === "access" && access && (
                  <AccessFacts access={access} showRules={showRules} />
               )}
            </Box>
         </Collapse>
      </Box>
   );
}

/** Lines a source's doc shows before "More". */
const DOC_LINES = 2;

/**
 * A source's doc under its name: at most two lines until the reader asks for the
 * rest, with `backticked` names set as code rather than showing the backticks. A
 * doc is written for the agent as much as the reader and runs long; the header's
 * job is to say what the source is at a glance.
 */
export function SourceDoc({
   doc,
   label = "description",
   testId = "model-viewer-source-doc",
}: {
   doc: string;
   /** What the More / Less button's accessible name calls the text. */
   label?: string;
   testId?: string;
}) {
   const [open, setOpen] = useState(false);
   const [overflows, setOverflows] = useState(false);
   const textRef = useRef<HTMLElement | null>(null);
   // Measured on every resize, not once: a panel narrowed after load clips more
   // lines, and without re-measuring the rest would be clipped with no More.
   useEffect(() => {
      const el = textRef.current;
      if (!el || open) return;
      const measure = () => setOverflows(el.scrollHeight > el.clientHeight + 1);
      measure();
      if (typeof ResizeObserver === "undefined") return;
      const observer = new ResizeObserver(measure);
      observer.observe(el);
      return () => observer.disconnect();
   }, [open, doc]);
   return (
      <Box data-testid={testId} sx={{ pb: 0.5 }}>
         <Box
            ref={textRef}
            sx={{
               fontSize: fontSizes.body,
               lineHeight: 1.5,
               color: "text.secondary",
               whiteSpace: "pre-line",
               ...(open
                  ? {}
                  : {
                       display: "-webkit-box",
                       WebkitBoxOrient: "vertical",
                       WebkitLineClamp: DOC_LINES,
                       overflow: "hidden",
                    }),
            }}
         >
            {withInlineCode(doc)}
         </Box>
         {(overflows || open) && (
            <Box
               component="button"
               type="button"
               onClick={() => setOpen(!open)}
               aria-expanded={open}
               aria-label={
                  open ? `Show less of the ${label}` : `Show the full ${label}`
               }
               sx={{
                  all: "unset",
                  cursor: "pointer",
                  fontSize: fontSizes.small,
                  color: "text.secondary",
                  textDecoration: "underline",
                  "&:hover": { color: "text.primary" },
                  "&:focus-visible": {
                     outline: "2px solid",
                     outlineColor: "primary.main",
                     outlineOffset: 2,
                  },
               }}
            >
               {open ? "Less" : "More"}
            </Box>
         )}
      </Box>
   );
}

/** `name` spans as code; everything else as written. */
export function withInlineCode(text: string): ReactNode[] {
   return text.split(/(`[^`\n]+`)/g).map((part, i) =>
      /^`[^`]+`$/.test(part) ? (
         <Box
            key={i}
            component="code"
            sx={{
               fontFamily: MONO_FONT_FAMILY,
               fontSize: "0.92em",
               color: "text.primary",
            }}
         >
            {part.slice(1, -1)}
         </Box>
      ) : (
         part
      ),
   );
}
