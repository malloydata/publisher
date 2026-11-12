// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, ButtonBase, Popover, Typography } from "@mui/material";
import type { ReactNode } from "react";
import { AccessPills, accessRowLabel } from "./AccessChips";
import { focusRingSx } from "./clickableBox";
import { withInlineCode } from "./HeaderBar";
import { IconColumn } from "./icons";
import { MODEL_SOURCES_HELP } from "./modelHeaderPresentation";
import { hasAccessRules, type SourceAccess } from "./sourceAccess";
import type { PickerDescription } from "./sourcePicker";
import { StatusPill } from "./StatusPill";
import { CaptionText } from "./text";
import { fontSizes, MONO_FONT_FAMILY, roles } from "./theme";
import { ACCENT } from "./tokens";

/** A source as this dropdown needs it: what to show. */
export interface SourceNavItem {
   name: string;
   /** The row's description, two lines at most: the source's `#(doc)`, else a
    *  host's summary, marked as generated (`pickerDescription`). */
   description?: PickerDescription;
   /** Hold the description's line: the source has no doc, and a host's
    *  summaries are still being read, so one may arrive. */
   descriptionPending?: boolean;
   /** "16 views · 37 fields · 4 joins" (`sourceMetaLine`), from the compiled model. */
   meta: string;
   /** The source the model opens on, when it has the most views: a quiet token. */
   mostViews?: boolean;
   /** The source's access declarations. A restricted source stays listed (a
    *  reader learns it exists, and that it is restricted), marked beside its
    *  name. */
   access?: SourceAccess;
   /** A host's chips after the source's name (`describePickerSource`). */
   status?: ReactNode;
}

/** The header's line on the package's root `index.malloy`, whose sources are the curated set,
 *  when they are listed in its export order. */
export const CURATED_SOURCES_HELP =
   "This package's curated sources, in the order its index.malloy exports them.";

/** The same, when the file's export order is unknown (its text withheld): it claims no order. */
export const PUBLIC_SOURCES_HELP = "This package's public sources.";

/** The words a row's accessible name adds for its tokens. */
function tokenRowLabel(source: SourceNavItem): string {
   return source.mostViews ? ", most views" : "";
}

/**
 * The sources of a model, as a dropdown hanging off the source picker pill
 * beside the title ({@link import("./SourcesNav").SourcesNav}). A dropdown
 * rather than a resident switcher: a model has a handful of sources and a
 * reader settles into one, so the page stays in view and it closes on a click
 * away.
 *
 * <p>Each row is what a reader needs to choose the source an analysis starts
 * from: its name, its pills (the default's quiet **Most views**, access), two
 * lines of description, and one line of counts, in `orderPickerSources`' order.
 */
export function SourcesDropdown({
   anchorEl,
   onClose,
   sources,
   currentName,
   onPickSource,
   curated = false,
   exportOrdered = false,
}: {
   /** The button this dropdown hangs from; null while closed. */
   anchorEl: HTMLElement | null;
   onClose: () => void;
   sources: ReadonlyArray<SourceNavItem>;
   currentName: string;
   onPickSource: (name: string) => void;
   /** The model is the package's root `index.malloy`: its header says these are the curated set. */
   curated?: boolean;
   /** The curated set is listed in the order its index.malloy exports it, so the header says so. */
   exportOrdered?: boolean;
}) {
   const open = Boolean(anchorEl);
   return (
      <Popover
         open={open}
         anchorEl={anchorEl}
         onClose={onClose}
         // Growing rightward from the pill beside the title.
         anchorOrigin={{ vertical: "bottom", horizontal: "left" }}
         transformOrigin={{ vertical: "top", horizontal: "left" }}
         slotProps={{
            paper: {
               sx: {
                  width: 460,
                  maxWidth: "calc(100vw - 32px)",
                  maxHeight: "min(70vh, 560px)",
                  display: "flex",
                  flexDirection: "column",
                  borderRadius: "10px",
               },
               "data-testid": "model-sources-dropdown",
            } as object,
         }}
      >
         <Box
            sx={{
               px: 2,
               pt: 1.5,
               pb: 1,
               borderBottom: 1,
               borderColor: "divider",
            }}
         >
            <Typography variant="subtitle2" component="h2" sx={{ m: 0 }}>
               Sources
            </Typography>
            <CaptionText component="p" sx={{ m: 0 }}>
               {curated
                  ? exportOrdered
                     ? CURATED_SOURCES_HELP
                     : PUBLIC_SOURCES_HELP
                  : MODEL_SOURCES_HELP}
            </CaptionText>
         </Box>
         <Box sx={{ overflowY: "auto", py: 0.5 }}>
            {sources.length === 0 ? (
               <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ py: 4, textAlign: "center" }}
               >
                  This model defines no sources.
               </Typography>
            ) : (
               sources.map((source) => {
                  const current = source.name === currentName;
                  return (
                     <ButtonBase
                        key={`source:${source.name}`}
                        data-item-name={source.name}
                        aria-label={`Open source ${source.name}${tokenRowLabel(source)}${accessRowLabel(source.access)}`}
                        aria-current={current || undefined}
                        onClick={() => {
                           // Selecting with the dropdown still mounted would leave
                           // it covering the source it just opened.
                           onClose();
                           onPickSource(source.name);
                        }}
                        sx={{
                           display: "flex",
                           width: "100%",
                           alignItems: "flex-start",
                           justifyContent: "flex-start",
                           textAlign: "left",
                           gap: 1.25,
                           px: 2,
                           py: 1,
                           bgcolor: current
                              ? roles.background.muted
                              : undefined,
                           "&:hover": {
                              bgcolor: current
                                 ? roles.background.strong
                                 : roles.background.muted,
                           },
                           ...focusRingSx,
                        }}
                     >
                        <Box
                           component="span"
                           aria-hidden
                           sx={{
                              color: ACCENT.steel.main,
                              display: "inline-flex",
                              pt: "2px",
                           }}
                        >
                           <IconColumn size={16} />
                        </Box>
                        <Box component="span" sx={{ minWidth: 0, flex: 1 }}>
                           <Box
                              component="span"
                              sx={{
                                 display: "flex",
                                 alignItems: "center",
                                 flexWrap: "wrap",
                                 gap: 1,
                              }}
                           >
                              <Box
                                 component="span"
                                 sx={{
                                    fontFamily: MONO_FONT_FAMILY,
                                    fontSize: fontSizes.body,
                                    fontWeight: 500,
                                    color: "text.primary",
                                 }}
                              >
                                 {source.name}
                              </Box>
                              {source.mostViews && (
                                 <StatusPill
                                    tone="quiet"
                                    sx={{ alignSelf: "center" }}
                                 >
                                    {MOST_VIEWS_LABEL}
                                 </StatusPill>
                              )}
                              {hasAccessRules(source.access) && (
                                 <AccessPills access={source.access} />
                              )}
                              {source.status}
                           </Box>
                           {(source.description ||
                              source.descriptionPending) && (
                              <Box
                                 component="span"
                                 sx={{
                                    display: "-webkit-box",
                                    WebkitBoxOrient: "vertical",
                                    WebkitLineClamp: 2,
                                    overflow: "hidden",
                                    mt: 0.25,
                                    fontSize: fontSizes.small,
                                    color: "text.secondary",
                                    minHeight: !source.description
                                       ? "1.4em"
                                       : undefined,
                                 }}
                              >
                                 {source.description?.generated && (
                                    <StatusPill
                                       tone="quiet"
                                       data-testid="model-sources-summary-generated"
                                       sx={{
                                          mr: 0.75,
                                          verticalAlign: "baseline",
                                       }}
                                    >
                                       Generated
                                    </StatusPill>
                                 )}
                                 {source.description &&
                                    withInlineCode(source.description.text)}
                              </Box>
                           )}
                           <CaptionText
                              data-testid="model-sources-row-meta"
                              sx={{ display: "block", mt: 0.25 }}
                           >
                              {source.meta}
                           </CaptionText>
                        </Box>
                     </ButtonBase>
                  );
               })
            )}
         </Box>
      </Popover>
   );
}

/** The quiet token on the source a model opens on, when it has the most views. */
export const MOST_VIEWS_LABEL = "Most views";
