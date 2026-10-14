// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box } from "@mui/material";
import type { ReactNode } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { ReportHeader, TextTileFrame } from "../Report/ReportPage";
import { InlineMarkdown } from "./InlineMarkdown";
import { InlineText } from "./InlineText";
import { selectionSx } from "./TileFrame";
import type { ReportEditorHandle } from "./useReportEditor";

/** How far the description's selection ring stands off its text. */
const DESCRIPTION_RING_OFFSET_PX = 12;

/**
 * The reader's {@link ReportHeader}, with its title and description edited
 * where they stand and the builder's actions on the title's line: the same
 * header in the same place, so editing a page does not move it.
 */
export function BuilderHeader({
   editor,
   documentSlug,
   actions,
   descriptionSelected,
   selectDescription,
}: {
   editor: ReportEditorHandle;
   /** The name a reader's view titles an untitled document with. */
   documentSlug: string | undefined;
   actions: ReactNode;
   descriptionSelected: boolean;
   selectDescription: () => void;
}) {
   const { theme } = usePublisherTheme();
   return (
      <ReportHeader
         actions={actions}
         title={
            <InlineText
               value={editor.document.title}
               placeholder={
                  // What a reader's view falls back to: the file's own name.
                  documentSlug ?? "Untitled report"
               }
               ariaLabel="Report title"
               onCommit={(next) =>
                  editor.update((draft) => {
                     draft.title = next;
                  })
               }
            />
         }
         description={
            // Selected as a tile is: one selection on the page, tile or
            // description.
            <Box
               role="group"
               aria-label="Description"
               aria-current={descriptionSelected}
               onPointerDown={selectDescription}
               onFocus={selectDescription}
            >
               <TextTileFrame
                  sx={{
                     // Anchors the ring the bare description draws around itself.
                     position: "relative",
                     overflow: "visible",
                     // Wider than a grid tile's ring: the description is not in
                     // the grid, so its ring is not held inside a gutter, and
                     // a paragraph needs more room round it than a tile's prose.
                     ...selectionSx(theme, {
                        selected: descriptionSelected,
                        bare: true,
                        ringOffset: DESCRIPTION_RING_OFFSET_PX,
                     }),
                  }}
               >
                  <InlineMarkdown
                     markdown={editor.document.description ?? ""}
                     placeholder="Add a description"
                     onCommit={(next) =>
                        editor.update((draft) => {
                           if (next.trim() === "") delete draft.description;
                           else draft.description = next;
                        })
                     }
                  />
               </TextTileFrame>
            </Box>
         }
      />
   );
}
