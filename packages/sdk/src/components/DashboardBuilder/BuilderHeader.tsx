// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Stack, Typography } from "@mui/material";
import type { ReactNode } from "react";
import { BARE_DESCRIPTION_MARGIN_PX } from "../Dashboard/DashboardGrid";
import { TileCard, type TileChrome } from "../Dashboard/TileCard";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { InlineMarkdown } from "./InlineMarkdown";
import { InlineText } from "./InlineText";
import { selectionSx } from "./TileFrame";
import type { DashboardEditor } from "./useDashboardEditor";

/**
 * The document's title, with the builder's actions on its line, and its
 * description beneath: both edited where they stand, as the reader sees them.
 */
export function BuilderHeader({
   editor,
   notebook,
   documentSlug,
   chrome,
   actions,
   descriptionSelected,
   selectDescription,
}: {
   editor: DashboardEditor;
   notebook: boolean;
   /** The name a reader's view titles an untitled document with. */
   documentSlug: string | undefined;
   chrome: TileChrome;
   actions: ReactNode;
   descriptionSelected: boolean;
   selectDescription: () => void;
}) {
   const { theme } = usePublisherTheme();
   return (
      <>
         <Stack direction="row" sx={{ alignItems: "center", gap: 2 }}>
            <Typography
               variant="h5"
               // The host theme's h5 weight, as the reader's title takes it.
               sx={{ flex: 1, minWidth: 0 }}
            >
               <InlineText
                  value={editor.document.title}
                  placeholder={
                     // What a reader's view falls back to: the file's own name.
                     documentSlug ??
                     (notebook ? "Untitled notebook" : "Untitled dashboard")
                  }
                  ariaLabel={notebook ? "Notebook title" : "Dashboard title"}
                  onCommit={(next) =>
                     editor.update((draft) => {
                        draft.title = next;
                     })
                  }
               />
            </Typography>
            {/* Overhangs the title's line rather than heightening it, so
             the title sits where the reader's view puts it. */}
            <Box sx={{ my: "-4px" }}>{actions}</Box>
         </Stack>
         {/* The description, drawn as a text block is in this document — a card
          on a dashboard, bare on a notebook, as the reader draws it —
          lifting on hover and selected as a tile is: one selection on the
          page, tile or description. */}
         <Box
            role="group"
            aria-label="Description"
            aria-current={descriptionSelected}
            onPointerDown={selectDescription}
            onFocus={selectDescription}
            sx={
               chrome === "none"
                  ? { my: `${BARE_DESCRIPTION_MARGIN_PX}px` }
                  : undefined
            }
         >
            <TileCard
               chrome={chrome}
               kind="text"
               sx={{
                  // Anchors the ring a bare description draws around itself.
                  position: "relative",
                  overflow: "visible",
                  ...selectionSx(theme, {
                     selected: descriptionSelected,
                     bare: chrome === "none",
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
            </TileCard>
         </Box>
      </>
   );
}
