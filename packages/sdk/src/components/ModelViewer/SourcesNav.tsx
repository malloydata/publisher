// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useState } from "react";
import { IconDropdown } from "./icons";
import { PillButton } from "./ui";
import { SourcesDropdown, type SourceNavItem } from "./SourcesDropdown";

/**
 * The model's source picker, beside the source's h1 ({@link import("./HeaderBar").HeaderBar})
 * as the package page's version pill sits beside the package's ("v1.0.1 ▾"): the same
 * `PillButton`, with the same props, so the two read as one family. Its face is the count —
 * "2 sources ▾", or "4 public sources ▾" on the package's root `index.malloy`, whose sources
 * are what the package publishes — and it opens the {@link SourcesDropdown} . The caller draws it only when there is something to switch to:
 * more than one source. A model's named queries are rows on the page (QueriesGroup).
 *
 * <p>Reaching for it (pointer over, focus, a click) calls `onWarm`, which is when the
 * picker's summaries are asked for (§7a).
 */
export function SourcesNav({
   sources,
   currentName,
   countLabel,
   onPickSource,
   curated = false,
   exportOrdered = false,
   onWarm,
}: {
   sources: ReadonlyArray<SourceNavItem>;
   currentName: string;
   /** The pill's face: "2 sources", "4 public sources" (`sourceCountLabel`). */
   countLabel: string;
   onPickSource: (name: string) => void;
   /** The model is the package's root `index.malloy` (see SourcesDropdown). */
   curated?: boolean;
   /** Its sources are listed in the order its index.malloy exports them (see SourcesDropdown). */
   exportOrdered?: boolean;
   /** The reader is reaching for the picker: start what its rows will need. */
   onWarm?: () => void;
}) {
   const [anchor, setAnchor] = useState<HTMLElement | null>(null);
   return (
      <>
         {/* The package page's version pill, prop for prop (PackagePage's title). */}
         <PillButton
            size="small"
            variant="outlined"
            onClick={(e) => {
               onWarm?.();
               setAnchor(e.currentTarget);
            }}
            endIcon={<IconDropdown size={20} />}
            // The visible text first (§7 "A visible label is contained in the accessible
            // name"), then what it means.
            aria-label={`${countLabel}. Showing ${currentName}, one of the ${countLabel} in this model`}
            aria-haspopup="dialog"
            aria-expanded={Boolean(anchor)}
            onPointerEnter={onWarm}
            onFocus={onWarm}
            sx={{ ml: 1, transition: "none" }}
         >
            {countLabel}
         </PillButton>
         <SourcesDropdown
            anchorEl={anchor}
            onClose={() => setAnchor(null)}
            sources={sources}
            currentName={currentName}
            onPickSource={onPickSource}
            curated={curated}
            exportOrdered={exportOrdered}
         />
      </>
   );
}
