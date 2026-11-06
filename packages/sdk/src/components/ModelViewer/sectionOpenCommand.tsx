// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { ButtonBase, Tooltip } from "@mui/material";
import { IconCollapse, IconExpand } from "./icons";
import { createContext, useContext, useState } from "react";
import { focusRingSx } from "./clickableBox";

/**
 * A section's last "Expand" / "Collapse": every row in the section opens or
 * shuts when `n` changes, then each is the reader's own again. Undefined until
 * the reader asks, so a row starts shut.
 */
export interface SectionOpenCommand {
   open: boolean;
   n: number;
}

export const SectionOpenCommandContext = createContext<
   SectionOpenCommand | undefined
>(undefined);

/**
 * A row's open state, following its section's Expand / Collapse each time it is
 * asked and the reader's own toggle in between.
 */
export function useSectionOpen(): [
   boolean,
   (update: (open: boolean) => boolean) => void,
] {
   const [open, setOpen] = useState(false);
   const command = useContext(SectionOpenCommandContext);
   const [applied, setApplied] = useState(command?.n);
   if (command && command.n !== applied) {
      setApplied(command.n);
      setOpen(command.open);
   }
   return [open, setOpen];
}

/**
 * The Expand / Collapse control for a section: the chevron saying which way it goes, and
 * an accessible name saying what it opens, its visible words first and then the rows they
 * act on ("Expand all measures"), or why it can't. It changes what the section shows rather
 * than acting on the page, so it is a quiet text link alone on the line under the section's
 * title, in the caption voice, so it reads as part of the section's header.
 */
export function SectionExpandButton({
   allOpen,
   onToggle,
   noun,
   disabledReason,
}: {
   allOpen: boolean;
   onToggle: () => void;
   noun: readonly [string, string];
   disabledReason?: string;
}) {
   const chevron = allOpen ? (
      <IconCollapse size={16} aria-hidden />
   ) : (
      <IconExpand size={16} aria-hidden />
   );
   return (
      <Tooltip title={disabledReason ?? ""}>
         {/* The span keeps the tooltip reachable on a disabled button. */}
         <span>
            <ButtonBase
               disabled={!!disabledReason}
               onClick={onToggle}
               aria-label={
                  disabledReason ??
                  `${allOpen ? "Collapse" : "Expand"} all ${noun[1]}`
               }
               sx={{
                  // The caption's type under the title, so it reads as the header's own line.
                  font: "inherit",
                  color: "text.secondary",
                  gap: 0.25,
                  borderRadius: 1,
                  "&:hover": {
                     color: "text.primary",
                     textDecoration: "underline",
                  },
                  ...focusRingSx,
                  "&.Mui-disabled": { opacity: 0.5 },
               }}
            >
               {chevron}
               {allOpen ? "Collapse all" : "Expand all"}
            </ButtonBase>
         </span>
      </Tooltip>
   );
}

/**
 * A section's Expand / Collapse and the command it sends its rows. Always in
 * place on a page section, so the header never moves; disabled, with why, where
 * no row has anything to open.
 */
export function useSectionExpand({
   noun,
   disabledReason,
}: {
   noun: readonly [string, string];
   /** Set when no row in the section can open. */
   disabledReason?: string;
}) {
   const [command, setCommand] = useState<SectionOpenCommand | undefined>(
      undefined,
   );
   const allOpen = command?.open ?? false;
   const action = (
      <SectionExpandButton
         allOpen={allOpen}
         onToggle={() =>
            setCommand({ open: !allOpen, n: (command?.n ?? 0) + 1 })
         }
         noun={noun}
         disabledReason={disabledReason}
      />
   );
   return { command, action };
}
