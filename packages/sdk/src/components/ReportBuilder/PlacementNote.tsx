// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Alert, Snackbar } from "@mui/material";
import { useCallback, useState, type ReactNode } from "react";

/** What the note says, before the first move gives every tile a place. */
export const PLACEMENT_NOTE =
   "Moving a tile gives every tile on this report a fixed place, where it sits now, so the rest stop reflowing. Undo puts them back.";

const SEEN_KEY = "publisher.builder.placement-note-seen";

const seen = (): boolean => {
   try {
      return globalThis.localStorage?.getItem(SEEN_KEY) === "1";
   } catch {
      return false;
   }
};
const remember = () => {
   try {
      globalThis.localStorage?.setItem(SEEN_KEY, "1");
   } catch {
      // A browser that keeps nothing shows the note again next time.
   }
};

/**
 * The one-time note a grid report shows as its first move begins: on a grid
 * no tile has a place yet, and the first move places every tile where it is
 * drawn (docs/reports.md, "a move is free placement"). Said once per browser,
 * as the gesture starts, so the change is never a surprise afterwards.
 */
export function usePlacementNote(): {
   /** Call as a gesture that will place every tile begins. */
   warn: () => void;
   note: ReactNode;
} {
   const [open, setOpen] = useState(false);
   const warn = useCallback(() => {
      if (seen()) return;
      remember();
      setOpen(true);
   }, []);
   const note = (
      <Snackbar
         open={open}
         onClose={(_, reason) => {
            if (reason !== "clickaway") setOpen(false);
         }}
         autoHideDuration={12_000}
         anchorOrigin={{ vertical: "bottom", horizontal: "center" }}
      >
         <Alert severity="info" onClose={() => setOpen(false)}>
            {PLACEMENT_NOTE}
         </Alert>
      </Snackbar>
   );
   return { warn, note };
}
