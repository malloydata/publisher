// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useRef, useState } from "react";

/** A save waiting on the reader's answer: go ahead, or stop without writing. */
export interface ConversionConfirm {
   go: () => void;
   stop: () => void;
}

/**
 * Asking before the save that converts a cell-format notebook.
 *
 * `prepareSave` is `prepare` behind that question. While `confirmConversion`
 * is set the question is open; answering it runs the save, or drops it.
 */
export function useConversionConfirm({
   pendingOpen,
   prepare,
}: {
   /** Whether the document opened unsaved, as a conversion does. */
   pendingOpen: boolean | undefined;
   /** What runs before every save once the reader has said yes. */
   prepare: (run: () => Promise<void> | void) => Promise<void> | void;
}) {
   // A save that would convert a cell-format notebook, waiting on the reader's yes.
   const [confirmConversion, setConfirmConversion] = useState<
      ConversionConfirm | undefined
   >(undefined);
   // The first save of a cell-format notebook rewrites the file in the tile
   // layout, which the builder cannot take back: it asks before it writes.
   const pendingOpenRef = useRef(pendingOpen);
   pendingOpenRef.current = pendingOpen;
   const prepareSave = useCallback(
      (run: () => Promise<void> | void): Promise<void> | void => {
         if (!pendingOpenRef.current) return prepare(run);
         return new Promise<void>((resolve) =>
            setConfirmConversion({
               go: () => {
                  setConfirmConversion(undefined);
                  resolve(prepare(run) as Promise<void> | undefined);
               },
               stop: () => {
                  setConfirmConversion(undefined);
                  resolve();
               },
            }),
         );
      },
      [prepare],
   );

   return { confirmConversion, prepareSave };
}
