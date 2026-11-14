// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { OpenDraftSink } from "./openDraft";

/**
 * The text field open somewhere on the page, as the builder's save sees it.
 *
 * An inline field reports itself through `openDraft` (provided as
 * `OpenDraftContext`): whether it holds typing not yet in the document, and
 * how to commit it. `draftDirty` counts that typing as unsaved work, and
 * `prepare` commits it before a save reads the document.
 */
export function useOpenDraftCommit({
   editorDirty,
}: {
   /** The editor's own dirty flag: a commit landing changes it, which is when the waiting save runs. */
   editorDirty: boolean;
}) {
   const [draftDirty, setDraftDirty] = useState(false);
   const draftCommit = useRef<(() => boolean) | undefined>(undefined);
   const afterCommit = useRef<(() => void) | undefined>(undefined);
   const openDraft = useMemo<OpenDraftSink>(
      () => ({ setDirty: setDraftDirty, commitRef: draftCommit }),
      [],
   );
   // A save reads the committed document, so an open draft is committed first and the save runs once that has rendered.
   const prepare = useCallback(
      (run: () => Promise<void> | void): Promise<void> | void => {
         if (!draftCommit.current) return run();
         if (!draftCommit.current()) return;
         return new Promise<void>((resolve) => {
            afterCommit.current = () => resolve(run());
         });
      },
      [],
   );
   useEffect(() => {
      if (draftDirty || !afterCommit.current) return;
      const run = afterCommit.current;
      afterCommit.current = undefined;
      run();
   }, [draftDirty, editorDirty]);

   return { draftDirty, openDraft, prepare };
}
