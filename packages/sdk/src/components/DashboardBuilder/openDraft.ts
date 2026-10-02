// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   createContext,
   useContext,
   useEffect,
   useRef,
   type MutableRefObject,
} from "react";

/** Where an inline field tells the builder it holds text that is not in the document yet. */
export interface OpenDraftSink {
   setDirty: (dirty: boolean) => void;
   /** Holds a function that commits the open draft; true when it did, false when the draft was refused. */
   commitRef: MutableRefObject<(() => boolean) | undefined>;
}

export const OpenDraftContext = createContext<OpenDraftSink | undefined>(
   undefined,
);

/** Reports an open field's draft as unsaved while `dirty`, and lets a save commit it first. */
export function useReportOpenDraft(dirty: boolean, commit: () => boolean) {
   const sink = useContext(OpenDraftContext);
   const latestCommit = useRef(commit);
   latestCommit.current = commit;
   useEffect(() => {
      if (!sink || !dirty) return;
      const mine = () => latestCommit.current();
      sink.commitRef.current = mine;
      return () => {
         if (sink.commitRef.current === mine)
            sink.commitRef.current = undefined;
      };
   }, [sink, dirty]);
   // A field that never held a dirty draft stays silent, or mounting would clear another field's.
   const reported = useRef(false);
   useEffect(() => {
      if (!dirty && !reported.current) return;
      reported.current = dirty;
      sink?.setDirty(dirty);
   }, [sink, dirty]);
   useEffect(
      () => () => {
         if (reported.current) sink?.setDirty(false);
      },
      [sink],
   );
}
