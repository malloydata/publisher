// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useState } from "react";
import {
   isDocumentNotFound,
   type DocumentStorage,
   type Workspace,
} from "../DocumentStorage";
import { locatorFor } from "../DocumentCreate/documentPath";
import type { DocumentKind } from "./document";
import { chooseWorkspace, storageErrorMessage } from "./documentSession";

/**
 * The host's copy of the document, if one was saved earlier: which workspace
 * holds it, its text, and whether the read has answered yet.
 *
 * Read once per document, from the host's {@link DocumentStorage}. A host
 * without one has nothing to read, so the answer is in at once.
 */
export function useHostCopy({
   storage,
   kind,
   notebook,
   environmentName,
   packageName,
   modelPath,
}: {
   storage: DocumentStorage | undefined;
   kind: DocumentKind;
   notebook: boolean;
   environmentName: string;
   packageName: string;
   modelPath: string;
}) {
   // The host's copy, if one was saved earlier.
   const [workspace, setWorkspace] = useState<Workspace | undefined>(undefined);
   const [draft, setDraft] = useState<string | undefined>(undefined);
   const [draftChecked, setDraftChecked] = useState(storage === undefined);
   // Whether a copy was there when the editor opened: only that one is
   // offered. A copy this session saves is not "edits from an earlier visit".
   const [offered, setOffered] = useState(false);
   // A read the backend could not answer, which is not the same as no
   // document. Held rather than swallowed, because "there is nothing saved"
   // is what makes overwriting the record look safe.
   const [readFailure, setReadFailure] = useState<string | undefined>(
      undefined,
   );
   useEffect(() => {
      if (!storage) return;
      let stale = false;
      setReadFailure(undefined);
      (async () => {
         try {
            // Every workspace, not only the writeable ones: a reader who
            // cannot write to the record still has to be shown the record.
            // Asking for writeable only would hide it and fall back to the
            // package, which on a server that takes writes means publishing a
            // deploy of the record over the record.
            const chosen = chooseWorkspace(await storage.listWorkspaces(false));
            if (stale) return;
            setWorkspace(chosen);
            // Only a notebook's record is read: a copy beside the package is never offered back.
            if (chosen !== undefined && (!notebook || chosen.authoritative)) {
               const text = await storage
                  .getDocument(
                     locatorFor(
                        kind,
                        chosen.name,
                        environmentName,
                        packageName,
                        modelPath,
                     ),
                  )
                  // Absence is the ordinary first visit, not a failure.
                  .catch((error: unknown) => {
                     if (isDocumentNotFound(error)) return undefined;
                     throw error;
                  });
               if (stale) return;
               setDraft(text);
               setOffered(text !== undefined);
            }
         } catch (error) {
            if (stale) return;
            setReadFailure(storageErrorMessage(error));
         }
         if (!stale) setDraftChecked(true);
      })();
      return () => {
         stale = true;
      };
   }, [storage, kind, notebook, environmentName, packageName, modelPath]);

   return {
      workspace,
      draft,
      setDraft,
      draftChecked,
      offered,
      setOffered,
      readFailure,
   };
}
