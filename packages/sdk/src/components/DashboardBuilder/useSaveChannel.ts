// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { QueryClient } from "@tanstack/react-query";
import {
   useCallback,
   useState,
   type Dispatch,
   type MutableRefObject,
   type SetStateAction,
} from "react";
import { modelResultsKey } from "../../hooks/useQueryResult";
import { encodeResourceUri } from "../../utils/formatting";
import {
   isDocumentNotFound,
   type DocumentStorage,
   type Workspace,
} from "../DocumentStorage";
import { locatorFor } from "../DocumentCreate/documentPath";
import type { useServer } from "../ServerProvider";
import type { DocumentKind } from "./document";
import {
   expectedHashFor,
   saveTarget,
   storageErrorMessage,
   writePackageFile,
} from "./documentSession";
import type { OpenedDocument } from "./useOpenedDocument";

/**
 * Where Save writes, and the write itself: into the host's store, or into the
 * package, and what each write does to the editor's idea of which text is
 * open, which was seen, and what the next save is spliced against.
 */
export function useSaveChannel({
   storage,
   workspace,
   kind,
   notebook,
   environmentName,
   packageName,
   modelPath,
   versionId,
   apiClients,
   queryClient,
   mutable,
   textHeld,
   readFailure,
   authoritative,
   resume,
   setResume,
   setOpened,
   setDraft,
   setOffered,
   setSeen,
   setWrote,
   savedHashRef,
   packageBaseRef,
   fetchedAtRef,
}: {
   storage: DocumentStorage | undefined;
   workspace: Workspace | undefined;
   kind: DocumentKind;
   notebook: boolean;
   environmentName: string;
   packageName: string;
   modelPath: string;
   versionId: string | undefined;
   apiClients: ReturnType<typeof useServer>["apiClients"];
   queryClient: QueryClient;
   /** Whether the server takes writes; unknown while `/status` loads. */
   mutable: boolean | undefined;
   /** A document held as text is saved only to the host's record, never into the package. */
   textHeld: boolean;
   readFailure: string | undefined;
   authoritative: boolean;
   resume: boolean | undefined;
   setResume: Dispatch<SetStateAction<boolean | undefined>>;
   setOpened: Dispatch<SetStateAction<OpenedDocument | undefined>>;
   setDraft: Dispatch<SetStateAction<string | undefined>>;
   setOffered: Dispatch<SetStateAction<boolean>>;
   setSeen: Dispatch<SetStateAction<string | undefined>>;
   setWrote: Dispatch<
      SetStateAction<{ text: string; onFetch: number } | undefined>
   >;
   savedHashRef: MutableRefObject<string | undefined>;
   packageBaseRef: MutableRefObject<string | undefined>;
   fetchedAtRef: MutableRefObject<number>;
}) {
   const locator =
      workspace === undefined
         ? undefined
         : locatorFor(
              kind,
              workspace.name,
              environmentName,
              packageName,
              modelPath,
           );
   // Into the host's store: the record when the host says so, and otherwise a
   // copy kept beside the package.
   const saveToStorage = useCallback(
      async (source: string) => {
         if (!storage || !locator)
            throw new Error(
               "This host keeps no documents, so there is nowhere to save.",
            );
         await storage.saveDocument(locator, source);
         // The builder keeps its history and its text; what it holds is now
         // this, not the text it was opened with.
         setOpened((previous) => previous && { ...previous, source });
         setDraft(source);
         // Whenever the copy IS the channel — the record, or a copy the reader
         // resumed — the write moved that channel, and its own text is not an
         // incoming version. A copy written while the package is what is open
         // leaves the package where it was, so nothing there has moved.
         if (authoritative || resume === true) setSeen(source);
         // Saving without choosing is choosing the package file.
         if (!authoritative) setResume((chosen) => chosen ?? false);
      },
      [
         storage,
         locator,
         authoritative,
         resume,
         setOpened,
         setDraft,
         setSeen,
         setResume,
      ],
   );
   // A supersede that did not happen, left where a reader can see it: the copy
   // is still there and will be offered again on the next visit.
   const [supersedeFailure, setSupersedeFailure] = useState<string | undefined>(
      undefined,
   );
   // Into the package itself, when the server takes writes: compile-checked,
   // written atomically and reloaded there, refused if the file changed since
   // it was opened. A copy of the same file kept beside it is superseded by it.
   const saveToPackage = useCallback(
      async (source: string) => {
         // The hash of the package file this editor opened against, never of
         // the latest fetch. A version held back while the reader keeps
         // editing moves the fetch past that file, and a hash taken from it
         // would match, be accepted, and overwrite the change the reader was
         // just told about.
         const expectedHash = await expectedHashFor(
            savedHashRef.current,
            packageBaseRef.current,
         );
         if (expectedHash === undefined)
            throw new Error("The package file is still loading; try again.");
         const modelKey = [
            "dashboard-editor-model",
            environmentName,
            packageName,
            modelPath,
            versionId,
            notebook,
         ];
         await writePackageFile({
            apiClients,
            queryClient,
            environmentName,
            packageName,
            modelPath,
            source,
            expectedHash,
            // A refused write usually means the file moved: fetching it offers the reader that version instead of re-saving against a base the server keeps rejecting.
            invalidateOnError: [modelKey],
            afterWrite: async (contentHash) => {
               setOpened((previous) => previous && { ...previous, source });
               setWrote({ text: source, onFetch: fetchedAtRef.current });
               setSeen(source);
               savedHashRef.current = contentHash;
               setSupersedeFailure(undefined);
               if (storage && locator) {
                  try {
                     await storage.deleteDocument(locator);
                     setDraft(undefined);
                     setOffered(false);
                  } catch (error) {
                     // Only absence means the copy is gone; any other rejection leaves it there, so the state must keep saying so.
                     if (isDocumentNotFound(error)) {
                        setDraft(undefined);
                        setOffered(false);
                     } else setSupersedeFailure(storageErrorMessage(error));
                  }
               }
               // The package is what is open now even if the copy survived the supersede; reading the channel off a stale copy would put the builder back on the text this save replaced.
               setResume(false);
            },
            invalidate: [
               { queryKey: modelKey, wait: true },
               // A cached result is keyed on the query text, not the file, so a changed chart would otherwise be drawn from the old rows.
               {
                  queryKey: modelResultsKey({
                     environmentName,
                     packageName,
                     versionId,
                     modelPath,
                  }),
               },
               { queryKey: ["dashboard-editor-manifest"] },
               { queryKey: ["dashboards"] },
               { queryKey: ["dashboard"] },
               // The notebook viewer keys its read on the resource URI.
               ...(notebook
                  ? [
                       {
                          queryKey: [
                             encodeResourceUri({
                                environmentName,
                                packageName,
                                modelPath,
                             }),
                          ],
                       },
                    ]
                  : []),
            ],
         });
      },
      [
         apiClients,
         environmentName,
         packageName,
         modelPath,
         versionId,
         notebook,
         storage,
         locator,
         queryClient,
         savedHashRef,
         packageBaseRef,
         fetchedAtRef,
         setOpened,
         setWrote,
         setSeen,
         setDraft,
         setOffered,
         setResume,
      ],
   );
   const canWriteWorkspace = workspace?.writeable === true;
   // Unknown while `/status` loads, and a package write needs a yes.
   const takesWrites = mutable === true && !textHeld;
   const { savesTo, pinnedPackageSave, writer } = saveTarget({
      authoritative,
      mutable: takesWrites,
      ...(versionId !== undefined ? { versionId } : {}),
      // A notebook reads back only the record, so a copy anywhere else would be a write nobody sees.
      canStore:
         !!storage &&
         !!locator &&
         canWriteWorkspace &&
         (!notebook || authoritative),
      readFailed: readFailure !== undefined,
   });
   const save =
      writer === "storage"
         ? saveToStorage
         : writer === "package"
           ? saveToPackage
           : undefined;

   return {
      save,
      savesTo,
      writer,
      pinnedPackageSave,
      takesWrites,
      supersedeFailure,
   };
}
