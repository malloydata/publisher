// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useState } from "react";
import { chooseWorkspace } from "../DashboardBuilder/documentSession";
import {
   useOptionalDocumentStorage,
   type DocumentStorage,
   type DocumentType,
} from "../DocumentStorage";

/**
 * The type a host keeps its copy of the report at `path` under, or undefined
 * when it keeps none. A host files a document by type as well as path (a
 * backend may key on both), and a report that is only a draft is not in the
 * package's listing, so its layout cannot be read from there: the editor has
 * to be opened with the type the draft was saved as, or it looks in the wrong
 * place and finds nothing. Asks the workspace the editor reads from.
 */
export async function findDraftType(
   storage: DocumentStorage,
   environmentName: string,
   packageName: string,
   path: string,
): Promise<DocumentType | undefined> {
   const workspace = chooseWorkspace(await storage.listWorkspaces(false));
   if (!workspace) return undefined;
   const key = `${environmentName}/${packageName}/${path}`;
   return (await storage.listDocuments(workspace)).find(
      (locator) => locator.path === key,
   )?.type;
}

/**
 * {@link findDraftType} for a component: `settled` is false until the host
 * has answered. Without a host store, or when not `enabled`, it is settled at
 * once with no type. A store that cannot answer settles with no type too: the
 * editor then opens and reports the read failure itself.
 */
export function useDraftType(
   environmentName: string,
   packageName: string,
   path: string,
   enabled = true,
): { type: DocumentType | undefined; settled: boolean } {
   const storage = useOptionalDocumentStorage()?.documentStorage;
   const asking = enabled && storage !== undefined;
   const [answer, setAnswer] = useState<{
      key: string;
      type: DocumentType | undefined;
   }>();
   const key = `${environmentName}/${packageName}/${path}`;
   useEffect(() => {
      if (!asking || !storage) return;
      let stale = false;
      findDraftType(storage, environmentName, packageName, path)
         .catch(() => undefined)
         .then((type) => {
            if (!stale) setAnswer({ key, type });
         });
      return () => {
         stale = true;
      };
   }, [asking, storage, environmentName, packageName, path, key]);
   if (!asking) return { type: undefined, settled: true };
   return answer?.key === key
      ? { type: answer.type, settled: true }
      : { type: undefined, settled: false };
}
