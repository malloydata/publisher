// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useState } from "react";
import { chooseWorkspace } from "../ReportBuilder/documentSession";
import { draftKey } from "../DocumentCreate/documentPath";
import {
   useOptionalDocumentStorage,
   type DocumentStorage,
} from "../DocumentStorage";
import type { ReportLayout } from "./documentLocation";

/**
 * The layout of the host's copy of the report at `path`, as its store
 * recorded it on the last save, or undefined when it keeps no copy. A report
 * that is only a draft is not in the package's listing, so this is the one
 * place its layout can be read before the editor opens its text; the editor
 * then follows the text. Asks the workspace the editor reads from.
 */
export async function findDraftLayout(
   storage: DocumentStorage,
   environmentName: string,
   packageName: string,
   path: string,
): Promise<ReportLayout | undefined> {
   const workspace = chooseWorkspace(await storage.listWorkspaces(false));
   if (!workspace) return undefined;
   const key = draftKey(environmentName, packageName, path);
   return (await storage.listDocuments(workspace)).find(
      (locator) => locator.path === key,
   )?.type;
}

/**
 * {@link findDraftLayout} for a component: `settled` is false until the host
 * has answered. Without a host store, or when not `enabled`, it is settled at
 * once with no layout. A store that cannot answer settles with no layout too:
 * the editor then opens and reports the read failure itself.
 */
export function useDraftLayout(
   environmentName: string,
   packageName: string,
   path: string,
   enabled = true,
): { layout: ReportLayout | undefined; settled: boolean } {
   const storage = useOptionalDocumentStorage()?.documentStorage;
   const asking = enabled && storage !== undefined;
   const [answer, setAnswer] = useState<{
      key: string;
      layout: ReportLayout | undefined;
   }>();
   const key = draftKey(environmentName, packageName, path);
   useEffect(() => {
      if (!asking || !storage) return;
      let stale = false;
      findDraftLayout(storage, environmentName, packageName, path)
         .catch(() => undefined)
         .then((layout) => {
            if (!stale) setAnswer({ key, layout });
         });
      return () => {
         stale = true;
      };
   }, [asking, storage, environmentName, packageName, path, key]);
   if (!asking) return { layout: undefined, settled: true };
   return answer?.key === key
      ? { layout: answer.layout, settled: true }
      : { layout: undefined, settled: false };
}
