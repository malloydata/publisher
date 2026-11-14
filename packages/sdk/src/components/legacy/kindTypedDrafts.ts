// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ReportLayout } from "../Package/documentLocation";
import { layoutOfKind } from "./kindAdapter";

/** The key the browser store used when a document's type was part of its key: `publisher:document:<dashboard|notebook>:<path>`. */
const KIND_TYPED_KEY = /^publisher:document:(dashboard|notebook):(.+)$/;

/**
 * Move the browser's drafts saved under the old type-keyed scheme to the
 * path-keyed one, so a draft saved before the change is still found. Each
 * becomes the document at its path, its old type read as its layout, and the
 * old key goes. `adopt` writes one and answers false when its path already
 * holds a document: a path saved under both old types keeps the first moved,
 * and the other is left where it was rather than overwritten.
 */
export function migrateKindTypedDrafts(
   storage: Storage,
   adopt: (path: string, layout: ReportLayout, content: string) => boolean,
): void {
   const keys: string[] = [];
   for (let i = 0; i < storage.length; i++) {
      const key = storage.key(i);
      if (key !== null && KIND_TYPED_KEY.test(key)) keys.push(key);
   }
   for (const key of keys) {
      const [, kind, path] = KIND_TYPED_KEY.exec(key) as RegExpExecArray;
      const content = storage.getItem(key);
      if (content === null) continue;
      if (adopt(path, layoutOfKind(kind as "dashboard" | "notebook"), content))
         storage.removeItem(key);
   }
}
