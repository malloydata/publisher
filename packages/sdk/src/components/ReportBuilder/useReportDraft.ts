// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useEffect, useRef, useState } from "react";
import type { DashboardDocument } from "./document";
import type { ReportEditorHandle } from "./useReportEditor";

/**
 * Unsaved edits, kept in this browser while they are unsaved, so leaving the
 * builder — a navigation, a closed tab, a crash — never loses them, and the
 * one action that writes the file can be Done.
 *
 * The draft is the edited DOCUMENT, not file text: restoring it is one edit to
 * the document the builder opened on, so the writer splices it into the file
 * as it splices any edit, and Undo takes the restore back. It is written on
 * every change while the builder has unsaved edits and removed once it has
 * none, which is also how a save that lands clears it.
 *
 * Kept per key, in `localStorage`, which may be absent or refuse writes (a
 * private window, a full quota): {@link draftStorageAvailable} says so, and a
 * host then keeps its own leave guard.
 */

const PREFIX = "malloy-publisher.report-draft:";

/** Where drafts are kept: `localStorage`, behind one seam a test can replace. */
export const draftStore: { storage: () => Storage } = {
   storage: () => window.localStorage,
};

interface StoredDraft {
   v: 1;
   document: DashboardDocument;
   /** A hash of the file the edits were made over: a different file means the report changed since. */
   base: string;
   savedAt: number;
}

/**
 * What the builder found on opening. A draft put back is said in the save
 * status (its menu names when the edits are from) until the author has seen it
 * or edits again; a stale one is a choice the page asks for.
 */
export interface RestoredDraft {
   savedAt: number;
   /**
    * The file changed since the draft was kept. The draft is then OFFERED, not
    * applied: it is a whole document, and putting it back would undo whatever
    * changed the file (another author's save, an agent's write). Restoring it
    * is the author's call, and says it replaces the current version.
    */
   stale: boolean;
   /** Still to choose: a stale draft not yet restored or discarded. */
   pending: boolean;
}

/** The report a draft is kept for. */
export interface ReportDraftTarget {
   environmentName: string;
   packageName: string;
   /** The report's package path, `sales/q3.malloy`. */
   path: string;
   /** The package version the edits were made on; none for the live package. */
   versionId?: string;
}

/**
 * The key the builder keeps a report's draft under: one per server, report and
 * version, so a draft is never restored onto a file it was not made on.
 * `server` is the `ServerProvider`'s server: its `baseURL`, or this page's own
 * `/api/v0` when it was given none.
 */
export function reportDraftKey(
   target: ReportDraftTarget & { server: string },
): string {
   const { server, environmentName, packageName, versionId, path } = target;
   return `${server}|${environmentName}/${packageName}@${versionId ?? ""}/${path}`;
}

/**
 * Drop drafts kept in this browser: for a host that deleted, renamed or
 * replaced a report itself, so its old edits are not offered again.
 *
 * Given a report, every draft kept for it goes, on any server and any version,
 * unless `server` or `versionId` narrow it. Given a key from
 * {@link reportDraftKey}, that one draft.
 */
export function clearReportDraft(
   target: string | (ReportDraftTarget & { server?: string }),
): void {
   if (typeof target === "string") {
      remove(target);
      return;
   }
   const { server, environmentName, packageName, path } = target;
   const report = `${environmentName}/${packageName}@`;
   // Narrowed only by a version actually named: a spread that carries
   // `versionId: undefined` still clears every version.
   const version =
      target.versionId !== undefined
         ? `${target.versionId}/${path}`
         : undefined;
   try {
      const storage = draftStore.storage();
      const matching: string[] = [];
      for (let i = 0; i < storage.length; i++) {
         const stored = storage.key(i);
         if (!stored?.startsWith(PREFIX)) continue;
         const key = stored.slice(PREFIX.length);
         const bar = key.indexOf("|");
         if (bar === -1) continue;
         const [keyServer, rest] = [key.slice(0, bar), key.slice(bar + 1)];
         if (server !== undefined && keyServer !== server) continue;
         if (!rest.startsWith(report)) continue;
         const tail = rest.slice(report.length);
         if (
            version !== undefined
               ? tail !== version
               : tail.slice(tail.indexOf("/") + 1) !== path
         )
            continue;
         matching.push(stored);
      }
      // Removed after the walk: removing during it shifts the indexes.
      for (const stored of matching) storage.removeItem(stored);
   } catch {
      // Nothing kept, nothing to remove.
   }
}

/** Whether this browser keeps drafts at all. */
export function draftStorageAvailable(): boolean {
   try {
      const probe = `${PREFIX}probe`;
      draftStore.storage().setItem(probe, "1");
      draftStore.storage().removeItem(probe);
      return true;
   } catch {
      return false;
   }
}

/** A short, stable fingerprint of a file's text; not a security hash. */
export function textFingerprint(text: string): string {
   let hash = 5381;
   for (let i = 0; i < text.length; i++)
      hash = ((hash << 5) + hash + text.charCodeAt(i)) | 0;
   return `${text.length}:${(hash >>> 0).toString(36)}`;
}

function read(key: string): StoredDraft | undefined {
   try {
      const raw = draftStore.storage().getItem(PREFIX + key);
      if (!raw) return undefined;
      const parsed = JSON.parse(raw) as StoredDraft;
      return parsed?.v === 1 && parsed.document ? parsed : undefined;
   } catch {
      return undefined;
   }
}

function write(key: string, draft: StoredDraft): boolean {
   try {
      draftStore.storage().setItem(PREFIX + key, JSON.stringify(draft));
      return true;
   } catch {
      return false;
   }
}

function remove(key: string): void {
   try {
      draftStore.storage().removeItem(PREFIX + key);
   } catch {
      // Nothing kept, nothing to remove.
   }
}

/** Replace `target`'s contents with `next`'s, in place: what an editor's `update` can do with a whole document. */
function replaceContents(target: DashboardDocument, next: DashboardDocument) {
   const record = target as unknown as Record<string, unknown>;
   for (const key of Object.keys(record)) delete record[key];
   Object.assign(record, structuredClone(next));
}

export function useReportDraft(
   key: string | undefined,
   editor: ReportEditorHandle,
   /** The file the builder opened on. */
   opened: string,
): {
   /** When unsaved edits were last kept in this browser; undefined when none are. */
   keptAt: number | undefined;
   /** The draft the builder opened with: until it is seen, or edited over; a stale one until the author chooses. */
   restored: RestoredDraft | undefined;
   dismissRestored: () => void;
   /** Put the document back to the file's, as one undoable edit; the draft goes with the edits. */
   discard: () => void;
   /** Whether the browser is holding the edits: false once a write was refused. */
   keeping: boolean;
   /** Apply an offered (stale) draft, replacing the current version. */
   restoreOffered: () => void;
} {
   const [restored, setRestored] = useState<RestoredDraft | undefined>();
   const [keptAt, setKeptAt] = useState<number | undefined>();
   // The last write was refused (a full quota, a large document): nothing is
   // kept, so the host must not be told leaving is safe.
   const [writeFailed, setWriteFailed] = useState(false);
   // A stale draft waiting on the author: kept as it is, not overwritten or
   // removed by the clean document the builder opened on.
   const offered = useRef<StoredDraft | undefined>(undefined);
   // The document a draft was put back as, until the next edit: that edit
   // ends the "restored" news, since the draft is now just the edits.
   const restoredAs = useRef<string | undefined>(undefined);
   // Set until the put-back document has rendered: the pass in between still
   // sees the document from before, which is not an edit.
   const restoring = useRef(false);
   const base = textFingerprint(opened);
   const editorRef = useRef(editor);
   editorRef.current = editor;

   // On opening: a draft for this report goes back on, once.
   const restoredOnce = useRef(false);
   useEffect(() => {
      if (key === undefined || restoredOnce.current) return;
      restoredOnce.current = true;
      const draft = read(key);
      if (!draft) return;
      const current = editorRef.current;
      if (JSON.stringify(draft.document) === JSON.stringify(current.document)) {
         remove(key);
         return;
      }
      if (draft.base !== base) {
         offered.current = draft;
         setRestored({ savedAt: draft.savedAt, stale: true, pending: true });
         return;
      }
      current.update((document) => replaceContents(document, draft.document));
      restoredAs.current = JSON.stringify(draft.document);
      restoring.current = true;
      setRestored({ savedAt: draft.savedAt, stale: false, pending: false });
      // Only on opening: the key and the file it opened on are fixed for a mount.
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [key]);

   // Kept while there are edits to lose; gone once there are none.
   const { document, dirty } = editor;
   useEffect(() => {
      if (key === undefined || !restoredOnce.current) return;
      // The offered draft stands until the author chooses; an edit before
      // they do is a choice too, and replaces it.
      if (offered.current && !dirty) return;
      if (offered.current) {
         // Edited over instead of chosen: the edits are the choice, and the
         // offer goes, so nothing goes on asking about a draft no longer kept.
         offered.current = undefined;
         setRestored(undefined);
      }
      if (restoredAs.current !== undefined) {
         const now = JSON.stringify(document);
         if (restoring.current) {
            if (now === restoredAs.current) restoring.current = false;
         } else if (now !== restoredAs.current) {
            restoredAs.current = undefined;
            setRestored(undefined);
         }
      }
      if (!dirty) {
         remove(key);
         setKeptAt(undefined);
         setWriteFailed(false);
         return;
      }
      const savedAt = Date.now();
      if (write(key, { v: 1, document, base, savedAt })) {
         setKeptAt(savedAt);
         setWriteFailed(false);
      } else {
         setKeptAt(undefined);
         setWriteFailed(true);
      }
   }, [key, document, dirty, base]);

   // Put an offered (stale) draft back, replacing the current version.
   const restoreOffered = useCallback(() => {
      const draft = offered.current;
      if (!draft) return;
      offered.current = undefined;
      editorRef.current.update((document) =>
         replaceContents(document, draft.document),
      );
      restoredAs.current = JSON.stringify(draft.document);
      restoring.current = true;
      setRestored((was) => was && { ...was, pending: false });
   }, []);

   const dismissRestored = useCallback(() => {
      // A stale draft is a choice still to make, not news to dismiss.
      if (offered.current) return;
      restoredAs.current = undefined;
      setRestored(undefined);
   }, []);
   const discard = useCallback(() => {
      if (offered.current && key !== undefined) {
         // An offered draft was never applied: dropping it is all there is.
         offered.current = undefined;
         remove(key);
         setRestored(undefined);
         return;
      }
      const current = editorRef.current;
      current.update((document) => replaceContents(document, current.saved));
      restoredAs.current = undefined;
      setRestored(undefined);
   }, [key]);

   return {
      keptAt,
      keeping: !writeFailed,
      restored,
      dismissRestored,
      discard,
      restoreOffered,
   };
}
