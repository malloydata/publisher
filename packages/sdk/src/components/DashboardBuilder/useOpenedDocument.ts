// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useRef, useState, type MutableRefObject } from "react";
import type { Workspace } from "../DocumentStorage";
import { now } from "../../utils/clock";
import type { DashboardDocument } from "./document";
import { readForEditor } from "./readForEditor";
import type { BuilderEvent } from "./telemetry";

export const LEGACY_REFUSAL =
   "a .malloynb notebook is read, not edited. Fix: rewrite it as a `.malloy` notebook under notebooks/ to edit it here.";

const WITHHELD_REFUSAL =
   "the server did not send this notebook's text, so there is nothing here to edit. Fix: edit the file in the package.";

/** The document the builder is on, and what it was opened from. */
export interface OpenedDocument {
   source: string;
   document: DashboardDocument;
   conversion?: { from: string; to: string };
   /** The package file when this opened, which a draft's save overwrites. */
   packageText?: string;
   generation: number;
}

/**
 * Which text the editor opens, and when a newer one is held back.
 *
 * Three channels can hold the document — the host's record, a copy the reader
 * resumes, the package file — and each can move under an open editor. This
 * decides which one the builder is on, opens it (reporting the open or the
 * refusal), and keeps a newer version behind a banner rather than loading it
 * over work the reader has not saved.
 */
export function useOpenedDocument({
   workspace,
   draft,
   draftChecked,
   readFailure,
   packageText,
   fetchedAt,
   packageSettled,
   modelPath,
   notebook,
   noun,
   refusedEvent,
   legacyFormat,
   textHeld,
   startedAt,
   onEventRef,
}: {
   workspace: Workspace | undefined;
   /** The host's copy, if one was read. */
   draft: string | undefined;
   /** Whether the host's store has answered. */
   draftChecked: boolean;
   readFailure: string | undefined;
   /** The file as the package's last fetch has it. */
   packageText: string | undefined;
   /** When that fetch landed. */
   fetchedAt: number;
   /** Whether the package's fetch has succeeded and is not refetching. */
   packageSettled: boolean;
   modelPath: string;
   notebook: boolean;
   noun: string;
   refusedEvent: "notebook.open_refused" | "dashboard.open_refused";
   legacyFormat: boolean;
   /** The document is held as text by the host: there is no package file to fall back on. */
   textHeld: boolean;
   /** When the open was asked for, so "opened" can say how long it took. */
   startedAt: MutableRefObject<number>;
   onEventRef: MutableRefObject<((event: BuilderEvent) => void) | undefined>;
}) {
   // The host's copy is the record: the editor opens it, writes back to it,
   // and never offers to "resume" it, because a record is not a pending edit.
   const authoritative = workspace?.authoritative === true;
   // The package file is a deploy of the record, so opening it when the record
   // itself could not be read would put a reader on the wrong document.
   const blockedOnRecord = authoritative && readFailure !== undefined;

   // What the editor opens: the record when the host keeps one, the copy when
   // the reader chose to resume it, the package file otherwise. `generation`
   // remounts the builder for a fresh history when that choice changes.
   const [resume, setResume] = useState<boolean | undefined>(undefined);
   const fromDraft = authoritative
      ? draft !== undefined
      : resume === true && draft !== undefined;
   const [opened, setOpened] = useState<OpenedDocument | undefined>(undefined);
   const [openError, setOpenError] = useState<string | undefined>(
      legacyFormat ? LEGACY_REFUSAL : undefined,
   );
   // The package's hash after this editor's last write, as the server computed
   // it: the base the next save is spliced against.
   const savedHashRef = useRef<string | undefined>(undefined);
   // The package file as it stood when the editor last opened a document. The
   // base a save is spliced against is the package's, which is NOT the text
   // the builder holds: a resumed copy was opened from the store, and a
   // version held back while the reader keeps editing has moved the fetch
   // past the file the reader is answering for.
   const packageBaseRef = useRef<string | undefined>(undefined);
   // What this editor last wrote into the package, and the fetch it was
   // written on top of. The write is in the file before the fetch behind
   // `packageText` catches up, so until a fetch actually lands `packageText`
   // is the text the save replaced rather than a version to open. Tied to the
   // fetch and not to the text, so the NEXT fetch speaks for the file whether
   // it carries this editor's write or someone else's.
   const [wrote, setWrote] = useState<
      { text: string; onFetch: number } | undefined
   >(undefined);
   const fetchedAtRef = useRef(fetchedAt);
   fetchedAtRef.current = fetchedAt;
   const packageNow =
      wrote !== undefined && wrote.onFetch === fetchedAt
         ? wrote.text
         : packageText;
   const packageNowRef = useRef(packageNow);
   packageNowRef.current = packageNow;
   const latest = fromDraft ? draft : packageNow;

   // Whether the builder holds edits the record does not have, and the version
   // being held back because of them.
   const [dirty, setDirty] = useState(false);
   const [accepted, setAccepted] = useState<string | undefined>(undefined);
   // The text on this channel the editor has already reckoned with: what it
   // opened, and what it wrote. Compared against the CHANNEL rather than
   // against the builder, because the two diverge legitimately — a copy saved
   // beside the package leaves the builder ahead of a package that has not
   // moved, and reading that as an incoming version would offer a reader
   // their own work back forever.
   const [seen, setSeen] = useState<string | undefined>(undefined);
   const incoming = latest !== undefined && latest !== seen;
   // What the builder is on. A save does not remount it, so this follows the
   // save rather than the text the builder was opened with.
   const current = opened?.source;
   // Work the package does not have: unsaved edits, or a copy saved beside the
   // package that differs from it. Either way a newer version is held back
   // behind the banner rather than loaded over what the reader made. Only a
   // copy the builder is ON counts: one the reader passed over for the package
   // file is not work in front of them, and holding a version back for it
   // would tell them "your edits are still here" about edits they never opened.
   const aheadOfPackage =
      !authoritative &&
      draft !== undefined &&
      current === draft &&
      draft !== packageNow;
   const holding =
      incoming &&
      (dirty || aheadOfPackage) &&
      current !== undefined &&
      latest !== accepted;
   const opening = holding ? current : incoming ? latest : (current ?? latest);
   const held = holding ? latest : undefined;

   // Read by the open effect so it keeps its single dependency: the guard must
   // not re-run the effect when what it compares against changes.
   const openedSourceRef = useRef(current);
   openedSourceRef.current = current;
   const latestRef = useRef(latest);
   latestRef.current = latest;
   const fromRef = useRef<"package" | "draft" | "record">("package");
   fromRef.current = fromDraft
      ? authoritative
         ? "record"
         : "draft"
      : "package";
   useEffect(() => {
      // Not until the storage answer is in. Opening on the package file while
      // the editor still has no idea whether the host keeps the record would
      // open the wrong document, and report an open of it.
      if (opening === undefined || !draftChecked || blockedOnRecord) return;
      if (opening === openedSourceRef.current) return;
      let stale = false;
      const packageAtOpen = packageNowRef.current;
      const latestAtOpen = latestRef.current;
      void readForEditor(opening, modelPath, textHeld)
         .then((result) => {
            if (stale) return;
            if (result.ok === false) {
               setOpenError(result.reason);
               onEventRef.current?.({
                  type: refusedEvent,
                  reason: result.reason,
               });
               return;
            }
            setOpenError(undefined);
            // A different document is open, so what this editor wrote before is
            // no longer the base anything is spliced against; the package file
            // the reader is now answering for is the one current at this open.
            savedHashRef.current = undefined;
            packageBaseRef.current = packageAtOpen;
            setWrote(undefined);
            setAccepted(undefined);
            setSeen(latestAtOpen);
            setOpened((previous) => ({
               source: opening,
               document: result.document,
               ...(result.conversion ? { conversion: result.conversion } : {}),
               ...(packageAtOpen !== undefined
                  ? { packageText: packageAtOpen }
                  : {}),
               generation: (previous?.generation ?? 0) + 1,
            }));
            onEventRef.current?.(
               notebook || result.document.kind === "notebook"
                  ? {
                       type: "notebook.opened",
                       from:
                          fromRef.current === "package" ? "package" : "record",
                       cells: result.document.tiles.length,
                       durationMs: now() - startedAt.current,
                    }
                  : {
                       type: "dashboard.opened",
                       from: fromRef.current,
                       tiles: result.document.tiles.length,
                       durationMs: now() - startedAt.current,
                    },
            );
         })
         .catch((error: unknown) => {
            if (stale) return;
            const reason = `Could not read the ${noun}: ${error instanceof Error ? error.message : String(error)}`;
            setOpenError(reason);
            onEventRef.current?.({ type: refusedEvent, reason });
         });
      return () => {
         stale = true;
      };
   }, [
      opening,
      modelPath,
      draftChecked,
      blockedOnRecord,
      notebook,
      noun,
      refusedEvent,
      textHeld,
      // Refs, stable: read when the open lands, never a reason to open again.
      startedAt,
      onEventRef,
   ]);

   useEffect(() => {
      if (legacyFormat)
         onEventRef.current?.({
            type: "notebook.open_refused",
            reason: LEGACY_REFUSAL,
         });
   }, [legacyFormat, onEventRef]);

   // The record is the only document a text source has, so it must exist and be one the host calls authoritative.
   useEffect(() => {
      if (!textHeld || !draftChecked || opened || readFailure !== undefined)
         return;
      if (!authoritative)
         setOpenError(
            "a document held as text needs a storage whose workspace is authoritative. Fix: mark the workspace that keeps it `authoritative`.",
         );
      else if (draft === undefined)
         setOpenError("the host's storage has no document at this location.");
   }, [textHeld, draftChecked, opened, readFailure, authoritative, draft]);

   const withheld =
      notebook &&
      !textHeld &&
      !opened &&
      draftChecked &&
      !fromDraft &&
      packageSettled &&
      packageText === undefined;
   useEffect(() => {
      if (!withheld) return;
      setOpenError(WITHHELD_REFUSAL);
      onEventRef.current?.({
         type: "notebook.open_refused",
         reason: WITHHELD_REFUSAL,
      });
   }, [withheld, onEventRef]);

   // An explicit choice is a choice about the very text that would otherwise
   // be held back, so it is never held.
   const choose = (resumeDraft: boolean) => {
      startedAt.current = now();
      setAccepted(resumeDraft ? draft : packageNow);
      setResume(resumeDraft);
   };
   const acceptHeld = () => {
      startedAt.current = now();
      setAccepted(held);
   };

   return {
      authoritative,
      blockedOnRecord,
      resume,
      setResume,
      fromDraft,
      opened,
      setOpened,
      openError,
      savedHashRef,
      packageBaseRef,
      setWrote,
      fetchedAtRef,
      setDirty,
      setSeen,
      held,
      choose,
      acceptHeld,
   };
}
