// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   type KeyboardEvent,
   useEffect,
   useMemo,
   useRef,
   useState,
} from "react";
import { GIVEN_SETTLE_MS, useSettled } from "./useSettled";

/**
 * A control's own copy of a value that is applied the moment it is committed.
 *
 * A draft the reader has edited is the source of truth while the field is
 * focused, so a value echoing back from the URL cannot overwrite keystrokes
 * typed since; a field the reader has not edited follows `committed`. An edited
 * draft commits on blur, on Enter, and once it has settled (not mid-IME
 * composition), each only if it differs from `committed`. A change made while
 * the field is not focused (a calendar pick) commits at once. Mount and an
 * untouched focus never commit.
 *
 * `unset` says `committed` is the empty display of a value that was never
 * set, so an edit that lands back on that display still commits it: the
 * reader emptied the field on purpose.
 */
export function useSettledDraft<T>(
   committed: T,
   commit: (draft: T) => void,
   {
      same = Object.is,
      unset = false,
   }: { same?: (a: T, b: T) => boolean; unset?: boolean } = {},
) {
   const [draft, setDraft] = useState(committed);
   const [focused, setFocused] = useState(false);
   // A ref, set synchronously, so an unmount in the same batch as a reset sees it.
   const edited = useRef(false);
   const composing = useRef(false);
   const latest = useRef({ draft, committed, focused, unset, commit, same });
   useEffect(() => {
      latest.current = { draft, committed, focused, unset, commit, same };
   });

   const due = () => {
      const l = latest.current;
      return edited.current && (l.unset || !l.same(l.draft, l.committed));
   };
   const flush = () => {
      if (due()) latest.current.commit(latest.current.draft);
   };

   // Composition end bumps `gen` so a draft that settled mid-composition is
   // re-armed: the confirming input event repeats the tracked text, so the
   // draft alone would not change.
   const [gen, setGen] = useState(0);
   const pending = useMemo(() => ({ draft, gen }), [draft, gen]);
   const settled = useSettled(pending, GIVEN_SETTLE_MS);
   useEffect(() => {
      const l = latest.current;
      if (l.same(settled.draft, l.draft) && due() && !composing.current) {
         l.commit(settled.draft);
      }
      // eslint-disable-next-line react-hooks/exhaustive-deps -- `due` reads refs only
   }, [settled]);

   // A pending draft is committed rather than dropped if the field goes away.
   // eslint-disable-next-line react-hooks/exhaustive-deps -- `flush` reads refs only
   useEffect(() => () => flush(), []);

   useEffect(() => {
      if (same(draft, committed)) {
         if (!unset) edited.current = false;
         return;
      }
      if (!focused || !edited.current) setDraft(committed);
   }, [committed, focused, draft, unset, same]);

   return {
      draft,
      /** Show `next` as typed; applies it now if the field is not focused. */
      change: (next: T) => {
         edited.current = true;
         setDraft(next);
         if (!latest.current.focused && !same(next, committed)) commit(next);
      },
      /** Drop the draft for `next` without committing, for a clear. */
      reset: (next: T) => {
         edited.current = false;
         setDraft(next);
      },
      bind: {
         onFocus: () => setFocused(true),
         onBlur: () => {
            setFocused(false);
            flush();
         },
         onKeyDown: (e: KeyboardEvent) => {
            if (e.key === "Enter" && !e.nativeEvent.isComposing) flush();
         },
         onCompositionStart: () => {
            composing.current = true;
         },
         onCompositionEnd: () => {
            composing.current = false;
            setGen((g) => g + 1);
         },
      },
   };
}
