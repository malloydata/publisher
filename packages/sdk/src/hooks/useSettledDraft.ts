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
 * While the field is focused the draft is the source of truth and is never
 * resynced from `committed`, so a value echoing back from the URL cannot
 * overwrite keystrokes typed since. It commits on blur, on Enter, and once it
 * has settled (not mid-IME composition), each only if it differs from
 * `committed`. A change made while the field is not focused (a calendar pick)
 * commits at once. Mount never commits: the first settled value is the draft.
 */
export function useSettledDraft<T>(
   committed: T,
   commit: (draft: T) => void,
   same: (a: T, b: T) => boolean = Object.is,
) {
   const [draft, setDraft] = useState(committed);
   const [focused, setFocused] = useState(false);
   const composing = useRef(false);
   const latest = useRef({ draft, committed, focused, commit, same });
   useEffect(() => {
      latest.current = { draft, committed, focused, commit, same };
   });

   const flush = () => {
      const l = latest.current;
      if (!l.same(l.draft, l.committed)) l.commit(l.draft);
   };

   // Composition end bumps `gen` so a draft that settled mid-composition is
   // re-armed: the confirming input event repeats the tracked text, so the
   // draft alone would not change.
   const [gen, setGen] = useState(0);
   const pending = useMemo(() => ({ draft, gen }), [draft, gen]);
   const settled = useSettled(pending, GIVEN_SETTLE_MS);
   useEffect(() => {
      const l = latest.current;
      if (
         l.same(settled.draft, l.draft) &&
         !l.same(settled.draft, l.committed) &&
         !composing.current
      ) {
         l.commit(settled.draft);
      }
   }, [settled]);

   // A pending draft is committed rather than dropped if the field goes away.
   useEffect(() => () => flush(), []);

   // A reset or a drill arrives through a click, which blurs first, so the
   // field is never focused when a value it did not type needs showing.
   useEffect(() => {
      if (!focused && !same(draft, committed)) setDraft(committed);
   }, [committed, focused, draft, same]);

   return {
      draft,
      /** Show `next` as typed; applies it now if the field is not focused. */
      change: (next: T) => {
         setDraft(next);
         if (!latest.current.focused && !same(next, committed)) commit(next);
      },
      /** Drop the draft for `next` without committing, for a clear. */
      reset: (next: T) => setDraft(next),
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
