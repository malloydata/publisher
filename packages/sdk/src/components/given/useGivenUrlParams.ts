// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useMemo, useRef } from "react";
import { useLocation, useNavigate, useSearchParams } from "react-router-dom";

export interface UseGivenUrlParamsResult {
   /** The query string as a flat record: what a document reads its givens from. */
   params: Record<string, string>;
   /**
    * For a document's `onGivensChange`: writes the applied values into the
    * query string, in place, leaving every parameter that is not a given alone.
    */
   onGivensChange: (
      next: Record<string, string>,
      managed: readonly string[],
   ) => void;
}

/**
 * Keep a document's control values in the page's query string, so a filtered
 * dashboard or a parameterized notebook is a shareable link.
 *
 * This is the host side of `useGivensState`: the document reports its applied
 * values and the names it manages, this writes them into the URL and feeds the
 * URL back in as `params`. The two Console pages carried a copy each; they
 * agreed line for line, so this is that code once, for any host on
 * react-router.
 *
 * Three things it is careful about, each of which was a bug once:
 *
 * - **Merge, never replace.** The query string is not the document's alone: a
 *   tracking tag or any other unrelated parameter shares it, and replacing the
 *   whole string dropped those on LOAD, as soon as the first report arrived.
 *   Only names that are ours are touched: what the document manages now, plus
 *   anything this hook wrote earlier, so a control cleared after a model reload
 *   stopped declaring it still gets its parameter removed. That memory outlives
 *   a document swap on purpose: the parameter written for the previous
 *   document is the stale one the next should clear.
 * - **`hasOwnProperty`, not `in`.** `next` is a plain object literal, so `in`
 *   walks its prototype and reports `constructor`, `toString` and friends as
 *   present; a given with one of those names could never be cleared.
 * - **Replace, and keep the hash.** Changing a control is not a navigation
 *   step, so Back should leave the document rather than walk back through
 *   every value tried. And a navigation that names only `search` resolves to an
 *   empty fragment, which dropped whatever anchor the reader arrived on;
 *   `setSearchParams` has that problem, which is why this navigates itself.
 */
export function useGivenUrlParams(): UseGivenUrlParamsResult {
   const [searchParams] = useSearchParams();
   const navigate = useNavigate();
   const location = useLocation();

   const params = useMemo(
      () => Object.fromEntries(searchParams.entries()),
      [searchParams],
   );

   const writtenRef = useRef<Set<string>>(new Set());

   const onGivensChange = useCallback(
      (next: Record<string, string>, managed: readonly string[]) => {
         const ours = new Set([...managed, ...writtenRef.current]);
         writtenRef.current = new Set([
            ...writtenRef.current,
            ...Object.keys(next),
            ...managed,
         ]);
         const merged = new URLSearchParams(location.search);
         for (const name of ours) {
            if (!Object.prototype.hasOwnProperty.call(next, name)) {
               merged.delete(name);
            }
         }
         for (const [name, value] of Object.entries(next)) {
            merged.set(name, value);
         }
         navigate(
            { search: merged.toString(), hash: location.hash },
            { replace: true },
         );
      },
      [navigate, location],
   );

   return { params, onGivensChange };
}
