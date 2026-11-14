// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { QueryClientContext } from "@tanstack/react-query";
import { useContext, useEffect, useState } from "react";
import type { OptionPage, OptionSearch } from "../../hooks/useSuggestOptions";

/** How long typing has to pause before the server is asked. */
export const OPTION_SEARCH_DEBOUNCE_MS = 250;

/** How long a search's answer is reused for the same text: option lists change with the data, not the page. */
const SEARCH_STALE_MS = 5 * 60 * 1000;

export interface OptionSearchState {
   /** Text is typed and the list is cut off, so what is offered comes from the server. */
   active: boolean;
   /** Waiting on the server for the text as it is now: still debouncing, or in flight. */
   searching: boolean;
   /** The server's answer for exactly the text as it is now. Undefined until it has one. */
   page: OptionPage | undefined;
   /** The search for the text as it is now failed. */
   failed: boolean;
}

/**
 * Search a cut-off option list on the server as the reader types.
 *
 * Without `search` (a list that was not cut off, or a named query that cannot
 * be searched) this is inert and sends nothing: the picker filters the list it
 * has in the browser, as before.
 *
 * Each search is cached under the suggest's key plus the text, in the page's
 * query client, so typing back to an earlier text reuses its answer. An answer
 * is shown only for the text it was asked for: one arriving after the reader
 * has typed on is dropped, never shown over the newer text's.
 *
 * Reads the query client from context rather than requiring one: a picker
 * rendered outside a provider (a test, a host without react-query) has no
 * cache, and gets no server search rather than an error.
 */
export function useOptionSearch(
   search: OptionSearch | undefined,
   typed: string,
): OptionSearchState {
   const client = useContext(QueryClientContext);
   const text = typed.trim();
   // Empty at first, so even the first text waits for typing to pause.
   const [debounced, setDebounced] = useState("");
   useEffect(() => {
      const timer = setTimeout(
         () => setDebounced(text),
         OPTION_SEARCH_DEBOUNCE_MS,
      );
      return () => clearTimeout(timer);
   }, [text]);

   const [answer, setAnswer] = useState<{
      key: string;
      text: string;
      page?: OptionPage;
      failed?: boolean;
   }>();
   const keyText = search ? JSON.stringify(search.key) : undefined;
   useEffect(() => {
      if (!search || !client || debounced === "") return;
      let current = true;
      client
         .fetchQuery({
            queryKey: [...search.key, debounced],
            queryFn: ({ signal }) => search.run(debounced, signal),
            staleTime: SEARCH_STALE_MS,
         })
         .then((page) => {
            if (current) setAnswer({ key: keyText!, text: debounced, page });
         })
         .catch(() => {
            if (current)
               setAnswer({ key: keyText!, text: debounced, failed: true });
         });
      // A newer text, or another suggest, makes this answer stale: it is
      // dropped when it lands rather than shown over the newer one's.
      return () => {
         current = false;
      };
      // `keyText` stands for `search`, whose object identity is not the point.
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [client, keyText, debounced]);

   const active = search !== undefined && client !== undefined && text !== "";
   // For this suggest and exactly this text: an answer for another control's
   // list, or for what was typed a moment ago, is not this one's.
   const answered =
      answer !== undefined && answer.key === keyText && answer.text === text;
   return {
      active,
      searching: active && !answered,
      page: active && answered ? answer.page : undefined,
      failed: active && answered && answer.failed === true,
   };
}
