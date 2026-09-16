// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import Snackbar from "@mui/material/Snackbar";
import { useQueryClient, type QueryKey } from "@tanstack/react-query";
import * as React from "react";
import { useRef, useState } from "react";
import { useMutationWithApiError } from "./useQueryWithApiError";
import {
   reportConsoleEvent,
   type ConsoleResource,
} from "../telemetry/consoleEvents";

/**
 * A write from a dialog: run it, close the dialog, refetch what it changed,
 * and say what happened.
 *
 * Every create/edit/delete dialog in the Console does those four things in the
 * same order and reported failure in one of two different ways depending on
 * which file you were in. The shape is here once so the two conventions cannot
 * drift back apart: a thrown `Error` shows its own message, because the API
 * layer already turns a failed response into one worth reading, and anything
 * else falls back to a sentence rather than `[object Object]`.
 *
 * `notice` is the Snackbar, wired and ready to render. It is returned rather
 * than portalled from in here so a caller keeps it as a sibling of its dialog,
 * where it was.
 */
export function useCrudMutation<TVariables = void>({
   mutationFn,
   success,
   invalidates,
   onSettled,
   resource,
   action,
}: {
   mutationFn: (variables: TVariables) => Promise<unknown>;
   /** What the snackbar says when the write lands: "Package created". */
   success: string;
   /** The query keys the write invalidates. Name the narrowest that covers it. */
   invalidates: QueryKey[];
   /** Closes the dialog. Runs before the refetch, so the dialog goes at once. */
   onSettled: () => void;
   /** What is being written, for the event; see `ConsoleEvent`. */
   resource: ConsoleResource;
   action: "create" | "update" | "delete";
}) {
   const queryClient = useQueryClient();
   const [message, setMessage] = useState("");
   // Measured across the whole write, not the request: what a dialog reports
   // is how long the operator waited for it to close.
   const startedAt = useRef(0);
   const mutation = useMutationWithApiError({
      mutationFn,
      onMutate() {
         startedAt.current = Date.now();
      },
      onSuccess() {
         reportConsoleEvent({
            type: "console.mutation",
            resource,
            action,
            ok: true,
            durationMs: Date.now() - startedAt.current,
         });
         onSettled();
         for (const queryKey of invalidates)
            queryClient.invalidateQueries({ queryKey });
         setMessage(success);
      },
      onError(error) {
         const reason =
            error instanceof Error
               ? error.message
               : "An unknown error occurred";
         reportConsoleEvent({
            type: "console.mutation",
            resource,
            action,
            ok: false,
            durationMs: Date.now() - startedAt.current,
            reason,
         });
         setMessage(reason);
      },
   });
   return {
      mutate: mutation.mutate,
      isPending: mutation.isPending,
      notice: (
         <Snackbar
            open={message !== ""}
            autoHideDuration={6000}
            onClose={() => setMessage("")}
            message={message}
         />
      ) as React.ReactNode,
   };
}
