// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Alert, Box } from "@mui/material";
import type { ReactNode } from "react";
import { MONO_FONT_FAMILY } from "./styles";

export interface ApiError extends Error {
   status?: number;
   data?: {
      code: number;
      message: string;
   };
}

export interface ApiErrorDisplayProps {
   error: ApiError;
   context?: string;
}

/**
 * An error as an MUI `Alert`: a one-line summary, and the server's own words
 * under it in a monospace block that keeps their line breaks. Themed, so it
 * reads in dark mode as well as light. Shared with a notebook cell that could
 * not run, so every request failure in a document looks the same.
 */
export function ErrorDetailAlert({
   summary,
   detail,
}: {
   summary?: ReactNode;
   detail: ReactNode;
}) {
   return (
      <Alert
         severity="error"
         sx={{ "& .MuiAlert-message": { minWidth: 0, flex: 1 } }}
      >
         {summary !== undefined && <Box sx={{ mb: 0.5 }}>{summary}</Box>}
         <Box
            component="pre"
            sx={{
               m: 0,
               whiteSpace: "pre-wrap",
               wordBreak: "break-word",
               fontFamily: MONO_FONT_FAMILY,
               fontSize: "0.8125rem",
               color: "text.secondary",
            }}
         >
            {detail}
         </Box>
      </Alert>
   );
}

/**
 * What to say about a failed request. The server's own message when it sent
 * one. Otherwise the request never got a readable body back (a network error,
 * a timeout, an aborted request, an empty 502, a token that could not be
 * fetched), and `useQueryWithApiError` passes the raw error through with no
 * `data`. Its status and message are then the only facts there are, so they
 * are what the reader sees.
 */
export function apiErrorMessage(error: ApiError): string {
   const serverMessage = error.data?.message;
   if (serverMessage) return serverMessage;
   // `status` is set by `useQueryWithApiError` and, since axios 1.7, by axios
   // itself; `response.status` covers an error from anything older.
   const status =
      error.status ??
      (error as { response?: { status?: number } }).response?.status;
   const message = error.message?.trim();
   // axios already words a bad status as "Request failed with status code
   // 502"; saying the number twice adds nothing.
   if (status !== undefined && message)
      return message.includes(String(status))
         ? message
         : `${message} (HTTP ${status})`;
   if (status !== undefined) return `The request failed with HTTP ${status}.`;
   if (message) return message;
   return "The request failed, and the error carried no message or status.";
}

export function ApiErrorDisplay({ error, context }: ApiErrorDisplayProps) {
   return (
      <ErrorDetailAlert summary={context} detail={apiErrorMessage(error)} />
   );
}
