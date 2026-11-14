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

export function ApiErrorDisplay({ error, context }: ApiErrorDisplayProps) {
   const errorMessage = error.data?.message || "Unknown error";
   return <ErrorDetailAlert summary={context} detail={errorMessage} />;
}
