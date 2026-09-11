// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Typography } from "@mui/material";

export interface ApiError extends Error {
   status?: number;
   data?: {
      code: number;
      message: string;
   };
}

interface ApiErrorDisplayProps {
   error: ApiError;
   context?: string;
}

export function ApiErrorDisplay({ error, context }: ApiErrorDisplayProps) {
   const errorMessage = error.data?.message || "Unknown Error";
   return (
      <>
         {context && (
            <Typography variant="body2" sx={{ p: "10px", m: "auto" }}>
               {context}
            </Typography>
         )}

         <pre
            style={{
               whiteSpace: "pre-wrap",
               color: "red",
               padding: "10px",
               margin: "auto",
            }}
         >
            {errorMessage}
         </pre>
      </>
   );
}
