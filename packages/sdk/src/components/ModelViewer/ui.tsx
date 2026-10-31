// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Alert, Box, Button, type ButtonProps } from "@mui/material";
import { forwardRef, useEffect, useState, type ReactNode } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { highlight } from "../highlighter";
import { httpStatusOf } from "./httpStatus";
import { MONO_FONT_FAMILY } from "./theme";

/**
 * Malloy, highlighted, in a read-only block: a field's definition, a view's
 * code, the query a failed run sent. Plain text until the highlighter answers,
 * so the code is readable on the first frame.
 */
export function QueryCode({
   code,
   testId,
   framed = true,
}: {
   code: string;
   testId?: string;
   /** False: the code as one section of a card someone else frames (an open field row's). */
   framed?: boolean;
}) {
   const { mode } = usePublisherTheme();
   const [html, setHtml] = useState<string | undefined>(undefined);
   useEffect(() => {
      let live = true;
      highlight(code, "malloy", mode)
         .then((out) => live && setHtml(out))
         .catch(() => live && setHtml(undefined));
      return () => {
         live = false;
      };
   }, [code, mode]);
   return (
      <Box
         data-testid={testId}
         sx={{
            fontFamily: MONO_FONT_FAMILY,
            fontSize: "0.75rem",
            overflowX: "auto",
            "& pre": { m: 0, background: "transparent !important" },
            ...(framed && {
               border: "1px solid",
               borderColor: "divider",
               borderRadius: 1,
               px: 1.5,
               py: 1,
               bgcolor: "background.default",
            }),
         }}
      >
         {html ? (
            <div dangerouslySetInnerHTML={{ __html: html }} />
         ) : (
            <Box component="pre" sx={{ whiteSpace: "pre" }}>
               {code}
            </Box>
         )}
      </Box>
   );
}

/** A read that failed, in place of what it would have shown, with a Retry where one can help. */
export function LoadErrorAlert({
   message,
   onRetry,
   severity = "error",
}: {
   message: ReactNode;
   onRetry?: () => void;
   severity?: "error" | "warning";
}) {
   return (
      <Alert
         severity={severity}
         action={
            onRetry && (
               <Button color="inherit" size="small" onClick={onRetry}>
                  Retry
               </Button>
            )
         }
      >
         {message}
      </Alert>
   );
}

/** The server's own reason for a failed request (its Error body's `message`), if it gave one. */
export function serverMessage(error: unknown): string | undefined {
   const message = (
      error as { response?: { data?: { message?: unknown } } } | null
   )?.response?.data?.message;
   return typeof message === "string" && message ? message : undefined;
}

/** The server's own reason for a failed request, else what was being done. */
export function describeApiError(context: string, error: unknown): string {
   const message = serverMessage(error);
   return message ? `${context}: ${message}` : `${context}.`;
}

/**
 * Can re-sending this query succeed? Not for a 400 (the query does not compile)
 * or a 422 (a value it refuses), which hold until the query or the value
 * changes, nor for a refusal (401/403/404).
 */
export function isRetryableQueryFailure(error: unknown): boolean {
   const status = httpStatusOf(error);
   return !(
      status === 400 ||
      status === 401 ||
      status === 403 ||
      status === 404 ||
      status === 422
   );
}

/**
 * A failed query, wherever a result would have been. The query is shown
 * verbatim above the error, so a compile error's `line:column` points into text
 * the reader can see, and Retry is offered only when a second attempt can
 * succeed.
 */
export function QueryFailure({
   query,
   error,
   onRetry,
   testId,
   forbiddenMessage,
}: {
   /** The query as it was sent; omitted when there is none to show. */
   query?: string;
   error: unknown;
   onRetry?: () => void;
   testId?: string;
   /** What a refusal (403) says, where the caller knows the rule that refused it. */
   forbiddenMessage?: string;
}) {
   const refused =
      forbiddenMessage !== undefined && httpStatusOf(error) === 403;
   return (
      <Box
         data-testid={testId}
         sx={{ display: "flex", flexDirection: "column", gap: 1 }}
      >
         {query && <QueryCode code={query} testId="query-failure-code" />}
         <LoadErrorAlert
            message={
               refused
                  ? forbiddenMessage
                  : describeApiError("Query failed", error)
            }
            onRetry={
               onRetry && isRetryableQueryFailure(error) ? onRetry : undefined
            }
         />
      </Box>
   );
}

/** The one filled action on a card: Run. */
export const PrimaryButton = forwardRef<HTMLButtonElement, ButtonProps>(
   function PrimaryButton(props, ref) {
      return (
         <Button
            ref={ref}
            variant="contained"
            size="small"
            disableElevation
            {...props}
            sx={{ textTransform: "none", ...props.sx }}
         />
      );
   },
);

/** A rounded, quiet button that opens a menu: the sources picker. */
export const PillButton = forwardRef<HTMLButtonElement, ButtonProps>(
   function PillButton(props, ref) {
      return (
         <Button
            ref={ref}
            variant="outlined"
            size="small"
            color="inherit"
            {...props}
            sx={{
               textTransform: "none",
               borderRadius: 999,
               borderColor: "divider",
               color: "text.secondary",
               ...props.sx,
            }}
         />
      );
   },
);
