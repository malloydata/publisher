// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ApiError } from "../ApiErrorDisplay";

/** Malloy's code for a construct restricted mode refuses, as the query route returns it in `problems`. */
const RESTRICTED_CONSTRUCT_CODE = "restricted-construct-forbidden";

/** Why a query cell has no result; `restricted` is the query route's restricted compile, which the served cell does not use. */
export type CellFailure = "unavailable" | "restricted" | "error";

export function cellFailure(error: ApiError | null | undefined): CellFailure {
   const status = error?.status;
   if (status === 404 || status === 403) return "unavailable";
   const problems = (error?.data as { problems?: unknown } | undefined)
      ?.problems;
   if (
      status === 400 &&
      Array.isArray(problems) &&
      problems.some(
         (problem) =>
            (problem as { code?: unknown } | null)?.code ===
            RESTRICTED_CONSTRUCT_CODE,
      )
   )
      return "restricted";
   return "error";
}
