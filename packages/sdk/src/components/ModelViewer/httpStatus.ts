// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The HTTP status a rejected request carries (axios's `response.status`), if any. */
export function httpStatusOf(error: unknown): number | undefined {
   const status = (error as { response?: { status?: unknown } } | null)
      ?.response?.status;
   return typeof status === "number" ? status : undefined;
}

/** A refused read: 403, or 404 for something this caller may not know exists. */
export function isNotFoundOrForbidden(error: unknown): boolean {
   const status = httpStatusOf(error);
   return status === 403 || status === 404;
}
