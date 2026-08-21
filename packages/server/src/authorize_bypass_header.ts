// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Request header carrying an authorize bypass, for trusted data-management
 * callers. Read only here; nothing in this repo bounds who may send it.
 *
 * A header rather than a `QueryRequest` field because a deployment fronting this
 * API generally reuses the same generated request schema for its own inbound
 * body, so a body field able to reach the worker would also be settable by that
 * deployment's external callers. Set on the client at construction, it never
 * passes through body conversion, so no caller body value can reach it.
 *
 * Lowercase because Node lowercases inbound header names.
 */
export const BYPASS_AUTHORIZE_HEADER = "x-publisher-bypass-authorize";

/**
 * Minimal structural type the reader needs from an HTTP request. Narrower than
 * `express.Request` so tests can pass a bare object.
 */
export interface HeaderCarrier {
   headers: Record<string, string | string[] | undefined>;
}

/**
 * Read the authorize bypass off the request headers, returning `true` only for
 * an unambiguous opt-in and `undefined` otherwise.
 *
 * Anything else leaves the author's gates enforced, which covers a duplicated
 * header without a special case: for a custom header Node joins duplicates into
 * one comma-separated string (`"true, true"`), not an array — the `string[]`
 * arm of {@link HeaderCarrier} is reachable only for `set-cookie` — and a joined
 * value is not `"true"`, so it denies.
 *
 * Deliberately does NOT consult the request body: a `bypassAuthorize` body field
 * is inert, which is what keeps the public request schema from becoming a
 * gate-disabling control.
 */
export const readBypassAuthorize = (req: HeaderCarrier): true | undefined => {
   const raw = req.headers[BYPASS_AUTHORIZE_HEADER];
   return typeof raw === "string" && raw.trim().toLowerCase() === "true"
      ? true
      : undefined;
};
