/**
 * Request header carrying the private data-management authorize bypass.
 *
 * It is a header rather than a `QueryRequest` field because the router's
 * outbound `PublisherQueryRequest` and its own PUBLIC inbound `QueryRequest`
 * are generated from the same schema: a body field able to reach the worker
 * would also be settable by any external caller. The router sets this on the
 * `ApiClient` at construction (`addDefaultHeader`, the same mechanism that
 * carries `Authorization`), so it never passes through the body conversion and
 * no caller value can reach it.
 *
 * Lowercase because Node lowercases inbound header names.
 */
export const BYPASS_AUTHORIZE_HEADER = "x-publisher-bypass-authorize";

/**
 * Minimal structural type the reader needs from an HTTP request. Narrower than
 * `express.Request` so tests can pass a bare object, as
 * {@link HeaderSetter} does for the response side.
 */
export interface HeaderCarrier {
   headers: Record<string, string | string[] | undefined>;
}

/**
 * Read the authorize bypass off the request headers, returning `true` only for
 * an unambiguous opt-in and `undefined` otherwise.
 *
 * Fails closed on everything else, including a repeated header (an array): a
 * value we cannot read as exactly one `true` leaves the author's gates
 * enforced. Deliberately does NOT consult the request body — a
 * `bypassAuthorize` body field is inert, which is what keeps the public
 * `QueryRequest` schema from becoming a gate-disabling control.
 */
export const readBypassAuthorize = (req: HeaderCarrier): true | undefined => {
   const raw = req.headers[BYPASS_AUTHORIZE_HEADER];
   return typeof raw === "string" && raw.trim().toLowerCase() === "true"
      ? true
      : undefined;
};
