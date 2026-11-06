// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `name@url` split into its parts, as Malloy writes a compiled source's
 * `sourceID`, `extends` and `referenceID`. Undefined for anything else.
 */
export function splitSourceId(
   id: unknown,
): { source: string; url: string } | undefined {
   if (typeof id !== "string") return undefined;
   // Greedy, so a name containing "@" still splits at the URL's own "@".
   const match = /^(.+)@([a-z][a-z0-9+.-]*:\/\/.*)$/i.exec(id);
   return match ? { source: match[1], url: match[2] } : undefined;
}
