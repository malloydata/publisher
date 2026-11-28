// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { FCJ_MAX_CHARS } from "../../constants";
import { jsonResource } from "../tool_response";
import { largest, wireChars } from "../reply_budget";

/** What `get_workflow` and `get_skill_set` both say about installing a set's files: the rule that keeps a `SKILL.md` served as written from granting what its frontmatter asks for. */
export const INSTALL_RULE = `## Installing a skill set
- A SKILL.md arrives as written, frontmatter included, and that frontmatter can name tools or hooks (\`allowed-tools\`, \`hooks\`).
- An installer writes only \`name\` and \`description\` into the installed file's frontmatter, taken from the SKILL.md entry's \`name\` and \`description\` fields (the description JSON-quoted), then the text after the file's own frontmatter.
- An installer names any other frontmatter key the file carried to the user.`;

export type Payload = { message?: string; [key: string]: unknown };

/** The query string of a REST fallback URL: the `versionId` this reply was read from, so the read answers from the same version after `latest` moves, then `extra`. */
export function restQuery(
   source: { versionId?: string } | undefined,
   ...extra: string[]
): string {
   const parts = [
      ...(source?.versionId
         ? [`versionId=${encodeURIComponent(source.versionId)}`]
         : []),
      ...extra,
   ];
   return parts.length === 0 ? "" : `?${parts.join("&")}`;
}

/** Characters of file text in `sets`: a floor for any reply that carries it, since JSON never shortens a text. */
export function fileTextChars(
   sets: ReadonlyArray<{
      skills: ReadonlyArray<{ file_contents?: string }>;
   }>,
): number {
   let chars = 0;
   for (const set of sets) {
      for (const file of set.skills) chars += file.file_contents?.length ?? 0;
   }
   return chars;
}

/** What names a list in a reply: its key and the plural noun a note about it uses. */
export interface ListShape {
   key: string;
   noun: string;
   fetch: string;
}

/**
 * `payload` with as many of `available` as fit the tool-result limit beside it,
 * and a note counting the rest. `fits` is false when not even an empty list
 * leaves room, and `sentChars` is then the size of that answer, which was
 * measured to learn it: the one measure a note about a refused reply uses.
 */
export function withAvailable<T>(
   uri: string,
   id: string | number | undefined,
   list: ListShape,
   available: T[],
   payload: Payload,
): { payload: Payload; fits: boolean; sentChars: number } {
   const answer = (listed: number) => {
      const unlisted = available.length - listed;
      const message = [
         payload.message,
         unlisted > 0
            ? `${unlisted} of ${available.length} ${list.noun} are not listed to keep this reply within the host's tool-result limit; one is still fetched by its ${list.fetch}.`
            : "",
      ]
         .filter(Boolean)
         .join(" ");
      return {
         ...payload,
         [list.key]: available.slice(0, listed),
         ...(message ? { message } : {}),
      };
   };
   let emptyChars = 0;
   const listed = largest((n) => {
      const chars = wireChars(uri, answer(n), id);
      if (n === 0) emptyChars = chars;
      return chars <= FCJ_MAX_CHARS;
   }, available.length);
   return {
      payload: answer(listed ?? 0),
      fits: listed !== undefined,
      sentChars: emptyChars,
   };
}

/** One step down the ladder of views a reply is cut to: what it keeps, and the least it can weigh. */
export interface Rung {
   /** Named in the note of the next step, as "the <view> view would be N characters". */
   view: string;
   /** A floor on the reply's size, such as the file text it carries; over the limit, the rung is skipped unmeasured. */
   floor?: number;
   /** The reply this rung sends. `prior` says what the step before it weighed. */
   build: (prior?: {
      view: string;
      chars: number;
      atLeast: boolean;
   }) => Payload;
}

/** The sentence that opens a note about a step that did not fit. */
export const tooLarge = (prior: {
   view: string;
   chars: number;
   atLeast: boolean;
}) =>
   `The ${prior.view} view would be ${prior.atLeast ? "at least " : ""}${prior.chars} characters as a tool result, over the limit of ${FCJ_MAX_CHARS}`;

/**
 * Sends the first rung that fits, each measured once and none past a floor
 * already over the limit. The last rung is sent even when it does not fit, with
 * the list cut to what is left, so a caller always learns where to read on.
 */
export function sendLadder<T>(
   uri: string,
   id: string | number | undefined,
   list: ListShape,
   available: T[],
   rungs: Rung[],
) {
   let prior: { view: string; chars: number; atLeast: boolean } | undefined;
   for (const [i, rung] of rungs.entries()) {
      const last = i === rungs.length - 1;
      if (!last && rung.floor !== undefined && rung.floor > FCJ_MAX_CHARS) {
         prior = { view: rung.view, chars: rung.floor, atLeast: true };
         continue;
      }
      const sent = withAvailable(uri, id, list, available, rung.build(prior));
      if (sent.fits || last) return jsonResource(uri, sent.payload);
      prior = { view: rung.view, chars: sent.sentChars, atLeast: false };
   }
   throw new Error("a ladder has at least one rung");
}
