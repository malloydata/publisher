// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Containment for the served copy: a compile failure in one re-root costs
 * that source alone, not every source of the model.
 *
 * The served copy is one compiled model per author model, so a problem in one
 * re-rooted declaration would otherwise fail the whole compile and send every
 * stored source in the model live. Each applied re-root's own text (its
 * `type:` and its re-rooted declaration, {@link ServedCopySpan}) is the only
 * text the served copy changes, and the author's text compiles live, so a
 * problem placed in one is that re-root's. The serve side drops that re-root
 * (the source then answers live, with a coded reason) and compiles the rest
 * again.
 *
 * The build companion does the same for build copies and generated files
 * (`compileContained` in plan/materialize_compile.ts): trace a compile
 * problem's place to the source whose generated text it is in, refuse that
 * source, compile again. This module is the served copy's half, keyed on the
 * spans {@link buildServedCopy} reports; it is kept free of the serve class so
 * any other served-copy writer (a v1 source's re-root) can trace its own
 * failures with it.
 */

import { offsetOf } from "../text/malloy_lexer";
import { overlayKey } from "../../overlay_key";
import {
   errorProblems,
   isCrossConnectionRefusal,
   isJoinConnectionMismatch,
   mayFollowFromLeftOut,
} from "../plan/cross_connection_joins";
import type { ServedCopySpan } from "./served_copy";

/**
 * How strictly a problem is traced to a re-root.
 *
 * `precise`: a problem that may follow from a join refused across
 * connections (a missing field) is not traced to a re-root whose own text
 * holds such a refusal, since leaving that join out (cross_connection_joins.ts)
 * may clear it and keep the table for every other query; a failure that is
 * nothing but such refusals is traced to no re-root at all.
 *
 * `last_resort`: every problem but the refusal itself is traced, for when
 * nothing else made the copy compile and the alternative is losing it whole.
 */
export type TraceMode = "precise" | "last_resort";

/** A re-root a compile failure is traced to, by file and source name. */
interface FailingReroot {
   url: string;
   sourceName: string;
   /** The first traced problem's message, for the log and the warning. */
   message: string;
}

/** `url\0sourceName`, the key a re-root is known by across passes. */
export function rerootKey(url: string, sourceName: string): string {
   return `${overlayKey(url)}\u0000${sourceName}`;
}

/**
 * The re-roots a served copy's compile failure is in: each whose own text
 * (its `type:` or its re-rooted declaration) holds a problem, under `mode`.
 * `files` is the text each file compiled with, by URL (a pass that left a
 * join out blanks it in place, so offsets are unchanged); `spans` is where
 * each re-root lies in it. Empty when no problem can be placed in one: the
 * failure is then the caller's to handle as before (a problem in the
 * author's own text, outside every re-root, is not one source's).
 */
export function failingReroots(
   error: unknown,
   files: ReadonlyMap<string, string>,
   spans: ReadonlyMap<string, readonly ServedCopySpan[]>,
   mode: TraceMode,
): FailingReroot[] {
   const problems = errorProblems(error);
   if (!problems) return [];
   if (mode === "precise" && isCrossConnectionRefusal(error)) return [];
   const textOf = new Map([...files].map(([u, t]) => [overlayKey(u), t]));
   const spansOf = new Map([...spans].map(([u, s]) => [overlayKey(u), s]));
   const urlOf = new Map([...spans.keys()].map((u) => [overlayKey(u), u]));
   const placed = problems.flatMap((p) => {
      const url = p.at?.url;
      const start = p.at?.range?.start;
      if (typeof url !== "string" || !start) return [];
      const key = overlayKey(url);
      const text = textOf.get(key);
      const list = spansOf.get(key);
      if (text === undefined || !list) return [];
      const offset = offsetOf(text, start);
      if (offset < 0) return [];
      const span = list.find(
         (s) =>
            (offset >= s.declaration.start && offset < s.declaration.end) ||
            (offset >= s.type.start && offset < s.type.end),
      );
      if (!span) return [];
      return [
         {
            url: urlOf.get(key) as string,
            span,
            code: p.code,
            message: (p as { message?: unknown }).message,
         },
      ];
   });
   // Re-roots whose own text holds a join refused across connections.
   const crossed = new Set(
      placed
         .filter((p) => isJoinConnectionMismatch(p.code))
         .map((p) => rerootKey(p.url, p.span.sourceName)),
   );
   const out = new Map<string, FailingReroot>();
   for (const p of placed) {
      if (isJoinConnectionMismatch(p.code)) continue;
      const key = rerootKey(p.url, p.span.sourceName);
      if (
         mode === "precise" &&
         crossed.has(key) &&
         mayFollowFromLeftOut(p.code)
      ) {
         continue;
      }
      if (out.has(key)) continue;
      out.set(key, {
         url: p.url,
         sourceName: p.span.sourceName,
         message:
            typeof p.message === "string"
               ? p.message.split("\n")[0]
               : "does not compile",
      });
   }
   return [...out.values()];
}
