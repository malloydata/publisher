// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Discovery under a lock: which sources a caller cannot enter, decided on the
 * `#(secure)` givens the trusted tier set for them, so `get_context` and the
 * REST model read can omit those sources the way they omit a source locked
 * with `#(authorize) false`.
 *
 * Only a SECURE given takes part. A plain given is whatever the request
 * supplies, so hiding on it would hide nothing from a caller who can send
 * any value; a lock term that reads a plain given is simply not evaluated
 * here and the source stays visible. A lock term that reads a secure given
 * the trusted tier did not supply cannot admit the caller, so the source is
 * omitted. Row filters (`#(access_filter)`) never hide anything: they scope
 * rows, not schema.
 *
 * With no givens supplied nothing is omitted, which is what every caller saw
 * before this existed. This is discovery, not enforcement: the query route's
 * gates decide what is served, and they decide the same way whether or not a
 * source was listed.
 */

import type { GivenValue } from "@malloydata/malloy";
import type { components } from "../api";
import { parseAuthorizeGrammarBody } from "./authorize_grammar";
import { AUTHORIZE_ROUTE } from "./authorize_routes";

type ApiSource = components["schemas"]["Source"];

/** A Malloy string literal's content: the quotes off, no escape handling needed for a lock literal. */
function unquote(literal: string): string {
   const t = literal.trim();
   if (t.length >= 2 && (t[0] === "'" || t[0] === '"') && t.at(-1) === t[0]) {
      return t.slice(1, -1);
   }
   return t;
}

/** Whether one secure-given lock term admits the caller's value. */
function termAdmits(literal: string, value: GivenValue | undefined): boolean {
   if (value === undefined || value === null) return false;
   const want = unquote(literal);
   if (Array.isArray(value)) return value.some((v) => String(v) === want);
   return String(value) === want;
}

/**
 * The names of the sources among `sources` whose `#(authorize)` lock cannot
 * admit a caller holding `givens`, judged on secure givens only. Empty when
 * `givens` is absent.
 */
export function lockedForCaller(
   sources: readonly ApiSource[] | undefined,
   givens: Readonly<Record<string, GivenValue>> | undefined,
): Set<string> {
   const locked = new Set<string>();
   if (!givens || !sources) return locked;
   for (const source of sources) {
      if (!source.name) continue;
      const secure = new Set<string>();
      const declaredTypes = new Map<string, string>();
      for (const g of source.givens ?? []) {
         if (!g.name) continue;
         if (g.secure === true) secure.add(g.name);
         if (g.type) declaredTypes.set(g.name, g.type);
      }
      for (const expr of source.authorize ?? []) {
         let terms;
         try {
            terms = parseAuthorizeGrammarBody(
               source.name,
               expr,
               declaredTypes,
               AUTHORIZE_ROUTE,
            );
         } catch {
            // The load already judged this gate; discovery does not re-judge.
            continue;
         }
         for (const term of terms) {
            if (term.scope === "deny_all") {
               locked.add(source.name);
               break;
            }
            if (term.scope !== "source_level") continue;
            if (!secure.has(term.given)) continue;
            if (!termAdmits(term.literal, givens[term.given])) {
               locked.add(source.name);
               break;
            }
         }
         if (locked.has(source.name)) break;
      }
   }
   return locked;
}
