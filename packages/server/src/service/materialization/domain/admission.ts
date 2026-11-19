// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Read admission: whether an answer a leg produced from stored tables is
 * used, or declined and answered live, and why. One pure decision over the
 * tables the answer read and the facts gathered about the query, unifying
 * the rules a stored read is held to:
 *
 * - the gate policy ({@link answerKeepsGates}, which asks `legMayPassGate`
 *   first): a leg past an entry-point gate, and the tables it may read there;
 * - exactness: a column the gates or a row scope read, stored inexactly
 *   ({@link gateColumnsExact});
 * - zone: a gate column stored with a zone, compared in the destination
 *   session's zone rather than the source's;
 * - collation: the probes' answers for the source side and the stored side;
 * - row scope: an ungated `where:` re-applied to a stored table is held to
 *   the exactness and comparison rules too;
 * - the caller's own query filters, re-applied to stored rows, held to the
 *   comparison rules;
 * - a row filter declared on a composite, which live refuses every caller of.
 *
 * Pure: the switches and every fact are inputs. The collation probes run
 * SQL, so their answers are asked for only when a rule needs one: a verdict
 * `needs_probe` names the probe, and the caller (the admission shell,
 * `serve/read_admission.ts`) runs it and asks again with its answer.
 */

import {
   answerKeepsGates,
   GATE_LEGS,
   gateColumnsExact,
   type GateSwitches,
   legMayPassGate,
   SERVING_GATE_CONTEXT,
   type TableRead,
} from "./gate_policy";
import type { Leg } from "./vocabulary";

/** What an answer's gates and row scope read, as admission needs it. */
interface ScopeReads {
   /** A `where:` scopes the rows the query reads. */
   readonly scoped: boolean;
   /** The entry point carries a gate. */
   readonly gated: boolean;
}

/** A collation probe's answer: why it refuses, or undefined when it does not. */
interface ProbeAnswer {
   readonly refusal: string | undefined;
}

/**
 * The collation probes' answers for one comparison: the source side (asked
 * only when a `storage=` table is read) and the stored side (every table
 * read). Absent: not probed yet.
 */
export interface ComparisonProbes {
   readonly source?: ProbeAnswer;
   readonly stored?: ProbeAnswer;
}

/** Which comparison a probe is asked about. */
export type Comparison =
   /** What the entry point's gates and row scope compare. */
   | "scope"
   /** What the caller's own query filters compare. */
   | "filters";

/** One probe admission needs before it can decide. */
export interface ProbeRequest {
   readonly comparison: Comparison;
   readonly side: "source" | "stored";
}

/**
 * The caller's own filters (`where:`, `having:`, a filtered measure's, a
 * view's or a nest's) that an answer re-applies to stored rows.
 */
export type QueryFilterFacts =
   /** The answer applies none to stored rows: an exact call, or no table read. */
   | { readonly applies: false }
   | {
        readonly applies: true;
        /** What they compare; `unread` fails closed. */
        readonly reads: "unread" | "no_string" | "string";
        /** The tables they are applied to. */
        readonly tables: readonly TableRead[];
     };

/** What is known about one query and its answer, gathered by the caller. */
export interface ReadFacts {
   /** The router vetoes the query past its entry point: the entry point carries a gate. */
   readonly pastEntryPointGate: boolean;
   /** A row filter is declared on a composite the entry point reads. */
   readonly compositeRowFilter: boolean;
   /** What the gates and row scope read; undefined when they cannot be read. */
   readonly reads: ScopeReads | undefined;
   /**
    * Whether the tables' read columns were listed (a `storage=` table read,
    * or a database table whose columns moved since its build), so their
    * stored types were judged.
    */
   readonly columnsListed: boolean;
   /** The caller's own filters; absent, none is judged. */
   readonly queryFilters?: QueryFilterFacts;
   /** The probes' answers, by comparison, as far as they were asked. */
   readonly probes?: Partial<Record<Comparison, ComparisonProbes>>;
}

/** Why a read is declined. */
type AdmissionDecline =
   /** Past an entry-point gate, the answer does not keep the gates. */
   | "row_level_gate"
   /** A row scope reads a column a stored table does not hold exactly. */
   | "row_scope_inexact"
   /** A row scope reads a column a stored table does not compare as live. */
   | "row_scope_comparison"
   /** The caller's own filters compare a string a stored table does not compare as live. */
   | "query_filter_comparison";

export type AdmissionVerdict =
   | { readonly admit: true; readonly pastEntryPointGate: boolean }
   | {
        readonly admit: false;
        readonly reason: AdmissionDecline;
        /** Internal: for logs and the router's detail, never the wire. */
        readonly detail: string;
     }
   | { readonly admit: "needs_probe"; readonly probe: ProbeRequest };

/** A stored type that carries a zone: compared in the reading session's zone. */
const ZONED_STORED_TYPE = /\b(TIMESTAMP\s+WITH\s+TIME\s+ZONE|TIMESTAMPTZ)\b/i;

type GateLeg = (typeof GATE_LEGS)[number];

function gateLegOf(leg: Leg): GateLeg | undefined {
   return (GATE_LEGS as readonly Leg[]).includes(leg)
      ? (leg as GateLeg)
      : undefined;
}

/**
 * Whether `leg` may be tried past an entry-point gate at all (before any
 * attempt): the gate policy's row for it. A leg with no row (the query leg)
 * never is: the policy fails closed.
 */
export function mayAttempt(leg: Leg, s: GateSwitches): boolean {
   const gateLeg = gateLegOf(leg);
   return (
      gateLeg !== undefined && legMayPassGate(gateLeg, s, SERVING_GATE_CONTEXT)
   );
}

/**
 * Whether an answer `leg` produced reading `tables` keeps an entry point's
 * gates, when its read compares exactly as live does: the gate policy's part
 * of {@link admitRead}, without the per-query rules (zone, collation, row
 * scope, a composite's row filter). The serve verdict asks this of a
 * source's own table, before any query.
 */
export function keepsGatesWhenReadExact(
   leg: Leg,
   tables: readonly TableRead[],
   s: GateSwitches,
): boolean {
   const gateLeg = gateLegOf(leg);
   return (
      gateLeg !== undefined &&
      answerKeepsGates(gateLeg, tables, s, SERVING_GATE_CONTEXT)
   );
}

/** A comparison's outcome: a refusal (or none), or the probe it needs first. */
type ComparisonOutcome =
   | { readonly refusal: string | undefined }
   | { readonly probe: ProbeRequest };

/**
 * Why the stored tables do not compare what `comparison` reads as the
 * source does, or undefined: a column stored with a zone, then the source
 * side (when a `storage=` table is read), then the stored side.
 */
function comparisonRefusal(
   comparison: Comparison,
   tables: readonly TableRead[],
   facts: ReadFacts,
): ComparisonOutcome {
   const stored = tables.filter((t) => t.tier === "storage");
   const types = stored.flatMap((t) => t.gateColumnTypes ?? []);
   if (types.some((type) => ZONED_STORED_TYPE.test(type))) {
      return { refusal: "a gate column stored with a zone" };
   }
   const probes = facts.probes?.[comparison];
   if (stored.length > 0) {
      if (probes?.source === undefined) {
         return { probe: { comparison, side: "source" } };
      }
      if (probes.source.refusal !== undefined) return probes.source;
   }
   if (probes?.stored === undefined) {
      return { probe: { comparison, side: "stored" } };
   }
   return probes.stored;
}

const decline = (
   reason: AdmissionDecline,
   detail: string,
): AdmissionVerdict => ({ admit: false, reason, detail });

const needs = (probe: ProbeRequest): AdmissionVerdict => ({
   admit: "needs_probe",
   probe,
});

/**
 * The row-scope rule, for the served copy and the rollup leg: an ungated
 * `where:` re-applied to a stored table holds only where its columns are
 * stored exactly (the served copy's rule; the rollup leg judges its own
 * columns) and compared as live.
 */
function rowScopeVerdict(
   leg: Leg,
   tables: readonly TableRead[],
   facts: ReadFacts,
): AdmissionVerdict | undefined {
   if (leg !== "served" && leg !== "rollup") return undefined;
   const { reads } = facts;
   if (reads === undefined && !facts.columnsListed) return undefined;
   if (reads !== undefined && (!reads.scoped || reads.gated)) return undefined;
   // Only a listed table carries gateColumnTypes; an unlisted database one
   // is exact by its tier (gateColumnsExact).
   if (leg === "served" && !tables.every(gateColumnsExact)) {
      return decline(
         "row_scope_inexact",
         "the query's source scopes its rows by a column a stored table does not hold exactly",
      );
   }
   const outcome = comparisonRefusal("scope", tables, facts);
   if ("probe" in outcome) return needs(outcome.probe);
   return outcome.refusal === undefined
      ? undefined
      : decline(
           "row_scope_comparison",
           "the query's source scopes its rows by a column a stored table does not compare as its source does",
        );
}

/**
 * The caller's own filters: each string comparison they make must compare
 * on the stored rows as live compares it. Fails closed when they cannot be
 * read, or when a persisted result's table is unread.
 */
function queryFilterVerdict(facts: ReadFacts): AdmissionVerdict | undefined {
   const filters = facts.queryFilters;
   if (filters === undefined || !filters.applies) return undefined;
   if (filters.reads === "no_string") return undefined;
   let why: string | undefined;
   if (filters.reads === "unread") {
      why = "the query's filters unread";
   } else if (filters.tables.length === 0) {
      why = "the stored result's table unread";
   } else {
      const outcome = comparisonRefusal("filters", filters.tables, facts);
      if ("probe" in outcome) return needs(outcome.probe);
      why = outcome.refusal;
   }
   return why === undefined
      ? undefined
      : decline(
           "query_filter_comparison",
           `the query filters on a column a stored table does not compare as its source does: ${why}`,
        );
}

/**
 * Whether the answer `leg` produced, reading `tables`, is used: the row
 * scope first (judged as the leg answers), then the caller's own filters,
 * then, past an entry-point gate, the gate policy with the composite, zone
 * and collation rules. A leg the gate policy has no row for (the query leg)
 * is never used past a gate.
 */
export function admitRead(
   leg: Leg,
   tables: readonly TableRead[],
   facts: ReadFacts,
   s: GateSwitches,
): AdmissionVerdict {
   const scope = rowScopeVerdict(leg, tables, facts);
   if (scope !== undefined) return scope;
   const filters = queryFilterVerdict(facts);
   if (filters !== undefined) return filters;
   if (!facts.pastEntryPointGate) {
      return { admit: true, pastEntryPointGate: false };
   }
   const notKept = decline(
      "row_level_gate",
      "the answer does not keep the gates",
   );
   if (gateLegOf(leg) === undefined || facts.compositeRowFilter) return notKept;
   const outcome = comparisonRefusal("scope", tables, facts);
   if ("probe" in outcome) return needs(outcome.probe);
   if (
      outcome.refusal !== undefined ||
      !keepsGatesWhenReadExact(leg, tables, s)
   ) {
      return notKept;
   }
   return { admit: true, pastEntryPointGate: true };
}
