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
 * - collation: the probes' answers for the source side and the stored side,
 *   which the caller reads through its ports and passes in as facts;
 * - row scope: an ungated `where:` re-applied to a stored table is held to
 *   the exactness and comparison rules too;
 * - a row filter declared on a composite, which live refuses every caller of.
 *
 * Pure: the switches and every fact are inputs. The router's own checks are
 * this decision's reference until it is wired in;
 * `admission_characterization.spec.ts` holds both to one table.
 */

import {
   answerKeepsGates,
   GATE_LEGS,
   gateColumnsExact,
   type GateSwitches,
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
   /** The source side's collation refusal, when any `storage=` table is read. */
   readonly sourceCollationRefusal?: string;
   /** The stored side's collation refusal, over every table read. */
   readonly storedCollationRefusal?: string;
}

/** Why a read is declined. */
type AdmissionDecline =
   /** Past an entry-point gate, the answer does not keep the gates. */
   | "row_level_gate"
   /** A row scope reads a column a stored table does not hold exactly. */
   | "row_scope_inexact"
   /** A row scope reads a column a stored table does not compare as live. */
   | "row_scope_comparison";

type AdmissionVerdict =
   | { readonly admit: true; readonly pastEntryPointGate: boolean }
   | { readonly admit: false; readonly reason: AdmissionDecline };

/** A stored type that carries a zone: compared in the reading session's zone. */
const ZONED_STORED_TYPE = /\b(TIMESTAMP\s+WITH\s+TIME\s+ZONE|TIMESTAMPTZ)\b/i;

type GateLeg = (typeof GATE_LEGS)[number];

function gateLegOf(leg: Leg): GateLeg | undefined {
   return (GATE_LEGS as readonly Leg[]).includes(leg)
      ? (leg as GateLeg)
      : undefined;
}

/**
 * Why the stored tables do not compare what the gates and scopes read as
 * the source does, or undefined: a column stored with a zone, then the
 * source side (when a `storage=` table is read), then the stored side.
 */
function comparisonRefusal(
   tables: readonly TableRead[],
   facts: ReadFacts,
): string | undefined {
   const stored = tables.filter((t) => t.tier === "storage");
   const types = stored.flatMap((t) => t.gateColumnTypes ?? []);
   if (types.some((type) => ZONED_STORED_TYPE.test(type))) {
      return "a gate column stored with a zone";
   }
   if (stored.length > 0 && facts.sourceCollationRefusal !== undefined) {
      return facts.sourceCollationRefusal;
   }
   return facts.storedCollationRefusal;
}

/**
 * The row-scope rule, for the served copy and the rollup leg: an ungated
 * `where:` re-applied to a stored table holds only where its columns are
 * stored exactly (the served copy's rule; the rollup leg judges its own
 * columns) and compared as live.
 */
function rowScopeDecline(
   leg: Leg,
   tables: readonly TableRead[],
   facts: ReadFacts,
): AdmissionDecline | undefined {
   if (leg !== "served" && leg !== "rollup") return undefined;
   const { reads } = facts;
   if (reads === undefined && !facts.columnsListed) return undefined;
   if (reads !== undefined && (!reads.scoped || reads.gated)) return undefined;
   // Only a listed table carries gateColumnTypes; an unlisted database one
   // is exact by its tier (gateColumnsExact).
   if (leg === "served" && !tables.every(gateColumnsExact)) {
      return "row_scope_inexact";
   }
   return comparisonRefusal(tables, facts) !== undefined
      ? "row_scope_comparison"
      : undefined;
}

/**
 * Whether the answer `leg` produced, reading `tables`, is used: the row
 * scope first (judged as the leg answers), then, past an entry-point gate,
 * the gate policy with the composite, zone and collation rules. A leg the
 * gate policy has no row for (the query leg) is never used past a gate.
 */
export function admitRead(
   leg: Leg,
   tables: readonly TableRead[],
   facts: ReadFacts,
   s: GateSwitches,
): AdmissionVerdict {
   const scope = rowScopeDecline(leg, tables, facts);
   if (scope !== undefined) return { admit: false, reason: scope };
   if (!facts.pastEntryPointGate) {
      return { admit: true, pastEntryPointGate: false };
   }
   const gateLeg = gateLegOf(leg);
   if (
      gateLeg === undefined ||
      facts.compositeRowFilter ||
      comparisonRefusal(tables, facts) !== undefined ||
      !answerKeepsGates(gateLeg, tables, s, SERVING_GATE_CONTEXT)
   ) {
      return { admit: false, reason: "row_level_gate" };
   }
   return { admit: true, pastEntryPointGate: true };
}
