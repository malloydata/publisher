// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The parity seam between read admission's two ways of judging a stored
 * answer's string comparisons: the probes, which read the warehouse per
 * query (`gate_collation.ts`), and the lookups, which read what load, bind
 * and build recorded (`admission_lookup.ts`). `materialize.admissionMode`
 * picks one per query: `probe`, `lookup`, or `shadow`, which runs both,
 * decides by the probe, and records every disagreement here.
 *
 * A disagreement is a probe and a lookup that differ on whether a comparison
 * is refused; the two refusals' wording is never compared. It is logged at
 * warn, counted, and kept for a spec to read (`test_helpers/admission_parity.ts`).
 * A spec that pins an accepted difference names it by a stable code
 * ({@link ParityAllowance}) around the queries it covers.
 */

import { recordAdmissionParityDrift } from "../../../materialization_metrics";
import { logger } from "../../../logger";
import type { CollationProbe, ProbedTable } from "./gate_reads_types";

/**
 * A difference between the probe and the lookup that is accepted, by name:
 *
 * - `bind_detects_alter`: a collation altered after the bind is caught at
 *   the next bind, not on the next answer, so until then the probe refuses
 *   and the lookup admits;
 * - `r10_binary64_orchestrated`: a stored binary64 gate column the probe's
 *   inputs cannot prove (no `upstreamSchemas`, as a host's manifest carries
 *   none) and the build's contract does;
 * - `bigquery_ttl`: a BigQuery catalog the probe re-read and the load did
 *   not;
 * - `destination_default_unrecorded`: TEMPORARY, until PR B records a
 *   destination's `default_collation` in the contract and checks it at bind
 *   (`compatible()` then reports `collation_changed`). A destination whose
 *   session default is set after the build is refused by the probe and
 *   admitted by the lookup. Remove it once that lands.
 */
export type ParityAllowance =
   | "bind_detects_alter"
   | "r10_binary64_orchestrated"
   | "bigquery_ttl"
   | "destination_default_unrecorded";

/** One disagreement shadow admission found. */
export interface ParityDrift {
   readonly side: "source" | "stored";
   /** The probe's refusal, or undefined when it admitted. */
   readonly probe: string | undefined;
   /** The lookup's refusal, or undefined when it admitted. */
   readonly lookup: string | undefined;
   /** The tables the stored side judged, by handle. */
   readonly tables: readonly string[];
   /** The accepted difference it falls under, when a spec named one. */
   readonly allowed?: ParityAllowance;
}

/** Every disagreement found in this process, oldest first; bounded. */
const drifts: ParityDrift[] = [];
const MAX_KEPT = 1000;

/** The allowance a spec is running under, innermost last. */
const allowances: ParityAllowance[] = [];

/** The disagreements found so far (a copy). */
export function parityDrifts(): readonly ParityDrift[] {
   return [...drifts];
}

/** Forget every disagreement found so far. */
export function clearParityDrifts(): void {
   drifts.length = 0;
}

/**
 * Run `fn` with every disagreement it produces marked as `allowance`: a spec
 * pinning an accepted difference around the queries it covers.
 */
export async function withParityAllowance<T>(
   allowance: ParityAllowance,
   fn: () => Promise<T>,
): Promise<T> {
   allowances.push(allowance);
   try {
      return await fn();
   } finally {
      allowances.splice(allowances.lastIndexOf(allowance), 1);
   }
}

/**
 * Mark every disagreement from now until the returned release as
 * `allowance`: a spec's `beforeAll` and `afterAll` around a block of cases.
 */
export function holdParityAllowance(allowance: ParityAllowance): () => void {
   allowances.push(allowance);
   let held = true;
   return () => {
      if (!held) return;
      held = false;
      allowances.splice(allowances.lastIndexOf(allowance), 1);
   };
}

function record(drift: Omit<ParityDrift, "allowed">): void {
   const allowed = allowances[allowances.length - 1];
   const kept: ParityDrift = allowed ? { ...drift, allowed } : drift;
   if (drifts.length >= MAX_KEPT) drifts.shift();
   drifts.push(kept);
   recordAdmissionParityDrift(
      drift.side,
      drift.probe !== undefined ? "probe_refuses" : "lookup_refuses",
   );
   logger.warn("admission parity drift", {
      side: drift.side,
      probe: drift.probe ?? null,
      lookup: drift.lookup ?? null,
      tables: drift.tables,
      ...(allowed ? { allowed } : {}),
   });
}

/** Whether two refusals disagree on whether the comparison is refused. */
function disagree(a: string | undefined, b: string | undefined): boolean {
   return (a === undefined) !== (b === undefined);
}

/** A lookup that threw: a refusal, so it never admits what it could not judge. */
const threw = (err: unknown) =>
   `the lookup failed: ${err instanceof Error ? err.message : String(err)}`;

/**
 * The port that runs `probe` and `lookup` side by side and answers as the
 * probe does, recording each disagreement.
 */
export function shadowProbe(
   probe: CollationProbe,
   lookup: CollationProbe,
): CollationProbe {
   return {
      async sourceRefusal(args) {
         const [old, now] = await Promise.all([
            probe.sourceRefusal(args),
            lookup.sourceRefusal(args).catch(threw),
         ]);
         if (disagree(old, now)) {
            record({ side: "source", probe: old, lookup: now, tables: [] });
         }
         return old;
      },
      async storedRefusal(args) {
         const [old, now] = await Promise.all([
            probe.storedRefusal(args),
            lookup.storedRefusal(args).catch(threw),
         ]);
         if (disagree(old, now)) {
            record({
               side: "stored",
               probe: old,
               lookup: now,
               tables: handlesOf(args.tables),
            });
         }
         return old;
      },
   };
}

function handlesOf(tables: readonly ProbedTable[]): string[] {
   return tables.flatMap((t) =>
      typeof t.virtualHandle === "string" ? [t.virtualHandle] : [],
   );
}
