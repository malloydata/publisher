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
 * is refused; the two refusals' wording is never compared. It is reported to
 * the shell's observer (which logs it at warn and counts it), and kept for a
 * spec to read (`test_helpers/admission_parity.ts`).
 * A spec that pins an accepted difference names it by a stable code
 * ({@link ParityAllowance}) around the queries it covers.
 */

import type { CollationProbe, ProbedTable } from "./gate_reads_types";

/** Where on a query's path a probe reads the warehouse or compiles. */
export type AdmissionReadSite = "source_probe" | "stored_probe";

/**
 * Where a query walks the IR for a fact a compile or bind records, because
 * the record did not cover it (an inline `extend`, a lane or a stored set
 * the record does not hold) or because admission runs in `probe` or
 * `shadow`: the served copy's per-source declines (`served_copy_declines`),
 * the gates' and row scope's reads and flags (`scope`), the rollups a
 * query's source could read (`rollup_candidates`).
 */
type QueryWalkSite = "served_copy_declines" | "scope" | "rollup_candidates";

/** Which way admission judges: `materialize.admissionMode`. */
type AdmissionMode = "probe" | "lookup" | "shadow";

/**
 * The comparisons shadow admission runs: the collation ports' two sides,
 * and each recorded fact against the walk it replaces.
 */
type ParitySide = "source" | "stored" | FactSide;

/**
 * The recorded facts shadow admission compares with the walks they replace:
 * each walk site, and `gate_exact`, a stored gate column's exactness read
 * from its column contract against the one judged from the bind's types.
 */
export type FactSide = QueryWalkSite | "gate_exact";

/**
 * What admission reports as it judges, for the shell to count and log: this
 * layer reads no telemetry and writes no log of its own, so the query
 * pipeline (`query_serving.ts`) installs it once.
 */
interface AdmissionObserver {
   catalogRead(site: AdmissionReadSite): void;
   probeCompile(site: AdmissionReadSite): void;
   queryWalk?(site: QueryWalkSite): void;
   drift(drift: ParityDrift): void;
}

let observer: AdmissionObserver | undefined;

let modeReader: () => AdmissionMode = () => "lookup";

/**
 * Install how the serve layer reads `materialize.admissionMode` (the shell
 * reads settings; this layer does not). Read once per use, so a spec that
 * sets the mode sees it at once.
 */
export function readAdmissionModeFrom(read: () => AdmissionMode): void {
   modeReader = read;
}

/** The admission mode in force. */
export function admissionMode(): AdmissionMode {
   return modeReader();
}

/** Run `fn` under `mode`, whatever settings say: for specs. */
export async function withAdmissionMode<T>(
   mode: AdmissionMode,
   fn: () => Promise<T>,
): Promise<T> {
   const before = modeReader;
   modeReader = () => mode;
   try {
      return await fn();
   } finally {
      modeReader = before;
   }
}

/** Every query-path walk reported in this process, by site, for specs. */
const walks = new Map<QueryWalkSite, number>();

/** Report one query-path walk for a fact a compile or bind records. */
export function reportQueryWalk(site: QueryWalkSite): void {
   walks.set(site, (walks.get(site) ?? 0) + 1);
   observer?.queryWalk?.(site);
}

/** The query-path walks reported so far, by site (a copy). */
export function queryWalks(): ReadonlyMap<QueryWalkSite, number> {
   return new Map(walks);
}

/** Forget the query-path walks reported so far. */
export function clearQueryWalks(): void {
   walks.clear();
}

/** Install what admission reports to; the last one installed wins. */
export function observeAdmission(o: AdmissionObserver): void {
   observer = o;
}

/** Report one catalog read a probe made on a query's path. */
export function reportCatalogRead(site: AdmissionReadSite): void {
   observer?.catalogRead(site);
}

/** Report one compile a probe made on a query's path, beyond the query's own. */
export function reportProbeCompile(site: AdmissionReadSite): void {
   observer?.probeCompile(site);
}

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
 *   not.
 */
export type ParityAllowance =
   | "bind_detects_alter"
   | "r10_binary64_orchestrated"
   | "bigquery_ttl";

/** One disagreement shadow admission found. */
export interface ParityDrift {
   readonly side: ParitySide;
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
   observer?.drift(kept);
}

/** Whether two refusals disagree on whether the comparison is refused. */
function disagree(a: string | undefined, b: string | undefined): boolean {
   return (a === undefined) !== (b === undefined);
}

/**
 * Shadow one recorded fact against the walk it replaces: record a
 * disagreement when `show` renders the two differently, and answer the
 * walk's. `show` renders a fact as the verdict it leads to (undefined for
 * none), so two facts that lead to the same verdict agree.
 */
export function shadowFact<T>(
   side: FactSide,
   walked: T,
   recorded: T,
   show: (fact: T) => string | undefined,
   tables: readonly string[] = [],
): T {
   const old = show(walked);
   const now = show(recorded);
   if (old !== now) record({ side, probe: old, lookup: now, tables });
   return walked;
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
