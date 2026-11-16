// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The access record: one structured line per query-route call, carrying what
 * an audit control asks for (HIPAA 164.312(b), SOC 2 CC7.2, PCI DSS 10.2,
 * NIST AU-2) and nothing else. Separate from the request log, which is a
 * debug-grade dump of the request and response, and from the gate metrics,
 * which count decisions with no identity attached.
 *
 * What it carries: when, trace id, environment, package and version, model
 * path; the source entered; the gates in force and the decision the route
 * made; the VALUES of the `#(secure)` givens the trusted tier set, which are
 * the caller's verified attributes (roles, organizations) and so the line an
 * auditor reads, and only the NAMES of plain givens, which the caller chose
 * and which already sit in the warehouse's own query log; the fields the
 * compiled query reads with their classes, so the record says "read two
 * `phi.direct_identifier` fields" in the policy's words; status, row count,
 * duration, served-from; and a caller identity taken from the request header
 * the deployment names (`PUBLISHER_CALLER_HEADER`, default
 * `x-publisher-caller`), since Publisher does not authenticate and the
 * gateway in front of it does.
 *
 * Emitted through the logger as JSON (`event: "access"`) and as attributes
 * on the active span, so it lands wherever the operator already ships logs
 * and traces. Retention and tamper evidence are the log store's. Nothing
 * here changes what a query returns, and a failure to build the record never
 * fails the query: the record is then emitted with what was known.
 */

import { trace } from "@opentelemetry/api";
import type { GivenValue, ModelDef } from "@malloydata/malloy";
import type { components } from "../api";
import { errorKind, internalErrorToHttpError, type ErrorKind } from "../errors";
import { logger, traceIdOf } from "../logger";
import { redactConnectionSecretShapes } from "../pg_helpers";
import { settings } from "../settings";
import { ACCESS_FILTER_ROUTE, AUTHORIZE_ROUTE } from "./authorize_routes";
import { readQueryFieldReads } from "./classification";
import { readPreparedQuery } from "./governance/ir/prepared_query";
import { accessRecordSink } from "./access_record_sink";
import { workloadShapeOf, type WorkloadShape } from "./workload_shape";

const MAX_RECORDED_ERROR_CHARS = 2000;

type ApiSource = components["schemas"]["Source"];
type ApiGiven = components["schemas"]["Given"];

/** The request header the deployment names for the caller identity, lowercased. */
export function callerHeaderName(): string {
   return settings.get("callerHeader").toLowerCase();
}

/** Who the gateway says is asking and the trace the request belongs to. */
export type AccessIdentity = Pick<AccessRecordBase, "caller" | "traceId">;

/** The identity a request's headers carry, with a key only for what is present so it spreads into a record base. */
export function accessIdentityOf(
   headers: Record<string, string | string[] | undefined> = {},
): AccessIdentity {
   const raw = headers[callerHeaderName()];
   const caller = (Array.isArray(raw) ? raw[0] : raw) || undefined;
   const traceId = traceIdOf({ headers });
   return {
      ...(caller ? { caller } : {}),
      ...(traceId ? { traceId } : {}),
   };
}

/** A field the compiled query read, with its classes as the model declares or infers them. */
interface FieldRead {
   path: string;
   classifications: string[];
   /** Classes its inputs carry that it declares below; present only when some. */
   dropped?: string[];
}

/**
 * What the query service knows about one admitted query's access, built
 * once the gates have run and the query has returned.
 */
interface QueryAccessSummary {
   /** The source the query entered, when it could be resolved. */
   source?: string;
   /** The gates in force on that source, as the model declares them. */
   gates: { authorize: string[]; accessFilter: string[] };
   /**
    * `open`: no gate on the source. `admitted`: a lock admitted the caller and
    * no row filter applied. `filtered`: a row filter was grafted (a lock may
    * also have admitted). `bypassed`: the data-management header skipped the
    * gates. `ungated`: a route that serves with no gate to run (raw SQL).
    */
   decision: "open" | "admitted" | "filtered" | "bypassed" | "ungated";
   /** The names of every given supplied, secure or not; a plain given's value is never recorded. */
   givenNames: string[];
   /**
    * The values of the supplied givens declared `#(secure)`: the caller's
    * attributes as the trusted tier resolved them. Present only when there
    * are any.
    */
   secureGivens?: Record<string, GivenValue>;
   /** The given names the grafted row filters read. */
   gateGivenNames: string[];
   /** The fields the compiled query reads, by path from the source, with classes. */
   fieldsRead: FieldRead[];
   /** False when a read could not be placed or the join walk was cut short, so `fieldsRead` may be short. */
   fieldsReadComplete: boolean;
   /** Rows returned, after the cap. */
   rowCount: number;
   /**
    * The query's workload shape, literals replaced (materialization v2, T1):
    * what the workload log groups by. Absent when the compiled query could
    * not be read.
    */
   shape?: WorkloadShape;
}

const byCodepoint = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

/**
 * The gates enforcement evaluated for one bound query, per route: the locks it
 * decided (and admitted) and the row filters it grafted, read off the entry
 * point's gate walk, not looked up by source name. A source the caller
 * declares in the query (`mine is chart extend {}`) has no load-time entry, so
 * only this says which lock admitted it. On the bypass path, the gates the
 * walk found in force and skipped.
 */
export interface EnforcedGates {
   authorize: string[];
   accessFilter: string[];
}

/**
 * {@link EnforcedGates} from the gate entries enforcement evaluated, in the
 * order it evaluated them. A gate is evaluated once per check that reaches it
 * (a request-declared entry point is decided by the walk and again by the
 * laundering check, each building its own entry), so an entry is listed once
 * per route and expression list, not per object.
 */
export function enforcedGatesOf(
   entries: ReadonlyArray<{ route: string; exprs: readonly string[] }>,
): EnforcedGates {
   const seen = new Set<string>();
   const unique = entries.filter((e) => {
      const key = JSON.stringify([e.route, e.exprs]);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
   });
   const of = (route: string) =>
      unique.filter((e) => e.route === route).flatMap((e) => [...e.exprs]);
   return {
      authorize: of(AUTHORIZE_ROUTE),
      accessFilter: of(ACCESS_FILTER_ROUTE),
   };
}

/**
 * Build the summary for an admitted query. Resolves the fields the compiled
 * query's first stage references against the lineage of the struct it ran
 * against, so a reference through a join (`patient.ssn`) carries the class
 * declared where the field lives. A read that cannot be placed, or a join
 * walk that was cut short, sets `fieldsReadComplete` false.
 */
export async function summarizeQueryAccess(args: {
   runnable: { getPreparedQuery(): Promise<unknown> };
   modelDef: ModelDef | undefined;
   /**
    * What enforcement evaluated for this query ({@link EnforcedGates}); the
    * record's gates and decision come from it. Absent only when no bind
    * recorded one, and then the load-time `apiSources` entry for `source`
    * stands in, as before enforcement recorded its own.
    */
   enforced: EnforcedGates | undefined;
   apiSources: readonly ApiSource[] | undefined;
   /** The model's givens, for which are `#(secure)`. */
   modelGivens: readonly ApiGiven[] | undefined;
   source: string | undefined;
   givens: Record<string, GivenValue> | undefined;
   bypassAuthorize: boolean;
   grafts: ReadonlyArray<{ givenNames: readonly string[] }>;
   rowCount: number;
   /** The view the request named on its source, for the workload shape. */
   view?: string;
}): Promise<QueryAccessSummary> {
   const apiSource =
      !args.enforced && args.source
         ? args.apiSources?.find((s) => s.name === args.source)
         : undefined;
   const authorize = args.enforced?.authorize ?? apiSource?.authorize ?? [];
   const accessFilter =
      args.enforced?.accessFilter ?? apiSource?.accessFilter ?? [];
   const gateGivenNames = Array.from(
      new Set(args.grafts.flatMap((g) => Array.from(g.givenNames))),
   ).sort(byCodepoint);
   const decision: QueryAccessSummary["decision"] = args.bypassAuthorize
      ? "bypassed"
      : args.grafts.length > 0
        ? "filtered"
        : authorize.length > 0 || accessFilter.length > 0
          ? "admitted"
          : "open";

   let fieldsRead: FieldRead[] = [];
   let fieldsReadComplete = false;
   let shape: WorkloadShape | undefined;
   try {
      const prepared = await readPreparedQuery(args.runnable);
      try {
         shape = workloadShapeOf({
            query: prepared.query,
            modelDef: prepared.modelDef ?? args.modelDef,
            source: args.source,
            view: args.view,
         });
      } catch {
         // The record is emitted without a shape rather than not at all.
      }
      // The compiled model, which holds a source the caller declared in the query.
      const reads = readQueryFieldReads(
         prepared.modelDef ?? args.modelDef,
         prepared.query,
      );
      if (reads) {
         const { lineage, paths } = reads;
         fieldsReadComplete = reads.complete && !lineage.truncated;
         const found = new Map<string, FieldRead>();
         const add = (path: string, classifications: string[]): void => {
            if (found.has(path)) return;
            const dropped = lineage.get(path)?.dropped ?? [];
            found.set(path, {
               path,
               classifications,
               ...(dropped.length > 0 ? { dropped } : {}),
            });
         };
         for (const path of paths) {
            const known = lineage.get(path);
            if (!known) {
               fieldsReadComplete = false;
               continue;
            }
            // A view or a join is not a field a caller read.
            if (!known.isField) continue;
            if (known.unresolved.length > 0 && known.declared.length === 0) {
               fieldsReadComplete = false;
            }
            add(path, known.classifications);
            // A derived field reads the classified fields it is built from.
            for (const input of known.inputs) {
               add(input.field, input.classifications);
            }
         }
         fieldsRead = Array.from(found.values()).sort((a, b) =>
            byCodepoint(a.path, b.path),
         );
      }
   } catch {
      // The record is emitted with no fields rather than not at all.
   }

   const secureNames = new Set(
      (args.modelGivens ?? [])
         .filter((g) => g.secure === true && g.name)
         .map((g) => g.name as string),
   );
   const secureGivens = Object.fromEntries(
      Object.entries(args.givens ?? {})
         .filter(([name]) => secureNames.has(name))
         .sort(([a], [b]) => byCodepoint(a, b)),
   );
   return {
      ...(args.source ? { source: args.source } : {}),
      gates: { authorize: [...authorize], accessFilter: [...accessFilter] },
      decision,
      givenNames: Object.keys(args.givens ?? {}).sort(byCodepoint),
      ...(Object.keys(secureGivens).length > 0 ? { secureGivens } : {}),
      gateGivenNames,
      fieldsRead,
      fieldsReadComplete,
      rowCount: args.rowCount,
      ...(shape ? { shape } : {}),
   };
}

/** How the route answered, from the gate's point of view. */
type AccessOutcome =
   | "ok"
   | "denied_lock"
   | "refused_surface"
   | "bad_request"
   | "cancelled"
   | "error";

/** The outcome a thrown error means, by its kind; the status it maps to is the route's. */
export function outcomeOf(error: unknown, status: number): AccessOutcome {
   const kind = errorKind(error);
   if (kind === "access_denied") return "denied_lock";
   if (kind === "model_not_found" || status === 404) {
      return "refused_surface";
   }
   if (status === 403) return "denied_lock";
   if (status === 400) return "bad_request";
   return "error";
}

/** A table the router considered for a query, as the access record keeps it. */
interface AccessPathCandidate {
   tableId: string;
   kind: string;
   outcome: string;
   reason?: string;
   /** The serve verdict's reason on a `WARNED` skip, when the report gives one. */
   code?: string;
}

/** The access record's copy of a query's path report: see `AccessRecord.path`. */
interface AccessPath {
   /**
    * `via: "served_copy"` when the served copy answered, which is how a query
    * through a row-level gate is answered from a stored table (R3).
    */
   servedBy: { kind: string; tableId?: string; via?: "served_copy" };
   considered: AccessPathCandidate[];
}

/**
 * The access record's copy of a path report: which table answered and each
 * table considered. Structural, so it reads the API's `QueryPath` without this
 * module importing the materialization code that builds it.
 *
 * `undefined` when the query answered live with nothing considered, the
 * common case in a package with nothing materialized: that copy would say no
 * more than `servedFrom` and would add bytes to every record.
 */
/** The shape of a verdict code the record keeps (`storage_mode`, `unreachable`). */
const SAFE_CODE = /^[a-z_]+$/;

export function accessPathOf(report: {
   servedBy: { kind: string; tableId?: string; via?: string };
   considered: readonly {
      tableId: string;
      kind: string;
      outcome: string;
      reason?: string;
      code?: string;
   }[];
}): AccessPath | undefined {
   if (report.servedBy.kind === "live" && report.considered.length === 0) {
      return undefined;
   }
   return {
      servedBy: {
         kind: report.servedBy.kind,
         ...(report.servedBy.tableId !== undefined
            ? { tableId: report.servedBy.tableId }
            : {}),
         // The one value it can take; anything else is dropped.
         ...(report.servedBy.via === "served_copy"
            ? { via: "served_copy" as const }
            : {}),
      },
      considered: report.considered.map((c) => ({
         tableId: c.tableId,
         kind: c.kind,
         outcome: c.outcome,
         ...(c.reason !== undefined ? { reason: c.reason } : {}),
         // A coded value only: anything else is dropped, so free text can
         // never reach the log or the local store.
         ...(c.code !== undefined && SAFE_CODE.test(c.code)
            ? { code: c.code }
            : {}),
      })),
   };
}

/** One emitted record. */
export interface AccessRecord extends Partial<QueryAccessSummary> {
   event: "access";
   environment: string;
   package: string;
   version?: string;
   /** Absent on a route with no model (raw SQL). */
   modelPath?: string;
   /** The connection a raw SQL request ran against. */
   connection?: string;
   /** The named query, when one was run. */
   queryName?: string;
   /** The source named in the request, when one was. */
   requestedSource?: string;
   /** True when the request carried query text. */
   adHoc: boolean;
   caller?: string;
   traceId?: string;
   outcome: AccessOutcome;
   status: number;
   durationMs: number;
   servedFrom?: string | null;
   executionTimeMs?: number;
   /**
    * Warehouse bytes the query scanned, where the backend reports them inline
    * (BigQuery today). Absent is not zero.
    */
   scannedBytes?: number;
   /**
    * What answered the query, from its path report (materialization v2, D1),
    * kept compact: the table that answered and each table considered, with
    * why it was skipped, which is what a workload analysis counts. The query's
    * shape is `shape`; the report's nearby tables and skip details stay on the
    * query result. Absent when the query answered live with nothing
    * considered.
    */
   path?: AccessPath;
   /**
    * True when the query answered but its path report could not be
    * assembled, so an absent `path` reads as "unknown", not as "answered
    * live with nothing considered".
    */
   pathUnavailable?: true;
   /** The refusal's message, on a non-ok outcome. Never a gate's detail: the route already redacts those. */
   error?: string;
}

/**
 * Emit one record: a logger line at `info` with `event: "access"`, and the
 * same facts as attributes on the active span when there is one.
 */
export function emitAccessRecord(record: AccessRecord): void {
   logger.info("Access record", record);
   // The workload log's local store, when it is on; it never throws.
   accessRecordSink()?.add(record);
   const span = trace.getActiveSpan();
   if (!span) return;
   span.setAttributes({
      "publisher.access.outcome": record.outcome,
      "publisher.access.status": record.status,
      "publisher.access.package": record.package,
      ...(record.modelPath
         ? { "publisher.access.model_path": record.modelPath }
         : {}),
      ...(record.source ? { "publisher.access.source": record.source } : {}),
      ...(record.decision
         ? { "publisher.access.decision": record.decision }
         : {}),
      ...(record.caller ? { "publisher.access.caller": record.caller } : {}),
      ...(record.rowCount !== undefined
         ? { "publisher.access.row_count": record.rowCount }
         : {}),
      ...(record.fieldsRead
         ? {
              "publisher.access.fields_read": record.fieldsRead.map(
                 (f) => f.path,
              ),
              "publisher.access.classifications_read": Array.from(
                 new Set(record.fieldsRead.flatMap((f) => f.classifications)),
              ).sort(byCodepoint),
           }
         : {}),
   });
}

declare const recorded: unique symbol;

/**
 * The slot a request's access record is read from. Only {@link withAccessRecord}
 * mints one, so a query cannot run without a record being owed for it. The
 * Model writes what it learned (`summary`, how the answer was served) here and
 * never emits; a route writes what only it knows (`version`).
 */
export interface QueryAudit {
   readonly [recorded]: true;
   summary?: QueryAccessSummary;
   version?: string;
   servedFrom?: string | null;
   executionTimeMs?: number;
   scannedBytes?: number;
   path?: AccessPath;
   pathUnavailable?: true;
}

/** What a route knows before it runs the request. */
export type AccessRecordBase = Omit<
   AccessRecord,
   "event" | "outcome" | "status" | "durationMs"
>;

const mintAudit = (): QueryAudit => ({}) as QueryAudit;

/** For specs that call the Model directly; `access_record_audit.spec` fails any other caller. */
export const unrecordedAudit = mintAudit;

/**
 * Run a query the system issues for itself (the value-index build at package
 * load), which no caller asked for and so is owed no record. Nothing else may
 * call it (`access_record_audit.spec`).
 */
export const runUnrecorded = <T>(
   run: (audit: QueryAudit) => Promise<T>,
): Promise<T> => run(mintAudit());

/** The kinds whose recorded text is what the HTTP path answered (ConnectionError and its subclasses, and rejected credentials). */
const ANSWERED_TEXT_KINDS: ReadonlySet<ErrorKind> = new Set<ErrorKind>([
   "connection",
   "connection_failed",
   "connection_pool_exhausted",
   "connection_auth",
]);

/** A connection error's text is what the HTTP path answers, since a driver quotes the statement back and given values are literals in it. */
function recordedErrorText(error: unknown, answered: string): string {
   const text = ANSWERED_TEXT_KINDS.has(errorKind(error))
      ? answered
      : // Driver errors quote connection config back; the record is a log line.
        redactConnectionSecretShapes(
           String((error as Error)?.message ?? error),
        );
   return text.slice(0, MAX_RECORDED_ERROR_CHARS);
}

/** Emit a record such that a failure to build or write it never replaces the query's own answer. */
function emitBestEffort(build: () => AccessRecord): void {
   try {
      emitAccessRecord(build());
   } catch (failure) {
      try {
         logger.error("Access record not emitted", { error: failure });
      } catch {
         // The sink that failed the record may fail this line too.
      }
   }
}

/**
 * Run one request and emit exactly one access record for it, the only place
 * one is emitted: `ok` when `run` resolves, `cancelled` when `signal` aborted,
 * else the outcome the error maps to. A route wraps its whole request,
 * pre-model steps included, so a refusal before the model is recorded too.
 */
export async function withAccessRecord<T>(
   base: AccessRecordBase,
   signal: AbortSignal | undefined,
   run: (audit: QueryAudit) => Promise<T>,
): Promise<T> {
   const started = performance.now();
   const audit = mintAudit();
   const durationMs = () => Math.round(performance.now() - started);
   let result: T;
   try {
      result = await run(audit);
   } catch (error) {
      emitBestEffort(() => {
         const { status, json } = internalErrorToHttpError(error as Error, {
            log: false,
         });
         return {
            event: "access",
            ...base,
            ...(audit.version ? { version: audit.version } : {}),
            // Set by a refusal that read the query first (the ceiling), so the record says what was read.
            ...audit.summary,
            ...(signal?.aborted
               ? { outcome: "cancelled" as const, status: 499 }
               : { outcome: outcomeOf(error, status), status }),
            durationMs: durationMs(),
            error: recordedErrorText(error, json.message),
         };
      });
      throw error;
   }
   emitBestEffort(() => ({
      event: "access",
      ...base,
      ...(audit.version ? { version: audit.version } : {}),
      ...audit.summary,
      outcome: "ok",
      status: 200,
      durationMs: durationMs(),
      ...(audit.servedFrom !== undefined
         ? { servedFrom: audit.servedFrom }
         : {}),
      ...(audit.executionTimeMs !== undefined
         ? { executionTimeMs: audit.executionTimeMs }
         : {}),
      ...(audit.scannedBytes !== undefined
         ? { scannedBytes: audit.scannedBytes }
         : {}),
      ...(audit.path !== undefined ? { path: audit.path } : {}),
      ...(audit.pathUnavailable ? { pathUnavailable: true as const } : {}),
   }));
   return result;
}

/** Record a request refused before it reached the controller that wraps it (a bad parameter, the concurrency cap). */
export async function recordRefusal(
   base: AccessRecordBase,
   error: Error,
): Promise<void> {
   await withAccessRecord(base, undefined, () => Promise.reject(error)).catch(
      () => undefined,
   );
}
