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
import { AccessDeniedError, ModelNotFoundError } from "../errors";
import { logger } from "../logger";
import { settings } from "../settings";
import {
   fieldPathsIn,
   findIrSource,
   readSourceClassifications,
} from "./classification";

type ApiSource = components["schemas"]["Source"];
type ApiGiven = components["schemas"]["Given"];

/** The request header the deployment names for the caller identity, lowercased. */
export function callerHeaderName(): string {
   return settings.get("callerHeader").toLowerCase();
}

/** A field the compiled query read, with its classes as the model declares or infers them. */
export interface FieldRead {
   path: string;
   classifications: string[];
}

/**
 * What the query service knows about one admitted query's access, built
 * once the gates have run and the query has returned.
 */
export interface QueryAccessSummary {
   /** The source the query entered, when it could be resolved. */
   source?: string;
   /** The gates in force on that source, as the model declares them. */
   gates: { authorize: string[]; accessFilter: string[] };
   /**
    * `open`: no gate on the source. `admitted`: a lock admitted the caller and
    * no row filter applied. `filtered`: a row filter was grafted (a lock may
    * also have admitted). `bypassed`: the data-management header skipped the
    * gates.
    */
   decision: "open" | "admitted" | "filtered" | "bypassed";
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
   /** Rows returned, after the cap. */
   rowCount: number;
}

const byCodepoint = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

/**
 * Build the summary for an admitted query. Reads the prepared query's IR for
 * the fields it references and resolves each against the entry source's
 * lineage, so a reference through a join (`patient.ssn`) carries the class
 * declared where the field lives. Anything unresolvable is simply absent.
 */
export async function summarizeQueryAccess(args: {
   runnable: { getPreparedQuery(): Promise<unknown> };
   modelDef: ModelDef | undefined;
   apiSources: readonly ApiSource[] | undefined;
   /** The model's givens, for which are `#(secure)`. */
   modelGivens: readonly ApiGiven[] | undefined;
   source: string | undefined;
   givens: Record<string, GivenValue> | undefined;
   bypassAuthorize: boolean;
   grafts: ReadonlyArray<{ givenNames: readonly string[] }>;
   rowCount: number;
}): Promise<QueryAccessSummary> {
   const apiSource = args.source
      ? args.apiSources?.find((s) => s.name === args.source)
      : undefined;
   const authorize = apiSource?.authorize ?? [];
   const accessFilter = apiSource?.accessFilter ?? [];
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
   try {
      const prepared = (await args.runnable.getPreparedQuery()) as {
         _query?: unknown;
      };
      const lineage = args.source
         ? readSourceClassifications(
              findIrSource(args.modelDef, args.source)?.fields,
           )
         : undefined;
      const paths = new Set(
         fieldPathsIn(prepared._query).map((p) => p.join(".")),
      );
      fieldsRead = Array.from(paths)
         .sort(byCodepoint)
         .flatMap((path) => {
            const known = lineage?.get(path);
            // Only dimensions and measures of the entry source (own or
            // joined): a reference the lineage cannot place is not a field a
            // caller read from this source.
            if (!known || !known.isField) return [];
            return [{ path, classifications: known.classifications }];
         });
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
      rowCount: args.rowCount,
   };
}

/** How the route answered, from the gate's point of view. */
export type AccessOutcome =
   | "ok"
   | "denied_lock"
   | "refused_surface"
   | "bad_request"
   | "error";

/** The outcome a thrown error means, by its class; the status it maps to is the route's. */
export function outcomeOf(error: unknown, status: number): AccessOutcome {
   if (error instanceof AccessDeniedError) return "denied_lock";
   if (error instanceof ModelNotFoundError || status === 404) {
      return "refused_surface";
   }
   if (status === 403) return "denied_lock";
   if (status === 400) return "bad_request";
   return "error";
}

/** One emitted record. */
export interface AccessRecord extends Partial<QueryAccessSummary> {
   event: "access";
   environment: string;
   package: string;
   version?: string;
   modelPath: string;
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
   /** The refusal's message, on a non-ok outcome. Never a gate's detail: the route already redacts those. */
   error?: string;
}

/**
 * Emit one record: a logger line at `info` with `event: "access"`, and the
 * same facts as attributes on the active span when there is one.
 */
export function emitAccessRecord(record: AccessRecord): void {
   logger.info("Access record", record);
   const span = trace.getActiveSpan();
   if (!span) return;
   span.setAttributes({
      "publisher.access.outcome": record.outcome,
      "publisher.access.status": record.status,
      "publisher.access.package": record.package,
      "publisher.access.model_path": record.modelPath,
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
