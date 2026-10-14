// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createContext, useContext, useMemo, useRef } from "react";
import type { ReportLayout } from "../Package/documentLocation";

/**
 * Where a Save lands, named by which storage took the write: `package` is the
 * package file on the server, `browser` the Console's own browser store, and
 * `host` a host storage that holds the record. Separate because a save into
 * the package or the record is a change every reader sees, while a browser
 * save is one person's copy on one machine.
 */
export type SavesTo = "package" | "browser" | "host";

/**
 * Why something failed, as a category a sink can count. Never the error's
 * message: that can carry a path, a value or a query the reader typed.
 */
export type ReportFailure =
   | "not_found"
   | "forbidden"
   | "invalid"
   | "unsupported"
   | "server"
   | "network"
   | "unknown";

/** What a `# drill` click came to. */
export type DrillOutcome =
   /** It reached a destination: a report the listing has, or this report's own control. */
   | "resolved"
   /** A well-formed destination nothing answers to. */
   | "unresolved"
   /**
    * It navigated to the destination as written before the host could say
    * whether a report is there (its listing not read yet, or unreadable). Not
    * a failure: the page it lands on says what is there.
    */
   | "unverified"
   /** A target or value that cannot be a destination at all. */
   | "malformed";

/**
 * Which report an event is about, on every event. `path` is the report's
 * package path (`sales/q3.malloy`); the host that mounted the surface knows
 * the environment and package and adds them when it logs. `layout` is the
 * report's layout when the event happened, which an editor's layout switch
 * changes.
 */
export interface ReportEventContext {
   path: string;
   layout: ReportLayout;
}

/**
 * What a report surface reports about itself, for the host to log or count:
 * one event per outcome a reader or author would recognise, each with how long
 * it took where that means anything. Delivered through the `onEvent` prop of
 * `Report`, `ReportEditor`, `ReportBuilder`, `NewDocumentDialog` and
 * `Package`.
 */
export type ReportEvent =
   | {
        /**
         * The report could not be opened. `layout` is absent when it failed
         * before the report said what it is.
         */
        type: "report.load_failed";
        path: string;
        layout?: ReportLayout;
        surface: "view" | "edit";
        category: ReportFailure;
     }
   | (ReportEventContext &
        (
           | {
                type: "report.opened";
                /** The reader's view, or the builder. */
                surface: "view" | "edit";
                /**
                 * The package file, a draft the host had saved beside it, or the
                 * host's own copy when that copy is the record.
                 */
                from: "package" | "draft" | "record";
                /** Tiles, or cells on a report written as `run:` cells. */
                tiles: number;
                durationMs: number;
             }
           | {
                /** A tile's (or cell's) query failed; `tile` absent means the run as a whole. */
                type: "report.query_failed";
                tile?: number;
                category: ReportFailure;
             }
           | {
                type: "report.rows_shown";
                source: string;
                view: string;
                field: string;
                ok: boolean;
                durationMs: number;
             }
           | { type: "report.explored"; tile: string }
           | {
                type: "report.saved";
                tiles: number;
                /** Whether a tile was added or removed, which moves declarations. */
                structural: boolean;
                where: SavesTo;
                /** The host workspace that took the write; never set for a package save. */
                workspace?: string;
                durationMs: number;
             }
           | { type: "report.save_refused"; reason: string }
           | {
                /** Undo save wrote the file back as it was before the last save. */
                type: "report.save_undone";
                tiles: number;
                structural: boolean;
                where: SavesTo;
                workspace?: string;
                durationMs: number;
             }
           | { type: "report.save_undo_refused"; reason: string }
           | {
                type: "report.created";
                /** Which storage took the write, as on `report.saved`. */
                where: "package" | "host";
                durationMs: number;
             }
           | { type: "report.create_refused"; reason: string }
           | {
                /** The author switched the report between Linear and Grid; `layout` is `to`. */
                type: "report.layout_changed";
                from: ReportLayout;
                to: ReportLayout;
             }
           | {
                type: "report.drill";
                /** `self` filtered this report; `report` went to another. */
                to: "self" | "report";
                outcome: DrillOutcome;
             }
        ));

export type ReportEventHandler = (event: ReportEvent) => void;

type WithoutContext<E> = E extends unknown
   ? Omit<E, keyof ReportEventContext>
   : never;

/** An event before the surface stamps it with the report it is about. */
export type ReportEventBody = WithoutContext<ReportEvent>;

/** Which report a surface emits for: its layout is unknown until it has read the report. */
export type ReportEventTarget = Omit<ReportEventContext, "layout"> &
   Partial<Pick<ReportEventContext, "layout">>;

/** Stamp an event with its report. */
export const withReportContext = (
   body: ReportEventBody,
   target: ReportEventTarget,
): ReportEvent => ({ ...target, ...body }) as ReportEvent;

/**
 * How a surface deep inside a report emits: the report fills in which report
 * it is, and a sink's own failure never reaches the reader.
 */
export interface ReportEmitter {
   emit: (body: ReportEventBody) => void;
   /**
    * Whether a host is listening. A diagnostic with no other symptom (a cell
    * that failed, a drill that went nowhere) goes to the console only when no
    * host is, so it is never lost and never said twice.
    */
   listening: boolean;
}

const SILENT: ReportEmitter = { emit: () => {}, listening: false };

/** An emitter for one report, or a silent one when no host listens. */
export function reportEmitter(
   target: ReportEventTarget,
   onEvent: ReportEventHandler | undefined,
): ReportEmitter {
   if (!onEvent) return SILENT;
   return {
      listening: true,
      emit: (body) => {
         try {
            onEvent(withReportContext(body, target));
         } catch {
            // A sink that throws is the sink's problem, not the reader's.
         }
      },
   };
}

/**
 * {@link reportEmitter} for a component: stable while the report and whether
 * anyone listens stay the same, so an effect that emits on a change does not
 * emit again because the host passed a new handler.
 */
export function useReportEmitter(
   target: ReportEventTarget,
   onEvent: ReportEventHandler | undefined,
): ReportEmitter {
   const onEventRef = useRef(onEvent);
   onEventRef.current = onEvent;
   const listening = onEvent !== undefined;
   const { path, layout } = target;
   return useMemo(
      () =>
         reportEmitter(
            { path, ...(layout ? { layout } : {}) },
            listening ? (event) => onEventRef.current?.(event) : undefined,
         ),
      [path, layout, listening],
   );
}

/** Set by `Report` around what it renders; silent outside one. */
export const ReportEventsContext = createContext<ReportEmitter>(SILENT);

export const useReportEvents = (): ReportEmitter =>
   useContext(ReportEventsContext);

/** The category of a failed request, from its HTTP status or the lack of one. */
export function failureCategory(error: unknown): ReportFailure {
   const candidate = error as
      | { status?: unknown; response?: { status?: unknown }; code?: unknown }
      | undefined;
   const status =
      typeof candidate?.status === "number"
         ? candidate.status
         : typeof candidate?.response?.status === "number"
           ? candidate.response.status
           : undefined;
   if (status === undefined)
      return candidate?.code === "ERR_NETWORK" ? "network" : "unknown";
   if (status === 404) return "not_found";
   if (status === 401 || status === 403) return "forbidden";
   if (status === 501) return "unsupported";
   if (status >= 500) return "server";
   if (status >= 400) return "invalid";
   return "unknown";
}
