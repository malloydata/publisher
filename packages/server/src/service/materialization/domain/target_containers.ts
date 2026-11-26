// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The container (a BigQuery dataset, a schema) a database table's `name=`
 * places it in, and whether its warehouse holds it: read once per connection
 * and container when a package is planned (run/target_container_read.ts), so
 * a table whose container does not exist is refused at plan
 * (`container_not_found`) rather than failing its build.
 *
 * Types and pure helpers only.
 */

import type { Connection } from "@malloydata/malloy";
import { capabilitiesOf } from "./dialect";

/** One container a database table's `name=` names, on its connection. */
export interface TargetContainer {
   connectionName: string;
   /**
    * Every segment of the `name=` before the table's own, as written: a
    * dataset or schema (`analytics`), or a project and dataset, or a
    * database and schema (`proj.analytics`).
    */
   container: string;
}

/** A target container's key in its readings. */
export function targetContainerKey(target: TargetContainer): string {
   return JSON.stringify([target.connectionName, target.container]);
}

/**
 * What a read of one container found. `found` and `unread` refuse nothing:
 * a read that failed (permissions, network, a timeout), or a dialect this
 * read does not check, fails open, and the build finds out. `notFound`: the
 * warehouse holds no such container. `otherLocation`: a BigQuery dataset in
 * another location than the connection declares, which no query of the
 * connection's can write into.
 */
export type TargetContainerReading =
   | { found: true }
   | { unread: true }
   | {
        notFound: true;
        /** The BigQuery project read, when the container did not name one. */
        project?: string;
     }
   | { otherLocation: { location: string; expected: string } };

/** Readings by {@link targetContainerKey}. */
export type TargetContainerReadings = Record<string, TargetContainerReading>;

/** What a container read needs of a connection. */
export type TargetContainerConnection = Pick<
   Connection,
   "runSQL" | "dialectName"
>;

/**
 * Reads target containers on the main thread (run/target_container_read.ts).
 * The package-load pool answers a worker's request with the one its job was
 * handed, so it imports no run/ code.
 */
export type TargetContainerReader = (args: {
   containers: readonly TargetContainer[];
   connection: (connectionName: string) => Promise<TargetContainerConnection>;
}) => Promise<TargetContainerReadings>;

/**
 * The container of a database table name (`ds.t`, `proj.ds.t`): every
 * segment before the last, or undefined for a bare name, which lands in the
 * connection's default schema or dataset.
 */
export function containerOfTableName(name: string): string | undefined {
   const dot = name.lastIndexOf(".");
   return dot > 0 ? name.slice(0, dot) : undefined;
}

/**
 * Why a table's container refuses it, and what fixes it: the words of a
 * `container_not_found` refusal, and of the build's own check
 * (`ContainerNotFoundError`).
 */
export function missingContainerReason(
   target: TargetContainer,
   dialect: string,
   reading:
      | { notFound: true; project?: string }
      | { otherLocation: { location: string; expected: string } },
): string {
   const kind =
      capabilitiesOf(dialect).containerCatalog === "dataset"
         ? "dataset"
         : "schema";
   const where = `its name= places its table in ${kind} "${target.container}"`;
   if ("otherLocation" in reading) {
      const { location, expected } = reading.otherLocation;
      return (
         `${where}, which is in location "${location}", but connection ` +
         `"${target.connectionName}" runs its queries in "${expected}"; ` +
         `create the ${kind} in "${expected}", or change name= to a ${kind} ` +
         `there`
      );
   }
   const project = reading.project ? ` (project "${reading.project}")` : "";
   return (
      `${where}${project}, which connection "${target.connectionName}" does ` +
      `not hold, and no build creates one; create the ${kind}, or change ` +
      `name= to one that exists`
   );
}
