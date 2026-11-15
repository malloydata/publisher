// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where an emitted access record goes besides the log: the workload log's
 * local store, when it is on (workload_log_store.ts). A leaf, so the record
 * module needs nothing from the store that reads it.
 */

/** What takes each emitted record; it must never throw. */
export interface AccessRecordSink {
   add(record: object): void;
}

let sink: AccessRecordSink | undefined;

/** The sink in force, or none. */
export function accessRecordSink(): AccessRecordSink | undefined {
   return sink;
}

/** Install (or with `undefined`, remove) the sink. */
export function setAccessRecordSink(next: AccessRecordSink | undefined): void {
   sink = next;
}
