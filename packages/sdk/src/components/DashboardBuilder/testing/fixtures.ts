// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DashboardDocument } from "../document";
import { readDashboardDocument, readFailed } from "../readDocument";
import {
   spliceDashboardDocument,
   spliceFailed,
   type SpliceResult,
} from "../spliceDocument";

/** The document a file produces, or a thrown reason: what a spec starts from. */
export async function openDocument(source: string): Promise<DashboardDocument> {
   const result = await readDashboardDocument(source);
   if (readFailed(result))
      throw new Error(`expected a document: ${result.reason}`);
   return result.document;
}

/** Open, edit a copy, and hand back what the writer says. */
export async function splice(
   source: string,
   edit: (document: DashboardDocument) => void,
): Promise<SpliceResult> {
   const next = structuredClone(await openDocument(source));
   edit(next);
   return spliceDashboardDocument(source, next);
}

/** `splice`, for an edit the writer is expected to accept. */
export async function spliced(
   source: string,
   edit: (document: DashboardDocument) => void,
): Promise<string> {
   const result = await splice(source, edit);
   if (spliceFailed(result)) throw new Error(result.reason);
   return result.source;
}

/** `splice`, for an edit the writer is expected to refuse; its reason. */
export async function refused(
   source: string,
   edit: (document: DashboardDocument) => void,
): Promise<string> {
   const result = await splice(source, edit);
   if (!spliceFailed(result)) throw new Error("expected the writer to refuse");
   return result.reason;
}
