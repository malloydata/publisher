// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The cells a model holds: a `.malloynb`'s, or the ones discovery attached to
 * a linear `.malloy` report. No route reads a `.malloy` report as cells, so a
 * spec about those cells reads them off the model. Throws when the model or
 * its field is missing, so a rename fails the spec rather than reading as no
 * cells.
 */

import type { Model, RunnableNotebookCell } from "../service/model";

const FIELD = "runnableNotebookCells";

export function attachedCells(
   model: Model | undefined,
): RunnableNotebookCell[] {
   if (!model) throw new Error("attachedCells: no model");
   if (!Object.hasOwn(model, FIELD)) {
      throw new Error(`attachedCells: Model has no \`${FIELD}\` field`);
   }
   return (
      (model as unknown as { [FIELD]?: RunnableNotebookCell[] })[FIELD] ?? []
   );
}
