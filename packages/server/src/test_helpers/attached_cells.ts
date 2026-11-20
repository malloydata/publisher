// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The cells a model holds: a `.malloynb`'s, or the ones discovery attached to
 * a linear `.malloy` report. No route reads a `.malloy` report as cells, so a
 * spec about those cells reads them off the model.
 */

import type { Model, RunnableNotebookCell } from "../service/model";

export function attachedCells(
   model: Model | undefined,
): RunnableNotebookCell[] {
   return (
      (
         model as unknown as
            | { runnableNotebookCells?: RunnableNotebookCell[] }
            | undefined
      )?.runnableNotebookCells ?? []
   );
}
