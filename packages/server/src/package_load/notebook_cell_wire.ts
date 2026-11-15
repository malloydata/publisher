// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One notebook cell as the package-load worker sends it to the main thread.
 * A leaf, so the model runtime that hydrates a cell need not import the
 * whole load protocol.
 */
export interface SerializedNotebookCell {
   type: "code" | "markdown";
   /** Raw cell text. */
   text: string;
   /**
    * The cell's ModelDef at its point in the `extendModel` chain, hydrated by
    * the main thread so cell-level queries compile in the right scope.
    */
   cellModelDef?: unknown;
   /** The cell's final QueryDef, hydrated by the main thread without recompiling. */
   cellQueryDef?: unknown;
   newSources?: unknown[];
   queryInfo?: unknown;
}
