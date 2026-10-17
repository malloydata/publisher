// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   type ModelDef,
   type ModelMaterializer,
   type NamedQueryDef,
   type QueryMaterializer,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import { logger } from "../logger";
import { modelInfoOf } from "./model_info";
import { type NotebookCellKind } from "./notebook";

/** Registry ids and declaration names of every given a cell's closure declares. */
export interface CellDeclaredGivens {
   ids: ReadonlySet<string>;
   names: ReadonlySet<string>;
}

export function declaredGivensPerCell(
   cells: readonly RunnableNotebookCell[],
): (CellDeclaredGivens | undefined)[] {
   return cells.map((cell) => {
      if (!cell.modelDef) return undefined;
      const registry = cell.modelDef.givens ?? {};
      return {
         ids: new Set(Object.keys(registry)),
         names: new Set(Object.values(registry).map((g) => g.name)),
      };
   });
}

/** The `queryInfo` a plain cell gets from `anonymous_queries`, for a run compiled on its own on top of the model. */
export async function tileQueryInfo(
   materializer: ModelMaterializer,
   text: string,
): Promise<Malloy.QueryInfo | undefined> {
   try {
      const def = (await materializer.extendModel(text).getModel())._modelDef;
      const anonymous = modelInfoOf(def).anonymous_queries;
      const last = anonymous?.[anonymous.length - 1];
      const compiled = def.queryList[def.queryList.length - 1] as
         | NamedQueryDef
         | undefined;
      return last && { ...last, name: compiled?.as || compiled?.name || "" };
   } catch (error) {
      // The cell still runs on its own; only its description is missing.
      logger.warn("Could not describe a layout notebook tile's query", {
         text,
         error,
      });
      return undefined;
   }
}

export interface RunnableNotebookCell {
   type: "code" | "markdown";
   /** Set on a served notebook's cells; a `.malloynb` cell has none. */
   kind?: NotebookCellKind;
   text: string;
   /** A served code cell's own `#(markdown)` prose; author text, so it is served whatever the surface withholds of `queryInfo`. */
   markdown?: string;
   /** The 0-based inclusive lines of `text` that hold `markdown`, as the reader saw them. */
   proseLines?: [number, number][];
   codeLine?: number;
   /** The joined text of the statement's leading `#"` lines. */
   caption?: string;
   /** A served query cell's index into `modelInfo.anonymous_queries`. */
   queryIndex?: number;
   runnable?: QueryMaterializer;
   /** Retained so we can rebuild the query with filter refinements at execution time. */
   modelMaterializer?: ModelMaterializer;
   /**
    * This cell's own compiled modelDef — the model state AFTER this cell's
    * own declarations, i.e. what `modelMaterializer` above was hydrated
    * from. Retained for TWO graft roles (see `Model.graftScopeForCell` /
    * `Model.selfGraftScopeForCell` / `Model.resolveNotebookCellGraftScope`):
    *  - as a LATER cell's EARLIER scope: a cell's text compiles against
    *    whatever existed BEFORE its own declarations, so the correct graft
    *    scope for recompiling a LATER cell's text is an EARLIER cell's own
    *    (modelDef, modelMaterializer) pair, never that later cell's own —
    *    which already contains whatever it itself just declared, so
    *    grafting it and recompiling that cell's own `source:`+`run:` text
    *    against it fails with "Cannot redefine".
    *  - as THIS cell's OWN fallback scope, when no earlier cell can cover
    *    its gate at all (this cell declares AND runs its own gated source):
    *    bound by repointing this cell's already-compiled queryDef via
    *    `_loadQueryFromQueryDef`, never by recompiling text against it — the
    *    same "Cannot redefine" collision would occur if it were recompiled,
    *    but the repoint mechanism sidesteps it entirely by never
    *    re-parsing any text.
    *  - its `.givens` (transitive through every import) is every given
    *    anything this cell runs can reference.
    */
   modelDef?: ModelDef;
   newSources?: Malloy.SourceInfo[];
   queryInfo?: Malloy.QueryInfo;
   /** A layout tile's `queryInfo`, compiled on first read: its run is a string in the tag, so no `anonymous_queries` entry describes it. */
   deriveQueryInfo?: () => Promise<Malloy.QueryInfo | undefined>;
}

/** What running one notebook cell answers; `kind` only on a served notebook's cell. */
export interface NotebookCellRunResult {
   type: "code" | "markdown";
   kind?: NotebookCellKind;
   text: string;
   markdown?: string;
   proseLines?: [number, number][];
   codeLine?: number;
   caption?: string;
   queryName?: string;
   result?: string;
   newSources?: string[];
}
