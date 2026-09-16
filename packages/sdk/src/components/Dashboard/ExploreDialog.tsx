// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as Malloy from "@malloydata/malloy-interfaces";
import { useMemo } from "react";
import { encodeResourceUri } from "../../utils/formatting";
import { ModelExplorerDialog } from "../Model/ModelExplorerDialog";
import { useModelData } from "../Model/useModelData";
import { sourceOf, stepsOf } from "./RowsDialog";

/**
 * "Explore from here": the tile's query, open in the model explorer on the
 * tile's source, so the reader can change it rather than only look at it.
 *
 * The dashboard file is itself a model, so the explorer opens on that file:
 * the tile's source is an extension declared there, and its query resolves
 * as written. The model is fetched here rather than inside the explorer so
 * the tile's source can be selected before it renders.
 */
export function ExploreDialog({
   tile,
   environmentName,
   packageName,
   versionId,
   modelPath,
   onClose,
}: {
   /** The tile expression to open, or undefined when closed. */
   tile: string | undefined;
   environmentName: string;
   packageName: string;
   versionId?: string;
   modelPath: string;
   onClose: () => void;
}) {
   const resourceUri = encodeResourceUri({
      environmentName,
      packageName,
      modelPath,
      ...(versionId === undefined ? {} : { versionId }),
   });
   const { data } = useModelData(resourceUri, tile !== undefined);
   const sourceIndex = useMemo(() => {
      const source = sourceOf(tile);
      if (!data || source === undefined) return 0;
      const index = (data.sourceInfos ?? []).findIndex((info) => {
         try {
            return JSON.parse(info)?.name === source;
         } catch {
            return false;
         }
      });
      return index < 0 ? 0 : index;
   }, [data, tile]);
   // As a query AST when the expression is `source -> view`, so the explorer's
   // builder shows the view and its Run button works from the first render; a
   // string otherwise, which the explorer runs as written but cannot show.
   const existingQuery = useMemo(() => {
      if (tile === undefined) return undefined;
      const steps = stepsOf(tile);
      const malloyQuery: Malloy.Query | string = steps
         ? {
              definition: {
                 kind: "arrow",
                 source: { kind: "source_reference", name: steps.source },
                 view: { kind: "view_reference", name: steps.view },
              },
           }
         : `run: ${tile}`;
      return { query: `run: ${tile}`, malloyQuery, malloyResult: undefined };
   }, [tile]);
   if (tile === undefined || !data) return null;
   return (
      <ModelExplorerDialog
         open
         onClose={onClose}
         resourceUri={resourceUri}
         data={data}
         title={`Explore: ${tile}`}
         existingQuery={existingQuery}
         initialSelectedSourceIndex={sourceIndex}
      />
   );
}
