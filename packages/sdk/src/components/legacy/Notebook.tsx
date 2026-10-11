// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { parseResourceUri } from "../../utils/formatting";
import type { CellReportProps } from "../Report/CellReport";
import { LinearReport } from "../Report/LinearReport";
import { MalloynbCells, useNotebookQuery } from "./MalloynbCells";

type NotebookProps = CellReportProps;

/**
 * A notebook by its package path, read through the deprecated
 * `GET …/notebooks/{path}`: a tile layout drawn as a linear report, anything
 * else (a `.malloynb`, or a report written as `run:` cells) cell by cell
 * through the deprecated cell route.
 *
 * @deprecated Render `Report` with the file's path instead: it reads
 * `GET …/reports/{path}` and renders either layout. Kept, over the same
 * renderers, for hosts that still mount it and for the `.malloynb` format,
 * which only these routes run.
 */
// Requires PackageProvider
export default function Notebook(props: NotebookProps) {
   const { data: notebook } = useNotebookQuery(props.resourceUri);
   // A notebook written as a tile layout renders as a one-column grid.
   if (notebook?.dashboard) {
      const { environmentName, packageName, versionId, modelPath } =
         parseResourceUri(props.resourceUri);
      return (
         <LinearReport
            manifest={notebook.dashboard}
            environmentName={environmentName}
            packageName={packageName}
            versionId={versionId}
            documentName={modelPath}
            givens={props.givens}
            onGivensChange={props.onGivensChange}
            onDrillNavigate={props.onDrillNavigate}
            maxResultSize={props.maxResultSize}
         />
      );
   }
   return <MalloynbCells {...props} />;
}
