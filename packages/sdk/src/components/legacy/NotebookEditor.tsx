// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   ReportEditor,
   type ReportEditorProps,
} from "../ReportBuilder/ReportEditor";

/** The props a host passed the notebook editor before notebooks opened in the one builder. */
export type NotebookEditorProps = (
   | {
        /** `publisher://environments/{env}/packages/{pkg}`, optionally `?versionId=`. */
        resourceUri: string;
        /** The notebook's slug: `overview` for `notebooks/overview.malloy`. */
        notebook: string;
     }
   | {
        environmentName: string;
        packageName: string;
        /** The notebook's slug: `overview` for `notebooks/overview.malloy`. */
        notebookName: string;
     }
) &
   Pick<ReportEditorProps, "onEvent" | "onDirtyChange" | "onExit" | "path">;

/** `ReportEditor` opened on a notebook, under the name hosts already import. */
export function NotebookEditor(props: NotebookEditorProps) {
   const { onEvent, onDirtyChange, onExit, path } = props;
   const target =
      "resourceUri" in props
         ? { resourceUri: props.resourceUri, dashboard: props.notebook }
         : {
              environmentName: props.environmentName,
              packageName: props.packageName,
              dashboardName: props.notebookName,
           };
   return (
      <ReportEditor
         {...target}
         kind="notebook"
         {...(onEvent ? { onEvent } : {})}
         {...(onDirtyChange ? { onDirtyChange } : {})}
         {...(onExit ? { onExit } : {})}
         {...(path !== undefined ? { path } : {})}
      />
   );
}
