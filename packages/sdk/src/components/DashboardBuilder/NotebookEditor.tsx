// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DashboardEditor, type DashboardEditorProps } from "./DashboardEditor";

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
   Pick<DashboardEditorProps, "onExit" | "onEvent" | "onDirtyChange" | "path">;

/** `DashboardEditor` opened on a notebook, under the name hosts already import. */
export function NotebookEditor(props: NotebookEditorProps) {
   const { onExit, onEvent, onDirtyChange, path } = props;
   const target =
      "resourceUri" in props
         ? { resourceUri: props.resourceUri, dashboard: props.notebook }
         : {
              environmentName: props.environmentName,
              packageName: props.packageName,
              dashboardName: props.notebookName,
           };
   return (
      <DashboardEditor
         {...target}
         kind="notebook"
         {...(onExit ? { onExit } : {})}
         {...(onEvent ? { onEvent } : {})}
         {...(onDirtyChange ? { onDirtyChange } : {})}
         {...(path !== undefined ? { path } : {})}
      />
   );
}
