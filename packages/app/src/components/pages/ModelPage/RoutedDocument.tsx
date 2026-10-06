// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Loading, useDocumentLocation } from "@malloy-publisher/sdk";
import DashboardEditPage from "../DashboardEditPage/DashboardEditPage";
import DashboardPage from "../DashboardPage/DashboardPage";
import NotebookPage from "../NotebookPage/NotebookPage";

/**
 * A `dashboards/<slug>` or `notebooks/<slug>` route, opened as what the package
 * lists the document as: the artifact tag decides the kind and the file's folder
 * is only where it happens to live. A document no listing has opens as the route says.
 */
export default function RoutedDocument({
   environmentName,
   packageName,
   routeKind,
   slug,
   edit,
}: {
   environmentName: string;
   packageName: string;
   routeKind: "dashboard" | "notebook";
   slug: string;
   edit: boolean;
}) {
   const { location, settled } = useDocumentLocation(
      environmentName,
      packageName,
      routeKind,
      slug,
   );
   if (!settled) return <Loading text="Opening..." />;
   const kind = location?.kind ?? routeKind;
   if (edit)
      return (
         <DashboardEditPage
            environmentName={environmentName}
            packageName={packageName}
            dashboardName={slug}
            kind={kind}
            {...(location ? { path: location.path } : {})}
         />
      );
   if (kind === "dashboard")
      return (
         <DashboardPage
            environmentName={environmentName}
            packageName={packageName}
            dashboardName={slug}
         />
      );
   return (
      <NotebookPage
         environmentName={environmentName}
         packageName={packageName}
         notebookPath={location?.path ?? `notebooks/${slug}.malloy`}
      />
   );
}
