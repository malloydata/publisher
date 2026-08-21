// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Dashboard,
   encodeResourceUri,
   useRouterClickHandler,
   type DrillNavigation,
} from "@malloy-publisher/sdk";
import { Box } from "@mui/material";
import { useCallback, useMemo, useRef } from "react";
import { useLocation, useNavigate, useSearchParams } from "react-router-dom";

export interface DashboardPageProps {
   environmentName: string;
   packageName: string;
   /** The dashboard's slug: `overview`, not `dashboards/overview.malloy`. */
   dashboardName: string;
}

/**
 * The Console's host for the SDK `Dashboard`.
 *
 * Everything the component externalizes on purpose lands here: the URL sync that
 * makes a filtered dashboard a shareable link, and the drill navigation that
 * turns a slug and a seeded given into a route. The component itself reads
 * nothing from the router.
 */
export default function DashboardPage({
   environmentName,
   packageName,
   dashboardName,
}: DashboardPageProps) {
   const [searchParams] = useSearchParams();
   const navigate = useRouterClickHandler();

   // `setSearchParams` is deliberately not used to WRITE, for the reason
   // NotebookPage documents: it navigates to a search-only target, and a target
   // carrying no fragment resolves to an empty one, so changing a control
   // dropped whatever anchor the reader had arrived on. Navigating with an
   // explicit `hash` keeps it.
   const routerNavigate = useNavigate();
   const location = useLocation();

   // Control values ride in the query string, so a link reproduces the view.
   const givens = useMemo(
      () => Object.fromEntries(searchParams.entries()),
      [searchParams],
   );

   // Names this page has written into the query string, so a control cleared
   // after the dashboard stopped declaring it still gets cleaned up. Same device
   // NotebookPage uses, for the same reason.
   const writtenRef = useRef<Set<string>>(new Set());

   const onGivensChange = useCallback(
      (next: Record<string, string>, managed: readonly string[]) => {
         const ours = new Set([...managed, ...writtenRef.current]);
         writtenRef.current = new Set([
            ...writtenRef.current,
            ...Object.keys(next),
            ...managed,
         ]);
         // Merged into what is already there, not written over it. The query
         // string is not this page's alone: a tracking tag or any other unrelated
         // parameter shares it, and replacing the whole string dropped it, not on
         // the first control change as this comment used to claim, but on LOAD,
         // as soon as a manifest carrying a declared given arrived. It was also
         // inconsistent: on a warm react-query cache the first report never fired
         // and the parameter survived, so whether a link kept its tracking tag
         // depended on whether the reader had opened that dashboard before.
         //
         // Merging is possible here now because `Dashboard` hands over the names
         // it manages alongside the values, the way `Notebook` does. Without that
         // list a host cannot tell a control the reader CLEARED from a parameter
         // that was never its business, which is why this used to replace.
         const merged = new URLSearchParams(location.search);
         for (const name of ours) {
            // `hasOwnProperty`, not `in`: `next` is a plain object literal, so
            // `in` walks its prototype and reports `constructor`, `toString` and
            // friends as present, which would leave a given with one of those
            // names stuck in the address bar after its control was cleared.
            if (!Object.prototype.hasOwnProperty.call(next, name)) {
               merged.delete(name);
            }
         }
         for (const [name, value] of Object.entries(next)) {
            merged.set(name, value);
         }
         routerNavigate(
            // Filtering is not a navigation step: Back should leave the
            // dashboard, not walk back through every filter the user tried.
            { search: merged.toString(), hash: location.hash },
            { replace: true },
         );
      },
      [routerNavigate, location],
   );

   // A drill IS a navigation step, so unlike filtering it pushes history: Back
   // returns to the dashboard the user drilled from. `navigate` also honours
   // cmd/ctrl-click by opening the destination in a new tab.
   const onNavigate = useCallback(
      (target: DrillNavigation, event?: MouseEvent) => {
         const query = new URLSearchParams(target.givens).toString();
         // Every segment encoded. The destination slug comes from a `# drill`
         // tag naming a filename, so it can hold a character that would read as
         // structure in a path or start the query string early; the server
         // percent-encodes the same name when it publishes a dashboard's own
         // URL. The environment and package names are validated on the way in
         // and hold nothing worth encoding, but they are encoded on the same
         // principle as the `pages/` redirect in ModelPage.
         const env = encodeURIComponent(environmentName);
         const pkg = encodeURIComponent(packageName);
         const slug = encodeURIComponent(target.dashboard);
         navigate(
            `/${env}/${pkg}/dashboards/${slug}` + (query ? `?${query}` : ""),
            event,
         );
      },
      [navigate, environmentName, packageName],
   );

   return (
      <Box sx={{ p: 3, maxWidth: 1600, mx: "auto" }}>
         <Dashboard
            resourceUri={encodeResourceUri({ environmentName, packageName })}
            dashboard={dashboardName}
            givens={givens}
            onGivensChange={onGivensChange}
            onNavigate={onNavigate}
            maxResultSize={1024 * 1024}
         />
      </Box>
   );
}
