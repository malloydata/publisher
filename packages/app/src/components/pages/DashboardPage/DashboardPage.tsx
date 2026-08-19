import {
   Dashboard,
   encodeResourceUri,
   useRouterClickHandler,
   type DrillNavigation,
} from "@malloy-publisher/sdk";
import { Box } from "@mui/material";
import { useCallback, useMemo } from "react";
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

   const onGivensChange = useCallback(
      (next: Record<string, string>) => {
         // KNOWN GAP, unlike NotebookPage: the applied values REPLACE the query
         // string rather than merging into it, so an unrelated parameter on a
         // dashboard URL (a tracking tag, say) is dropped on the first control
         // change. Merging needs to know which names are the dashboard's to take
         // back out, and `Dashboard`'s `onGivensChange` hands over values only,
         // where `Notebook`'s hands over the managed names alongside them, which
         // is what lets NotebookPage merge. Guessing is worse than replacing:
         // without that list, a control the reader CLEARS cannot be told from a
         // parameter that was never ours, so its value would stay in the address
         // bar while the page ran without it.
         routerNavigate(
            // Filtering is not a navigation step: Back should leave the
            // dashboard, not walk back through every filter the user tried.
            {
               search: new URLSearchParams(next).toString(),
               hash: location.hash,
            },
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
