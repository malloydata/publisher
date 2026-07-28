import {
  Dashboard,
  encodeResourceUri,
  type DrillNavigation,
} from "@malloy-publisher/sdk";
import { Alert, Box, Stack, Tab, Tabs, Typography } from "@mui/material";
import { useCallback, useEffect, useState } from "react";
import Header from "./Header";
import type { AppView } from "../types/view";

const ENVIRONMENT = "examples";
const PACKAGE = "storefront";

type DashboardSummary = { name: string; title?: string };

/**
 * A package's own `dashboards/*.malloy`, rendered by the SDK's `<Dashboard>`.
 *
 * The other examples here build a UI out of individual query results. This one
 * renders an artifact the *package* declares: the dashboard's layout, filter
 * controls, and drill behaviour come from tags in
 * `examples/storefront/dashboards/`, so this component chooses which dashboard
 * to show and nothing else. Adding a control or a tile is a model change, with
 * no edit here.
 *
 * It is also the proof that `<Dashboard>` is host-agnostic. The Publisher
 * Console renders the same component and differs only in what it does with the
 * two callbacks: the Console has a router, so filter state goes in the query
 * string and a drill becomes a route. This app has no router, so both land in
 * React state — the component reads neither.
 */
export default function PackageDashboard({
  selectedView,
}: {
  selectedView: AppView;
}) {
  const [dashboards, setDashboards] = useState<DashboardSummary[]>([]);
  const [selected, setSelected] = useState<string>();
  const [givens, setGivens] = useState<Record<string, string>>({});
  const [error, setError] = useState<string>();

  useEffect(() => {
    fetch(`/api/v0/environments/${ENVIRONMENT}/packages/${PACKAGE}/dashboards`)
      .then((res) => (res.ok ? res.json() : Promise.reject(res.statusText)))
      .then((list: DashboardSummary[]) => {
        setDashboards(list);
        // The listing is alphabetical, which would open on "Category Detail"
        // with no category picked. This package's overview is the landing page.
        setSelected(
          (current) =>
            current ??
            list.find((d) => d.name === "overview")?.name ??
            list[0]?.name,
        );
      })
      .catch((err) => setError(String(err)));
  }, []);

  // A drill carries both halves: where to go, and the given to arrive with.
  const onNavigate = useCallback((target: DrillNavigation) => {
    setSelected(target.dashboard);
    setGivens(target.givens);
  }, []);

  return (
    <Stack spacing={2} sx={{ mt: { xs: 8, md: 0 }, mb: 8 }}>
      <Header selectedView={selectedView} />

      <Box sx={{ p: 2 }}>
        <Typography variant="h5" sx={{ mb: 1, fontWeight: "bold" }}>
          Package dashboards
        </Typography>
        <Typography variant="body1" sx={{ mb: 2, color: "text.secondary" }}>
          Declared in <code>examples/storefront/dashboards/</code> and rendered
          here by the SDK&apos;s <code>&lt;Dashboard&gt;</code>. Filter the
          category or click a category name in the table to drill — this app
          handles the drill by swapping tabs, where the Console would navigate.
        </Typography>

        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            Could not list dashboards: {error}. Is Publisher running on port
            4000 with the <code>storefront</code> package loaded?
          </Alert>
        )}

        {dashboards.length > 0 && selected && (
          <>
            <Tabs
              value={selected}
              onChange={(_, next: string) => {
                setSelected(next);
                // Controls are per-dashboard, so carrying values across would
                // send a given the next one may not declare.
                setGivens({});
              }}
              sx={{ mb: 2, borderBottom: 1, borderColor: "divider" }}
            >
              {dashboards.map((d) => (
                <Tab key={d.name} value={d.name} label={d.title ?? d.name} />
              ))}
            </Tabs>

            <Dashboard
              resourceUri={encodeResourceUri({
                environmentName: ENVIRONMENT,
                packageName: PACKAGE,
              })}
              dashboard={selected}
              givens={givens}
              onGivensChange={setGivens}
              onNavigate={onNavigate}
            />
          </>
        )}
      </Box>
    </Stack>
  );
}
