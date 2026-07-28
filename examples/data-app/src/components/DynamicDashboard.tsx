import Dashboard from "./Dashboard";
import type { AppView } from "../types/view";

export default function DynamicDashboard({
  selectedView,
  resourceUri,
}: {
  selectedView: AppView;
  resourceUri: string;
}) {
  return (
    <Dashboard
      selectedView={selectedView}
      storageKey="my-dashboard-widgets"
      defaultWidgets={[]}
      resourceUri={resourceUri}
    />
  );
}
