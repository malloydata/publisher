import Dashboard from "./Dashboard";

export default function DynamicDashboard({
  selectedView,
  resourceUri,
}: {
  selectedView:
    | "storefront"
    | "singleEmbed"
    | "dynamicDashboard"
    | "interactive";
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
