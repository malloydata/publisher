import Dashboard from "./Dashboard";
import defaultWidgets from "../constants/defaultMalloySamplesDashboardWidgets.json";

export default function MalloySamplesDashboard({
  selectedView,
}: {
  selectedView: "malloySamples" | "singleEmbed" | "dynamicDashboard";
}) {
  const customizeWidgetsEffect = (widgets: any[]) => {
    const baseUrl = import.meta.env.VITE_DEFAULT_MS2_URL || "";
    const org = import.meta.env.VITE_DEFAULT_ORGANIZATION || "";

    if (baseUrl && org) {
      const urlParts = baseUrl.match(/(https?:\/\/)(.+?)(?=\/|$)/);
      if (urlParts) {
        const [, protocol, domain] = urlParts;
        const newBaseUrl = `${protocol}${org}.${domain}/api/v0`;
        widgets.forEach((widget) => {
          widget.server = newBaseUrl;
        });
      }
    }
  };

  return (
    <Dashboard
      selectedView={selectedView}
      storageKey="my-dashboard-widgets-malloy-samples-v2"
      defaultWidgets={defaultWidgets}
      customizeWidgetsEffect={customizeWidgetsEffect}
      resourceUri={`publisher://projects/malloy-samples`}
    />
  );
}
