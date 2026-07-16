import Dashboard from "./Dashboard";
import defaultWidgets from "../constants/defaultStorefrontDashboardWidgets.json";

export default function StorefrontDashboard({
  selectedView,
}: {
  selectedView: "storefront" | "singleEmbed" | "dynamicDashboard";
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
      storageKey="my-dashboard-widgets-storefront-v1"
      defaultWidgets={defaultWidgets}
      customizeWidgetsEffect={customizeWidgetsEffect}
      resourceUri={`publisher://environments/examples`}
    />
  );
}
