import type { ResourceMetadata as SdkResourceMetadata } from "@modelcontextprotocol/sdk/server/mcp";

// Define the specific resource types expected as keys
type McpResourceType = "model" | "source" | "view" | "query" | "notebook";

// Descriptions and usage guidance for different Malloy resource types.
// Sourced from documentation and adapted for MCP client context.
// Type assertion ensures compatibility with the SDK's ResourceMetadata,
// which might include optional fields or an index signature.
export const RESOURCE_METADATA: Record<
   McpResourceType | "package" | "project",
   SdkResourceMetadata // Use the imported SDK type here
> = {
   model: {
      description:
         "A blueprint organizing your data tables, their connections, and reusable calculations (like 'revenue'). Models contain Sources, Views, and saved Queries.",
      usage: "This is your main map. Use its `definition` to see the available Sources, Views, and Queries inside (identified by their names or URIs). You can then fetch details for those specific items using `GetResource`. Use the `malloy_executeQuery` tool with queries defined here to get results, typically as structured data tables (JSON). The results might also include styling information suggesting visualizations.",
   },
   source: {
      description:
         "Represents a table or dataset (like 'orders'), including its columns and any attached calculations (dimensions/measures) or Views.",
      usage: "Examine the `definition` to understand the structure of this data and see available Views. Use `GetResource` on a View's URI to inspect it, or use the `malloy_executeQuery` tool (referencing the source and potentially a view) to get data results (JSON).",
   },
   view: {
      description:
         "A saved recipe within a Source for a specific analysis (like 'Top 10 Products').",
      usage: "Run this predefined analysis using the `malloy_executeQuery` tool (referencing the parent source/model and this view's name) to get results, typically structured data tables (JSON), sometimes with visualization hints.",
   },
   query: {
      description:
         "Similar to a View, a saved recipe for an analysis, usually stored directly in a Model.",
      usage: "Run this standard report using the `malloy_executeQuery` tool (referencing the parent model and this query's name) to get results, typically structured data tables (JSON), sometimes with visualization hints.",
   },
   notebook: {
      description:
         "An interactive document combining notes (Markdown text), Malloy code, and potentially saved queries or results.",
      usage: "Read the `definition` to explore the analysis steps, notes (Markdown), and code. Identify queries within the notebook cells that you can run using the `malloy_executeQuery` tool to get results (JSON data, visualization hints).",
   },
   package: {
      description: "A folder grouping related Models and Notebooks.",
      usage: "Use `ListResources` on this Package's URI to discover the Models and Notebooks it contains. You can then use `GetResource` on those items' URIs to explore them further.",
   },
   project: {
      description: "The main workspace folder holding Packages, Models, etc.",
      usage: "Use `ListResources` on this Project's URI to see the Packages inside and begin navigating your data assets.",
   },
};
