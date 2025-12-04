# Malloy Publisher SDK

The Publisher SDK (`@malloy-publisher/sdk`) is a comprehensive React component library for building data applications that interact with Publisher's REST API. It provides everything you need to browse semantic models, execute queries, visualize results, and build interactive data experiences.

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Core Concepts](#core-concepts)
4. [ServerProvider](#serverprovider)
5. [Page Components](#page-components)
6. [Query & Results Components](#query--results-components)
7. [Dimensional Filters](#dimensional-filters)
8. [Hooks](#hooks)
9. [Utilities](#utilities)
10. [Workbook Storage](#workbook-storage)
11. [Styling](#styling)
12. [Building a Custom Data App](#building-a-custom-data-app)
13. [API Reference](#api-reference)

---

## Installation

```bash
# Using bun
bun add @malloy-publisher/sdk

# Using npm
npm install @malloy-publisher/sdk

# Using yarn
yarn add @malloy-publisher/sdk
```

---

## Quick Start

### Basic Setup

```tsx
import { ServerProvider, Home } from "@malloy-publisher/sdk";
import "@malloy-publisher/sdk/styles.css";

function App() {
   return (
      <ServerProvider baseURL="http://localhost:4000/api/v0">
         <Home onClickProject={(path) => console.log("Navigate to:", path)} />
      </ServerProvider>
   );
}
```

### With React Router

```tsx
import {
   BrowserRouter,
   Routes,
   Route,
   useNavigate,
   useParams,
} from "react-router-dom";
import {
   ServerProvider,
   Home,
   Project,
   Package,
   Model,
   Notebook,
   encodeResourceUri,
   useRouterClickHandler,
} from "@malloy-publisher/sdk";
import "@malloy-publisher/sdk/styles.css";

function App() {
   return (
      <ServerProvider>
         <BrowserRouter>
            <Routes>
               <Route path="/" element={<HomePage />} />
               <Route path="/:projectName" element={<ProjectPage />} />
               <Route
                  path="/:projectName/:packageName"
                  element={<PackagePage />}
               />
               <Route
                  path="/:projectName/:packageName/*"
                  element={<ModelPage />}
               />
            </Routes>
         </BrowserRouter>
      </ServerProvider>
   );
}

function HomePage() {
   const navigate = useRouterClickHandler();
   return <Home onClickProject={navigate} />;
}

function ProjectPage() {
   const navigate = useRouterClickHandler();
   const { projectName } = useParams();
   const resourceUri = encodeResourceUri({ projectName });
   return <Project onSelectPackage={navigate} resourceUri={resourceUri} />;
}

function PackagePage() {
   const navigate = useRouterClickHandler();
   const { projectName, packageName } = useParams();
   const resourceUri = encodeResourceUri({ projectName, packageName });
   return <Package onClickPackageFile={navigate} resourceUri={resourceUri} />;
}

function ModelPage() {
   const params = useParams();
   const modelPath = params["*"];
   const resourceUri = encodeResourceUri({
      projectName: params.projectName,
      packageName: params.packageName,
      modelPath,
   });

   if (modelPath?.endsWith(".malloy")) {
      return <Model resourceUri={resourceUri} />;
   }
   if (modelPath?.endsWith(".malloynb")) {
      return <Notebook resourceUri={resourceUri} />;
   }
   return <div>Unknown file type</div>;
}
```

---

## Core Concepts

### Resource URIs

The SDK uses a standardized URI format to identify resources:

```
publisher://projects/{projectName}/packages/{packageName}/models/{modelPath}?versionId={version}
```

Examples:

- Project: `publisher://projects/my-project`
- Package: `publisher://projects/my-project/packages/analytics`
- Model: `publisher://projects/my-project/packages/analytics/models/orders.malloy`

Use the `encodeResourceUri()` and `parseResourceUri()` utilities to work with these URIs.

### Component Hierarchy

The SDK components follow a natural hierarchy:

```
ServerProvider (required wrapper)
├── Home (list all projects)
│   └── Project (show packages in a project)
│       └── Package (show models, notebooks, connections)
│           ├── Model (visual query builder + named queries)
│           └── Notebook (read-only notebook viewer)
└── Workbook (interactive analysis workbook)
```

### Navigation Pattern

Components accept callback functions for navigation rather than handling routing directly. This allows you to integrate with any routing solution:

```tsx
// With React Router
const navigate = useRouterClickHandler();
<Home onClickProject={navigate} />

// Custom navigation
<Home onClickProject={(path) => window.location.href = path} />

// SPA with history
<Home onClickProject={(path) => history.push(path)} />
```

---

## ServerProvider

The `ServerProvider` is the required context provider that wraps your application. It initializes API clients and passes auth headers (if required by the backend server).

### Props

| Prop             | Type                    | Default       | Description                                                          |
| ---------------- | ----------------------- | ------------- | -------------------------------------------------------------------- |
| `baseURL`        | `string`                | Auto-detected | Base URL of the Publisher API (e.g., `http://localhost:4000/api/v0`) |
| `getAccessToken` | `() => Promise<string>` | `undefined`   | Async function returning auth token                                  |
| `mutable`        | `boolean`               | `true`        | Enable/disable project/package management UI                         |

### Basic Usage

```tsx
<ServerProvider>{/* Your app */}</ServerProvider>
```

### With Authentication

```tsx
async function getAccessToken() {
   const response = await fetch("/auth/token");
   const { token } = await response.json();
   return `Bearer ${token}`;
}

<ServerProvider getAccessToken={getAccessToken}>
   {/* Your app */}
</ServerProvider>;
```

### Read-Only Mode

```tsx
// Disable add/edit/delete UI for production deployments
<ServerProvider mutable={false}>{/* Your app */}</ServerProvider>
```

### Custom Server URL

```tsx
<ServerProvider baseURL="https://publisher.example.com/api/v0">
   {/* Your app */}
</ServerProvider>
```

---

## Page Components

### Home

Displays a landing page with feature cards and a list of all available projects.

```tsx
import { Home } from "@malloy-publisher/sdk";

interface HomeProps {
   onClickProject?: (path: string, event?: React.MouseEvent) => void;
}

// Usage
<Home
   onClickProject={(path, event) => {
      // path is like "/my-project/"
      navigate(path);
   }}
/>;
```

**Features:**

- Hero section with Publisher branding
- Feature cards (Ad Hoc Analysis, Notebook Dashboards, AI Agents)
- Project listing with descriptions
- Add/Edit/Delete project dialogs (when `mutable=true`)

---

### Project

Shows all packages within a project.

```tsx
import { Project, encodeResourceUri } from "@malloy-publisher/sdk";

interface ProjectProps {
   onSelectPackage: (path: string, event?: React.MouseEvent) => void;
   resourceUri: string;
}

// Usage
const resourceUri = encodeResourceUri({ projectName: "my-project" });

<Project
   onSelectPackage={(path) => navigate(path)}
   resourceUri={resourceUri}
/>;
```

**Features:**

- Package listing with version info
- Add/Edit/Delete package dialogs (when `mutable=true`)
- Project README display

---

### Package

Displays package details including models, notebooks, databases, and connections.

```tsx
import { Package, encodeResourceUri } from "@malloy-publisher/sdk";

interface PackageProps {
   onClickPackageFile?: (path: string, event?: React.MouseEvent) => void;
   resourceUri: string;
}

// Usage
const resourceUri = encodeResourceUri({
   projectName: "my-project",
   packageName: "analytics",
});

<Package
   onClickPackageFile={(path) => navigate(path)}
   resourceUri={resourceUri}
/>;
```

**Features:**

- Models list (`.malloy` files)
- Notebooks list (`.malloynb` files)
- Embedded databases (`.parquet` files)
- Connection configuration
- Package README

---

### Model

The visual query builder and model explorer. This is the primary component for ad-hoc data analysis.

```tsx
import { Model, encodeResourceUri } from "@malloy-publisher/sdk";

interface ModelProps {
   resourceUri: string;
   onChange?: (query: QueryExplorerResult) => void;
   runOnDemand?: boolean; // Default: false
   maxResultSize?: number; // Default: 0 (no limit)
}

interface QueryExplorerResult {
   query: string | undefined;
   malloyQuery: Malloy.Query | string | undefined;
   malloyResult: Malloy.Result | undefined;
}

// Usage
const resourceUri = encodeResourceUri({
   projectName: "my-project",
   packageName: "analytics",
   modelPath: "models/orders.malloy",
});

<Model
   resourceUri={resourceUri}
   runOnDemand={true}
   maxResultSize={512 * 1024}
   onChange={(result) => {
      console.log("Query:", result.query);
      console.log("Result:", result.malloyResult);
   }}
/>;
```

**Features:**

- Source selector (dropdown for models with multiple sources)
- Visual query builder (Malloy Explorer integration)
- Named queries display
- Full-screen dialog mode
- Copy link to current view

---

### ModelExplorer

A lower-level component for embedding the query builder without the full Model chrome.

```tsx
import {
   ModelExplorer,
   useModelData,
   encodeResourceUri,
} from "@malloy-publisher/sdk";

interface ModelExplorerProps {
   data?: CompiledModel; // Pre-loaded model data
   onChange?: (query: QueryExplorerResult) => void;
   existingQuery?: QueryExplorerResult; // Initialize with existing query
   initialSelectedSourceIndex?: number; // Default: 0
   onSourceChange?: (index: number) => void;
   resourceUri: string;
}

// Usage with automatic data loading
<ModelExplorer
   resourceUri={resourceUri}
   onChange={(result) => console.log(result)}
/>;

// Usage with pre-loaded data
const { data } = useModelData(resourceUri);

<ModelExplorer
   data={data}
   resourceUri={resourceUri}
   onChange={(result) => console.log(result)}
/>;
```

---

### Notebook

Read-only notebook viewer that executes cells and displays results.

```tsx
import { Notebook, encodeResourceUri } from "@malloy-publisher/sdk";

interface NotebookProps {
   resourceUri: string;
   maxResultSize?: number; // Default: 0 (no limit)
}

// Usage
const resourceUri = encodeResourceUri({
   projectName: "my-project",
   packageName: "analytics",
   modelPath: "notebooks/sales-dashboard.malloynb",
});

<Notebook resourceUri={resourceUri} maxResultSize={1024 * 1024} />;
```

**Features:**

- Sequential cell execution
- Markdown rendering
- Code cell execution with results
- Error handling per cell

---

### Workbook

Interactive workbook editor for creating and saving custom analyses.

```tsx
import {
   Workbook,
   WorkbookStorageProvider,
   BrowserWorkbookStorage,
   encodeResourceUri,
} from "@malloy-publisher/sdk";

interface WorkbookProps {
   workbookPath?: WorkbookLocator; // { path: string, workspace: string }
   resourceUri: string;
}

// Usage
const workbookStorage = new BrowserWorkbookStorage();
const resourceUri = encodeResourceUri({
   projectName: "my-project",
   packageName: "analytics",
});

<WorkbookStorageProvider workbookStorage={workbookStorage}>
   <Workbook
      workbookPath={{ path: "my-analysis", workspace: "Local" }}
      resourceUri={resourceUri}
   />
</WorkbookStorageProvider>;
```

**Features:**

- Add/remove Markdown and Malloy cells
- Model picker for source selection
- Auto-save to storage backend
- Export to Malloy format
- Delete workbook

---

## Query & Results Components

### QueryResult

Executes a query and displays the visualization.

```tsx
import { QueryResult, encodeResourceUri } from "@malloy-publisher/sdk";

interface QueryResultProps {
  query?: string;        // Raw Malloy query
  sourceName?: string;   // Source name for named query
  queryName?: string;    // Named query to execute
  resourceUri?: string;  // Resource URI for model
}

// Execute a named query
<QueryResult
  sourceName="orders"
  queryName="by_region"
  resourceUri={encodeResourceUri({
    projectName: "my-project",
    packageName: "analytics",
    modelPath: "models/orders.malloy",
  })}
/>

// Execute a raw query
<QueryResult
  query="run: orders -> { group_by: status; aggregate: order_count }"
  resourceUri={encodeResourceUri({
    projectName: "my-project",
    packageName: "analytics",
    modelPath: "models/orders.malloy",
  })}
/>
```

---

### RenderedResult

Low-level component for rendering Malloy result JSON as a visualization.

```tsx
import RenderedResult from "@malloy-publisher/sdk";

interface RenderedResultProps {
   result: string; // JSON result string
   height?: number; // Fixed height in pixels
   onSizeChange?: (height: number) => void; // Callback when size changes
   onDrill?: (element: unknown) => void; // Drill-down callback
}

// Usage (result is the JSON string from query execution)
<RenderedResult
   result={queryResultJson}
   onDrill={(element) => {
      console.log("Drilled into:", element);
   }}
/>;
```

---

### EmbeddedQueryResult

Helper for embedding query results as serialized JSON (useful for storage/transfer).

```tsx
import {
   EmbeddedQueryResult,
   createEmbeddedQueryResult,
} from "@malloy-publisher/sdk";

// Create embedded query config
const embedded = createEmbeddedQueryResult({
   queryName: "by_region",
   sourceName: "orders",
   resourceUri: encodeResourceUri({
      projectName: "my-project",
      packageName: "analytics",
      modelPath: "models/orders.malloy",
   }),
});

// Later, render it
<EmbeddedQueryResult embeddedQueryResult={embedded} />;
```

---

## Dimensional Filters

The SDK supports interactive dimensional filtering for notebooks and embedded data apps. Filters are configured through annotations in Malloy source files and notebooks.

### Filter Types

| Type | UI Component | Use Case |
|------|--------------|----------|
| `Star` | Multi-select dropdown | String fields with discrete values |
| `MinMax` | Range slider | Numeric fields |
| `DateMinMax` | Date range picker | Date/timestamp fields |
| `Retrieval` | Semantic search input | Free-text concept search |
| `Boolean` | Toggle switch | Boolean fields |

### Source Declaration Syntax

Add filter annotations to dimensions in your Malloy source files using the `#(filter)` tag:

```malloy
source: flights is duckdb.table('data/flights.parquet') extend {
  dimension:
    // Multi-select dropdown for string values
    #(filter) {"type": "Star"}
    origin_code is origin

    // Range slider for numeric values
    #(filter) {"type": "MinMax"}
    distance_miles is distance

    // Date range picker
    #(filter) {"type": "DateMinMax"}
    flight_departure is dep_time

  join_one: carriers with carrier
}

source: carriers is duckdb.table('data/carriers.parquet') extend {
  dimension:
    #(filter) {"type": "Star"}
    nickname is nickname_old

    // Semantic search for text fields (requires embedding index)
    #(index_values) n=-1
    #(filter) {"type": "Retrieval"}
    name is name_old
}

source: recalls is duckdb.table('data/recalls.csv') extend {
  dimension:
    // Boolean toggle filter
    #(filter) {"type": "Boolean"}
    is_major_recall is potentially_affected > 100000
}
```

### Notebook Annotation Syntax

Enable filters in a notebook by adding a `##(filters)` annotation in a Malloy code cell. This annotation specifies which dimensions should appear as filters using `source.dimension` format:

**Simple array format:**
```malloy
##(filters) ["flights.origin_code", "carriers.name", "flights.flight_departure"]
import {flights, carriers} from 'flights.malloy'
```

The filter type for each dimension is determined by the `#(filter)` annotation on that dimension in the source file. If no source annotation exists, the dimension is ignored.

**Note**: Concept search is not supported by the Publisher.
When using the Notebook component, you supply an async function which implements the search for that column+query.
If no search function is supplied, the filter is ignored.

### React Hooks for Programmatic Filtering

For custom data apps, use the SDK's React hooks:

```tsx
import {
  useDimensionFiltersFromSpec,
  DimensionFiltersConfig
} from '@malloy-publisher/sdk';

const config: DimensionFiltersConfig = {
  project: "malloy-samples",
  package: "faa",
  indexLimit: 1000,
  dimensionSpecs: [
    { dimensionName: "origin_code", filterType: "Star", source: "flights", model: "flights.malloy" },
    { dimensionName: "distance", filterType: "MinMax", source: "flights", model: "flights.malloy" },
    { dimensionName: "dep_time", filterType: "DateMinMax", source: "flights", model: "flights.malloy" },
  ],
};

function FilteredDashboard() {
  const {
    filterStates,       // Current filter values
    updateFilter,       // Update a single filter
    clearAllFilters,    // Reset all filters
    activeFilters,      // Array of active filter selections
    data,               // Dimension values for dropdowns/sliders
    isLoading,          // Loading state
    executeQuery,       // Run query with current filters
    queryString,        // Generated Malloy query
  } = useDimensionFiltersFromSpec(config);

  // Render filter UI and results...
}
```

### Match Types

Filters support different match types depending on the filter type:

| Match Type | Description | Applicable To |
|------------|-------------|---------------|
| `Equals` | Exact match (multi-select supported) | Star, Retrieval |
| `Contains` | Substring match | Star |
| `Greater Than` / `Less Than` | Comparison | MinMax |
| `Between` | Range (inclusive) | MinMax, DateMinMax |
| `After` / `Before` | Date comparison | DateMinMax |
| `Concept Search` | Semantic similarity | Retrieval |

---

## Hooks

### useServer

Access the server context (API clients, configuration).

```tsx
import { useServer } from "@malloy-publisher/sdk";

function MyComponent() {
   const {
      server, // Base URL string
      apiClients, // API client instances
      mutable, // Whether mutations are allowed
      getAccessToken, // Auth token function
   } = useServer();

   // Use API clients directly
   const projects = await apiClients.projects.listProjects();
   const model = await apiClients.models.getModel(
      projectName,
      packageName,
      modelPath,
      versionId,
   );
}
```

### API Clients Available

```typescript
interface ApiClients {
   models: ModelsApi; // Get/execute models
   projects: ProjectsApi; // CRUD projects
   packages: PackagesApi; // CRUD packages
   notebooks: NotebooksApi; // Get/execute notebooks
   connections: ConnectionsApi; // CRUD connections
   databases: DatabasesApi; // Access embedded databases
   watchMode: WatchModeApi; // File watching for dev
}
```

---

### useQueryWithApiError

React Query wrapper with standardized error handling.

```tsx
import { useQueryWithApiError } from "@malloy-publisher/sdk";

function MyComponent() {
   const { data, isLoading, isError, error } = useQueryWithApiError({
      queryKey: ["my-data", someParam],
      queryFn: async () => {
         const response = await apiClients.projects.listProjects();
         return response.data;
      },
   });

   if (isLoading) return <Loading />;
   if (isError) return <ApiErrorDisplay error={error} context="Loading data" />;
   return <div>{JSON.stringify(data)}</div>;
}
```

**Features:**

- Automatic server-based cache key namespacing
- Standardized axios error transformation
- No automatic retries (explicit control)

---

### useMutationWithApiError

Mutation wrapper with standardized error handling.

```tsx
import { useMutationWithApiError } from "@malloy-publisher/sdk";

function MyComponent() {
   const mutation = useMutationWithApiError({
      mutationFn: async (newProject) => {
         const response = await apiClients.projects.createProject(newProject);
         return response.data;
      },
      onSuccess: () => {
         queryClient.invalidateQueries(["projects"]);
      },
   });

   return (
      <button onClick={() => mutation.mutate({ name: "new-project" })}>
         Create Project
      </button>
   );
}
```

---

### useModelData

Fetch compiled model data for a resource URI.

```tsx
import { useModelData } from "@malloy-publisher/sdk";

function MyComponent({ resourceUri }) {
   const {
      data, // CompiledModel
      isLoading,
      isError,
      error,
   } = useModelData(resourceUri);

   if (isLoading) return <Loading text="Loading model..." />;
   if (isError) return <ApiErrorDisplay error={error} />;

   // Access model data
   console.log("Sources:", data.sourceInfos);
   console.log("Queries:", data.queries);
}
```

---

### useRawQueryData

Execute a query and get raw data (array of rows) instead of visualization.

```tsx
import { useRawQueryData } from "@malloy-publisher/sdk";

function MyComponent({ resourceUri }) {
   const {
      data, // Array of row objects
      isLoading,
      isSuccess,
      isError,
      error,
   } = useRawQueryData({
      resourceUri,
      modelPath: "models/orders.malloy",
      queryName: "by_region",
      sourceName: "orders",
      enabled: true,
   });

   if (isSuccess) {
      // data is an array of row objects
      data.forEach((row) => {
         console.log(row.region, row.total_sales);
      });
   }
}
```

---

### useRouterClickHandler

Smart navigation hook that supports modifier keys (Cmd/Ctrl+click for new tab).

```tsx
import { useRouterClickHandler } from "@malloy-publisher/sdk";

function MyComponent() {
   const navigate = useRouterClickHandler();

   return (
      <button onClick={(e) => navigate("/projects/analytics", e)}>
         Go to Analytics
      </button>
   );
}
```

**Behavior:**

- Normal click: In-app navigation
- Cmd/Ctrl+click: Open in new tab
- Middle-click: Open in new tab
- Shift+click: Open in new window

---

## Utilities

### encodeResourceUri

Create a resource URI from components.

```tsx
import { encodeResourceUri } from "@malloy-publisher/sdk";

// Project only
const projectUri = encodeResourceUri({
   projectName: "my-project",
});
// Result: "publisher://projects/my-project"

// Package
const packageUri = encodeResourceUri({
   projectName: "my-project",
   packageName: "analytics",
});
// Result: "publisher://projects/my-project/packages/analytics"

// Model with version
const modelUri = encodeResourceUri({
   projectName: "my-project",
   packageName: "analytics",
   modelPath: "models/orders.malloy",
   versionId: "abc123",
});
// Result: "publisher://projects/my-project/packages/analytics/models/models/orders.malloy?versionId=abc123"
```

---

### parseResourceUri

Parse a resource URI back to components.

```tsx
import { parseResourceUri } from "@malloy-publisher/sdk";

const uri =
   "publisher://projects/my-project/packages/analytics/models/orders.malloy?versionId=abc123";
const parsed = parseResourceUri(uri);

// Result:
// {
//   projectName: "my-project",
//   packageName: "analytics",
//   modelPath: "orders.malloy",
//   versionId: "abc123"
// }
```

---

### ParsedResource Type

```typescript
type ParsedResource = {
   projectName: string;
   packageName?: string;
   connectionName?: string;
   versionId?: string;
   modelPath?: string;
};
```

---

## Workbook Storage

Workbooks are interactive analysis documents that can be saved and loaded. The SDK provides a storage abstraction that you can implement for different backends.

### WorkbookStorage Interface

```typescript
interface Workspace {
   name: string;
   writeable: boolean;
   description: string;
}

interface WorkbookLocator {
   path: string;
   workspace: string;
}

interface WorkbookStorage {
   listWorkspaces(writeableOnly: boolean): Promise<Workspace[]>;
   listWorkbooks(workspace: Workspace): Promise<WorkbookLocator[]>;
   getWorkbook(path: WorkbookLocator): Promise<string>;
   deleteWorkbook(path: WorkbookLocator): Promise<void>;
   saveWorkbook(path: WorkbookLocator, workbook: string): Promise<void>;
   moveWorkbook(from: WorkbookLocator, to: WorkbookLocator): Promise<void>;
}
```

---

### BrowserWorkbookStorage

Built-in implementation using browser localStorage.

```tsx
import {
   BrowserWorkbookStorage,
   WorkbookStorageProvider,
} from "@malloy-publisher/sdk";

const storage = new BrowserWorkbookStorage();

<WorkbookStorageProvider workbookStorage={storage}>
   <App />
</WorkbookStorageProvider>;
```

---

### Custom Storage Implementation

```tsx
class S3WorkbookStorage implements WorkbookStorage {
   private s3Client: S3Client;
   private bucket: string;

   constructor(s3Client: S3Client, bucket: string) {
      this.s3Client = s3Client;
      this.bucket = bucket;
   }

   async listWorkspaces(writeableOnly: boolean): Promise<Workspace[]> {
      return [
         {
            name: this.bucket,
            writeable: true,
            description: "S3 bucket storage",
         },
      ];
   }

   async listWorkbooks(workspace: Workspace): Promise<WorkbookLocator[]> {
      const objects = await this.s3Client.listObjects(
         this.bucket,
         "workbooks/",
      );
      return objects.map((obj) => ({
         path: obj.key,
         workspace: workspace.name,
      }));
   }

   async getWorkbook(path: WorkbookLocator): Promise<string> {
      const data = await this.s3Client.getObject(this.bucket, path.path);
      return data.toString();
   }

   async saveWorkbook(path: WorkbookLocator, workbook: string): Promise<void> {
      await this.s3Client.putObject(this.bucket, path.path, workbook);
   }

   async deleteWorkbook(path: WorkbookLocator): Promise<void> {
      await this.s3Client.deleteObject(this.bucket, path.path);
   }

   async moveWorkbook(
      from: WorkbookLocator,
      to: WorkbookLocator,
   ): Promise<void> {
      const content = await this.getWorkbook(from);
      await this.saveWorkbook(to, content);
      await this.deleteWorkbook(from);
   }
}

// Usage
const storage = new S3WorkbookStorage(s3Client, "my-workbooks-bucket");

<WorkbookStorageProvider workbookStorage={storage}>
   <App />
</WorkbookStorageProvider>;
```

---

### WorkbookStorageProvider

Context provider for workbook storage.

```tsx
import {
   WorkbookStorageProvider,
   useWorkbookStorage,
} from "@malloy-publisher/sdk";

// Provider setup
<WorkbookStorageProvider workbookStorage={myStorage}>
   <App />
</WorkbookStorageProvider>;

// Access in components
function MyComponent() {
   const { workbookStorage } = useWorkbookStorage();

   const workbooks = await workbookStorage.listWorkbooks({
      name: "Local",
      writeable: true,
      description: "",
   });
}
```

---

## Styling

### Required CSS

Import the SDK styles in your app entry point:

```tsx
// Main SDK styles (required)
import "@malloy-publisher/sdk/styles.css";

// If using Model/ModelExplorer outside of Publisher
import "@malloy-publisher/sdk/malloy-explorer.css";

// If using Workbook markdown editor
import "@malloy-publisher/sdk/markdown-editor.css";
```

---

### Material-UI Theme

The SDK uses Material-UI (MUI) v7. You can customize the theme:

```tsx
import { createTheme, ThemeProvider, CssBaseline } from "@mui/material";
import { ServerProvider } from "@malloy-publisher/sdk";

const theme = createTheme({
   palette: {
      primary: {
         main: "#14b3cb", // Malloy teal
      },
      secondary: {
         main: "#fbbb04", // Malloy yellow
      },
   },
   typography: {
      fontFamily: '"Inter", "Roboto", sans-serif',
   },
});

function App() {
   return (
      <ServerProvider>
         <ThemeProvider theme={theme}>
            <CssBaseline />
            {/* Your app */}
         </ThemeProvider>
      </ServerProvider>
   );
}
```

---

### Styled Components

The SDK exports several pre-styled components for consistent UI:

```tsx
import {
   StyledCard,
   StyledCardContent,
   StyledCardMedia,
   PackageCard,
   PackageCardContent,
   PackageSectionTitle,
   CleanNotebookContainer,
   CleanNotebookSection,
} from "@malloy-publisher/sdk";
```

---

## Building a Custom Data App

### Example: Dashboard with Multiple Visualizations

```tsx
import {
   ServerProvider,
   QueryResult,
   useModelData,
   encodeResourceUri,
   ApiErrorDisplay,
   Loading,
} from "@malloy-publisher/sdk";
import "@malloy-publisher/sdk/styles.css";
import { Grid, Typography, Paper } from "@mui/material";

function Dashboard() {
   const resourceUri = encodeResourceUri({
      projectName: "my-project",
      packageName: "analytics",
      modelPath: "models/sales.malloy",
   });

   const { data, isLoading, isError, error } = useModelData(resourceUri);

   if (isLoading) return <Loading text="Loading dashboard..." />;
   if (isError) return <ApiErrorDisplay error={error} context="Dashboard" />;

   return (
      <Grid container spacing={3}>
         <Grid item xs={12}>
            <Typography variant="h4">Sales Dashboard</Typography>
         </Grid>

         <Grid item xs={12} md={6}>
            <Paper sx={{ p: 2 }}>
               <Typography variant="h6">Sales by Region</Typography>
               <QueryResult
                  sourceName="orders"
                  queryName="by_region"
                  resourceUri={resourceUri}
               />
            </Paper>
         </Grid>

         <Grid item xs={12} md={6}>
            <Paper sx={{ p: 2 }}>
               <Typography variant="h6">Monthly Trends</Typography>
               <QueryResult
                  sourceName="orders"
                  queryName="monthly_trends"
                  resourceUri={resourceUri}
               />
            </Paper>
         </Grid>

         <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
               <Typography variant="h6">Custom Query</Typography>
               <QueryResult
                  query="run: orders -> {
              group_by: product_category
              aggregate:
                total_revenue is sum(revenue)
                avg_order_value is avg(order_value)
            }"
                  resourceUri={resourceUri}
               />
            </Paper>
         </Grid>
      </Grid>
   );
}

function App() {
   return (
      <ServerProvider baseURL="http://localhost:4000/api/v0">
         <Dashboard />
      </ServerProvider>
   );
}
```

---

### Example: Data Table with Raw Query Data

```tsx
import {
   ServerProvider,
   useRawQueryData,
   encodeResourceUri,
   Loading,
   ApiErrorDisplay,
} from "@malloy-publisher/sdk";
import { DataGrid } from "@mui/x-data-grid";

function DataTable() {
   const resourceUri = encodeResourceUri({
      projectName: "my-project",
      packageName: "analytics",
      modelPath: "models/customers.malloy",
   });

   const { data, isLoading, isError, error } = useRawQueryData({
      resourceUri,
      modelPath: "models/customers.malloy",
      sourceName: "customers",
      queryName: "all_customers",
   });

   if (isLoading) return <Loading text="Loading data..." />;
   if (isError) return <ApiErrorDisplay error={error} />;

   const columns =
      data.length > 0
         ? Object.keys(data[0]).map((key) => ({
              field: key,
              headerName: key,
              width: 150,
           }))
         : [];

   return (
      <DataGrid
         rows={data.map((row, i) => ({ id: i, ...row }))}
         columns={columns}
         pageSize={10}
         autoHeight
      />
   );
}
```

---

### Example: Interactive Model Explorer

```tsx
import {
   ServerProvider,
   ModelExplorer,
   encodeResourceUri,
} from "@malloy-publisher/sdk";
import "@malloy-publisher/sdk/styles.css";
import "@malloy-publisher/sdk/malloy-explorer.css";
import { useState } from "react";

function Explorer() {
   const [selectedQuery, setSelectedQuery] = useState(null);

   const resourceUri = encodeResourceUri({
      projectName: "my-project",
      packageName: "analytics",
      modelPath: "models/orders.malloy",
   });

   return (
      <div style={{ display: "flex", gap: "20px" }}>
         <div style={{ flex: 1 }}>
            <h2>Build Your Query</h2>
            <ModelExplorer
               resourceUri={resourceUri}
               onChange={(result) => {
                  setSelectedQuery(result);
                  console.log("Generated Query:", result.query);
               }}
            />
         </div>

         {selectedQuery && (
            <div style={{ flex: 1 }}>
               <h2>Query Preview</h2>
               <pre>{selectedQuery.query}</pre>
            </div>
         )}
      </div>
   );
}
```

---

### Example: Lightweight Client-Only Setup

For minimal bundle size when you only need API access:

```tsx
// Use the client entry point
import { ServerProvider, useServer } from "@malloy-publisher/sdk/client";

function MyApp() {
   return (
      <ServerProvider baseURL="http://localhost:4000/api/v0">
         <ProjectList />
      </ServerProvider>
   );
}

function ProjectList() {
   const { apiClients } = useServer();
   const [projects, setProjects] = useState([]);

   useEffect(() => {
      apiClients.projects
         .listProjects()
         .then((response) => setProjects(response.data));
   }, []);

   return (
      <ul>
         {projects.map((p) => (
            <li key={p.name}>{p.name}</li>
         ))}
      </ul>
   );
}
```

---

## API Reference

### Exported Components

| Component                 | Description                                     |
| ------------------------- | ----------------------------------------------- |
| `ServerProvider`          | Required context provider for API access        |
| `Home`                    | Project listing landing page                    |
| `Project`                 | Package listing for a project                   |
| `Package`                 | Package detail (models, notebooks, connections) |
| `Model`                   | Full model explorer with visual query builder   |
| `ModelExplorer`           | Lower-level query builder component             |
| `ModelExplorerDialog`     | Model explorer in a modal dialog                |
| `Notebook`                | Read-only notebook viewer                       |
| `Workbook`                | Interactive workbook editor                     |
| `WorkbookList`            | List workbooks from storage                     |
| `WorkbookManager`         | Workbook state management class                 |
| `WorkbookStorageProvider` | Context for workbook storage                    |
| `QueryResult`             | Execute and display query                       |
| `RenderedResult`          | Render Malloy result JSON                       |
| `EmbeddedQueryResult`     | Render serialized query config                  |
| `Loading`                 | Loading spinner with text                       |
| `ApiErrorDisplay`         | Error display component                         |
| `AnalyzePackageButton`    | Create/manage workbooks                         |
| `SourcesExplorer`         | Source schema browser                           |
| `ConnectionExplorer`      | Connection management UI                        |

### Exported Hooks

| Hook                            | Description                         |
| ------------------------------- | ----------------------------------- |
| `useServer`                     | Access ServerProvider context       |
| `useQueryWithApiError`          | React Query with error handling     |
| `useMutationWithApiError`       | Mutations with error handling       |
| `useModelData`                  | Fetch compiled model                |
| `useRawQueryData`               | Execute query, get raw data         |
| `useRouterClickHandler`         | Smart navigation with modifier keys |
| `useWorkbookStorage`            | Access workbook storage context     |
| `useDimensionFiltersFromSpec`   | Programmatic dimensional filtering  |

### Exported Utilities

| Utility                     | Description                         |
| --------------------------- | ----------------------------------- |
| `encodeResourceUri`         | Create resource URI from components |
| `parseResourceUri`          | Parse resource URI to components    |
| `createEmbeddedQueryResult` | Serialize query config              |
| `BrowserWorkbookStorage`    | localStorage-based workbook storage |
| `globalQueryClient`         | Shared React Query client           |

### Exported Types

| Type                     | Description                          |
| ------------------------ | ------------------------------------ |
| `ParsedResource`         | Parsed resource URI components       |
| `ServerContextValue`     | Server context interface             |
| `ServerProviderProps`    | ServerProvider props                 |
| `QueryExplorerResult`    | Query builder result                 |
| `SourceAndPath`          | Source info with model path          |
| `WorkbookStorage`        | Workbook storage interface           |
| `WorkbookLocator`        | Workbook path + workspace            |
| `Workspace`              | Workspace metadata                   |
| `ApiError`               | Standardized API error               |
| `ModelExplorerProps`     | ModelExplorer props                  |
| `DimensionFiltersConfig` | Dimensional filter configuration     |

---

## Additional Resources

- [Publisher GitHub Repository](https://github.com/malloydata/publisher)
- [Malloy Language Reference](https://docs.malloydata.dev/)
- [Malloy Slack Community](https://join.slack.com/t/malloy-community/shared_invite/zt-1kgfwgi5g-CrsdaRqs81QY67QW0~t_uw)
