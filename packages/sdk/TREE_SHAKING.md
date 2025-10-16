# SDK Tree-Shaking and Code Splitting Guide

This document explains how to use the Malloy Publisher SDK's optimized packaging for better tree-shaking and code splitting.

## Overview

The SDK provides two main entry points to enable better tree-shaking and code splitting:

- **Main entry** (`@malloy-publisher/sdk`): Full SDK with all components
- **Client entry** (`@malloy-publisher/sdk/client`): Lightweight client functionality only

## Client-Only Usage (Recommended for Lightweight Bundles)

If you only need `ServerProvider`, `useServer`, and API clients without the heavy UI components, use the client entry point:

```tsx
// ✅ Lightweight import - only pulls in client functionality
import { ServerProvider, useServer } from '@malloy-publisher/sdk/client';

function App() {
  return (
    <ServerProvider baseURL="https://your-publisher-instance.com">
      <YourComponents />
    </ServerProvider>
  );
}

function YourComponent() {
  const { apiClients } = useServer();
  
  // Use API clients for data fetching
  const projects = await apiClients.projects.getProjects();
  
  return <div>{/* Your custom UI */}</div>;
}
```

### Bundle Size Comparison

- **Full SDK import**: ~908KB+ (includes all UI components, Material-UI, etc.)
- **Client-only import**: ~2KB (includes only client logic, React Query, Axios)

### Why Use the Client Entry Point?

The client entry point (`@malloy-publisher/sdk/client`) is ideal when you:

- Want to use `ServerProvider` and `useServer` for API integration
- Need direct access to the OpenAPI-generated client APIs
- Want to build custom UI components without the overhead of Material-UI
- Need the query client for direct React Query access

The client entry exports:
- `ServerProvider` and `useServer` React components
- `globalQueryClient` for direct React Query access
- All OpenAPI-generated client APIs (`ConnectionsApi`, `ProjectsApi`, etc.)
- `Configuration` class for API client setup

## Full SDK Usage

If you need the complete UI components, use the main entry point:

```tsx
// ✅ Full SDK import - includes all components but larger bundle
import { 
  ServerProvider, 
  useServer,
  Home,
  Project,
  Package,
  Model,
  Notebook
} from '@malloy-publisher/sdk';

function App() {
  return (
    <ServerProvider>
      <Home />
    </ServerProvider>
  );
}
```

## Advanced Usage Examples

### Direct API Client Usage

```tsx
import { 
  ServerProvider, 
  useServer,
  Configuration,
  ProjectsApi,
  ConnectionsApi 
} from '@malloy-publisher/sdk/client';

// Configure API client directly
const config = new Configuration({
  basePath: 'https://your-publisher-instance.com'
});

const projectsApi = new ProjectsApi(config);

function MyComponent() {
  const { apiClients } = useServer();
  
  // Use either the context clients or direct instances
  const projects = await apiClients.projects.getProjects();
  // OR
  const projects2 = await projectsApi.getProjects();
  
  return <div>{/* Your UI */}</div>;
}
```

### Type-Only Imports

```tsx
// ✅ Types-only import - zero bundle impact
import type { 
  ProjectsApi, 
  ModelsApi, 
  QueryResult 
} from '@malloy-publisher/sdk/client';

// Your API wrapper
class MyApiWrapper {
  constructor(private projectsApi: ProjectsApi) {}
  
  async getProject(id: string): Promise<QueryResult> {
    return this.projectsApi.getProject(id);
  }
}
```

## Advanced Tree-Shaking Configuration

### Webpack Configuration

```javascript
// webpack.config.js
module.exports = {
  optimization: {
    usedExports: true,
    sideEffects: false, // Enable aggressive tree-shaking
  },
  resolve: {
    // Help webpack resolve the correct entry points
    mainFields: ['module', 'main'],
  },
};
```

### Vite Configuration

```javascript
// vite.config.js
export default {
  build: {
    rollupOptions: {
      // Externalize dependencies that shouldn't be bundled
      external: [
        '@tanstack/react-query',
        'axios',
        'react',
        'react-dom'
      ],
    },
  },
};
```

### Next.js Configuration

```javascript
// next.config.js
module.exports = {
  experimental: {
    // Enable better tree-shaking for Next.js
    optimizePackageImports: ['@malloy-publisher/sdk'],
  },
  webpack: (config) => {
    config.optimization.sideEffects = false;
    return config;
  },
};
```

## Migration Guide

### From Full SDK Import

**Before:**
```tsx
import { ServerProvider, useServer } from '@malloy-publisher/sdk';
```

**After (for lightweight bundles):**
```tsx
import { ServerProvider, useServer } from '@malloy-publisher/sdk/client';
```

The client entry point includes both the React components AND the API clients, making it a complete lightweight solution for most use cases.

### Bundle Analysis

To analyze your bundle and verify tree-shaking is working:

```bash
# Install bundle analyzer
npm install --save-dev webpack-bundle-analyzer

# For Webpack
npx webpack-bundle-analyzer build/static/js/*.js

# For Vite
npm install --save-dev rollup-plugin-visualizer
```

## Best Practices

1. **Use client entry for data-only applications**: If you're building custom UI, import only client functionality
2. **Import components individually**: Instead of `import * from '@malloy-publisher/sdk'`
3. **Externalize peer dependencies**: Don't bundle React, Material-UI, etc. in your library builds
4. **Enable `sideEffects: false`** in your bundler configuration for aggressive tree-shaking
5. **Use dynamic imports** for heavy components that aren't always needed:

```tsx
// Lazy load heavy components
const ModelExplorer = lazy(() => 
  import('@malloy-publisher/sdk').then(m => ({ default: m.ModelExplorer }))
);
```

## Troubleshooting

### Bundle Still Large?

1. Check if you're importing from the correct entry point
2. Verify your bundler configuration enables tree-shaking
3. Use bundle analyzer to identify what's being included
4. Ensure peer dependencies are externalized

### TypeScript Errors?

Make sure your `tsconfig.json` includes:

```json
{
  "compilerOptions": {
    "moduleResolution": "bundler", // or "node16"
    "allowSyntheticDefaultImports": true,
    "esModuleInterop": true
  }
}
```

### Runtime Errors?

Ensure you have all peer dependencies installed:

```bash
npm install react react-dom @tanstack/react-query
```

## Package Structure

```
@malloy-publisher/sdk/
├── index.js          # Full SDK (all components)
├── client/           # Lightweight client functionality
├── components/       # Individual UI components
└── styles.css        # CSS styles
```

This structure enables bundlers to:
- Tree-shake unused components
- Code-split at the entry point level  
- Minimize bundle size for client-only usage
- Preserve full functionality when needed