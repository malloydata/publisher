# Malloy Publisher SDK

The Malloy Publisher SDK is a comprehensive toolkit designed to facilitate the development and testing of Malloy packages.

## CSS
To pull in CSS styling required by various SDK components do:
```ts
// Import required CSS
import '@malloy-publisher/sdk/styles.css';
```

## Example Usage:
**Rendering a project**
```react
    <Project name="malloy-samples" navigate={navigate} />
``` 
**Rendering a package**
```react
    <Package name="ecommerce" projectName="malloy-samples" navigate={navigate} />
```

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