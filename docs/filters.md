# Source Filters

Source filters let you declare filterable dimensions directly in your Malloy model using `#(filter)` annotations. Publisher automatically parses these declarations, exposes filter metadata through the API, renders filter widgets in the notebook UI, and injects `where:` clauses into queries server-side.

## Declaring Filters

Add `#(filter)` annotations above a source definition:

```malloy
#(filter) name=Manufacturer dimension=Manufacturer type=in
#(filter) name=Subject dimension=Subject type=like
#(filter) name=Major_Recall dimension="Major Recall" type=equal
#(filter) name=Recall_After dimension="Report Received Date" type=greater_than
#(filter) name=Recall_Before dimension="Report Received Date" type=less_than
source: recalls is duckdb.table('data/auto_recalls.csv') extend {
  measure:
    recall_count is count()
    ...
}
```

### Annotation Syntax

```
#(filter) [name=NAME] dimension=DIMENSION type=TYPE [implicit] [required]
```

| Parameter | Required | Description |
|-----------|----------|-------------|
| `name` | No | Unique identifier for the filter. Defaults to the dimension name. Used as the API parameter key. |
| `dimension` | Yes | The source dimension this filter targets. Quote with `"..."` if the name contains spaces. |
| `type` | Yes | Comparator type (see below). |
| `implicit` | No | Flag. Hides the filter from the UI and API summaries. Used for infrastructure concerns like row-level security. |
| `required` | No | Flag. The server returns a 400 error if a required filter has no value at query time. |

### Filter Types

| Type | Malloy Clause | Use Case |
|------|---------------|----------|
| `equal` | `dimension = 'value'` | Exact match on a single value |
| `in` | `dimension ? 'a' \| 'b' \| 'c'` | Match any of multiple values |
| `like` | `dimension ~ '%value%'` | Substring / pattern matching |
| `greater_than` | `dimension > value` | Range filter (after, minimum) |
| `less_than` | `dimension < value` | Range filter (before, maximum) |

### Multiple Filters on the Same Dimension

You can declare multiple filters targeting the same dimension by giving each a unique `name`. This is useful for date ranges:

```malloy
#(filter) name=Start_Date dimension="Created At" type=greater_than
#(filter) name=End_Date dimension="Created At" type=less_than
```

Each filter operates independently in the UI and maps to its own API parameter.

## How It Works

### Server-Side Query Rewriting

When a query is executed, Publisher:

1. Looks up `#(filter)` definitions for the query's source
2. Matches provided parameter values to filter definitions by `name`
3. Builds Malloy predicates with proper literal formatting (strings, booleans, `@YYYY-MM-DD` dates)
4. Appends a `+ { where: ... }` refinement to the query before compilation

This happens transparently — the original query text is never modified for the caller.

### Type-Aware Literals

Publisher automatically formats filter values based on the dimension's data type:

| Dimension Type | Input | Malloy Literal |
|---------------|-------|----------------|
| `string` | `FORD` | `'FORD'` |
| `boolean` | `true` | `true` |
| `date` | `2024-01-15` | `@2024-01-15` |

### Bypass Filters

Pass `bypass_filters=true` (or `bypassFilters: true` in the POST body) to skip filter injection entirely. This is useful for admin queries or debugging.

## API

### Filter Metadata

Filter definitions are exposed on `Source` objects in the API response. Each filter includes:

```json
{
  "name": "Recall_After",
  "dimension": "Report Received Date",
  "type": "greater_than",
  "implicit": false,
  "required": false,
  "dimensionType": "date"
}
```

The `dimensionType` field reflects the Malloy type of the underlying dimension (`string`, `number`, `boolean`, `date`, `timestamp`, etc.), enabling clients to render appropriate input widgets.

### REST Endpoints

**Execute a model query** — `POST /api/v0/projects/:project/packages/:package/models/:model/query`

```json
{
  "query": "run: recalls -> by_manufacturer",
  "filterParams": {
    "Manufacturer": ["FORD", "TOYOTA"],
    "Recall_After": "2020-01-01"
  },
  "bypassFilters": false
}
```

**Execute a notebook cell** — `GET /api/v0/projects/:project/packages/:package/notebooks/:path/cells/:index`

Query parameters:
- `filter_params` — URL-encoded JSON string: `{"Manufacturer": ["FORD"], "Recall_After": "2020-01-01"}`
- `bypass_filters` — `"true"` to skip filter injection

### MCP Tool

The `malloy_executeQuery` tool accepts a `filterParams` parameter:

```json
{
  "projectName": "malloy-samples",
  "packageName": "auto_recalls",
  "model": "auto_recalls.malloy",
  "query": "run: recalls -> by_manufacturer",
  "filterParams": {
    "Manufacturer": ["FORD"]
  }
}
```

Implicit filters are automatically stripped from MCP responses so AI agents only see user-facing filters.

## Notebook UI

When a notebook's model declares `#(filter)` annotations, the Publisher UI automatically renders filter widgets above the notebook content. The widget type is chosen based on the dimension's data type:

| Dimension Type | Widget |
|---------------|--------|
| `string` (with `in` or `like`) | Searchable multi-select or text input |
| `boolean` | Toggle / dropdown |
| `date` / `timestamp` | Date picker |
| `number` | Numeric input |

Setting a filter re-executes all notebook cells with the filter applied server-side. Implicit filters are hidden from the UI but still applied.
