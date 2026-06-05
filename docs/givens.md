# Givens (Runtime Parameters)

Givens are Malloy's native mechanism for declaring runtime parameters on a model. Publisher introspects declared givens, exposes them through the API, renders parameter inputs in the notebook UI, and forwards values to Malloy's runtime so the model evaluates with the supplied parameter values.

Givens replace the older `#(filter)` annotation path. New models should declare `given:` blocks instead of `#(filter)` annotations. See the [Migration recipes](#migration-recipes) section for converting existing filter-based models.

For the authoritative Malloy reference (semantics, supported types, scoping rules), see [Malloy: Givens](https://docs.malloydata.dev/documentation/experiments/givens).

To gate *access* to a source based on these givens, see [Authorize (source access gates)](authorize.md).

## Declaring Givens

Givens are an experimental Malloy feature. Enable them once at the top of the model, then declare each given as a top-level statement before the source that uses it:

```malloy
##! experimental.givens

#(description="Two-letter IATA carrier code to spotlight")
given: carrier :: string is 'WN'

#(description="Earliest flight year to include")
given: start_year :: number is 2003

source: spotlight_carrier is duckdb.table('data/carriers.parquet') extend {
  primary_key: code
  measure: carrier_count is count()

  view: by_name is {
    where: code = $carrier
    select: code, name, nickname
    limit: 1
  }
}
```

A given has a name, a Malloy type, and an optional default. Queries reference the value with `$name`. When a caller supplies an override, Malloy substitutes the supplied value; otherwise the declared default applies.

### Supported Types

| Type        | Example declaration                                 | Use case                             |
| ----------- | --------------------------------------------------- | ------------------------------------ |
| `string`    | `given: code :: string is 'WN'`                     | Exact-match dimension values         |
| `string[]`  | `given: codes :: string[] is []`                    | Multi-value `in` filters             |
| `number`    | `given: year :: number is 2024`                     | Numeric ranges, year selectors       |
| `boolean`   | `given: include_canceled :: boolean is false`       | Toggle predicates                    |
| `date`      | `given: cutoff :: date is @2024-01-01`              | Date thresholds                      |
| `timestamp` | `given: since :: timestamp is @2024-01-01 00:00:00` | Timestamp thresholds                 |
| `filter<T>` | `given: where_carrier :: filter<string> is f'WN'`   | First-class Malloy filter expression |

### Annotations

Givens accept the standard Malloy `#(...)` annotation syntax. Publisher surfaces annotations on introspection and uses the `description="..."` form as helper text in the notebook UI:

```malloy
#(description="Earliest report date to include")
given: report_after :: date is @2024-01-01
```

## How It Works

When a query executes, Publisher forwards declared and supplied given values to Malloy's runtime:

1. Caller supplies givens as a `{ name: value }` map (request body, query string, or MCP tool argument).
2. Publisher passes the map to Malloy via `runnable.run({ givens })` (query execution) or `queryMaterializer.getSQL({ givens })` (compile-to-SQL).
3. Malloy substitutes the values inline when evaluating `$name` references.
4. Unset givens fall back to their declared defaults.

There is no Publisher-side query rewriting (no `+ { where: ... }` refinement). The substitution happens entirely inside Malloy.

### Accepted JS Shapes

Givens are typed in Malloy, but the wire format is JSON. The mapping is:

| Malloy type      | JS / JSON shape                               |
| ---------------- | --------------------------------------------- |
| `string`         | `"WN"`                                        |
| `string[]`       | `["WN", "AA"]`                                |
| `number`         | `42`                                          |
| `boolean`        | `true` / `false`                              |
| `date`           | `"2024-01-01"` (ISO date string)              |
| `timestamp`      | `"2024-01-01T12:00:00Z"` (ISO timestamp)      |
| `filter<string>` | `"WN, AA"` (Malloy filter syntax as a string) |

See the [Malloy accepted JS shapes table](https://docs.malloydata.dev/documentation/experiments/givens#accepted-js-shapes) for the full list.

## API

### Introspection

Givens declared on a model appear on `CompiledModel.givens` and on each `Source.givens` in the API response:

```json
{
  "givens": [
    {
      "name": "carrier",
      "type": "string",
      "annotations": [
        "#(description=\"Two-letter IATA carrier code to spotlight\")"
      ]
    },
    {
      "name": "start_year",
      "type": "number",
      "annotations": ["#(description=\"Earliest flight year to include\")"]
    }
  ]
}
```

Callers use this metadata to render input widgets without out-of-band knowledge of the model.

### REST Endpoints

**Execute a model query** — `POST /api/v0/environments/:env/packages/:package/models/:model/query`

```json
{
  "query": "run: spotlight_carrier -> by_name",
  "givens": {
    "carrier": "AA",
    "start_year": 2010
  }
}
```

**Compile Malloy source** — `POST /api/v0/environments/:env/packages/:package/models/:model/compile`

```json
{
  "source": "run: spotlight_carrier -> by_name",
  "includeSql": true,
  "givens": { "carrier": "AA" }
}
```

**Execute a notebook cell** — `GET /api/v0/environments/:env/packages/:package/notebooks/:path/cells/:index`

Query parameter `givens` accepts URL-encoded JSON:

```
?givens=%7B%22carrier%22%3A%22AA%22%2C%22start_year%22%3A2010%7D
```

### MCP Tool

The `malloy_executeQuery` tool accepts a `givens` parameter on the same wire shape:

```json
{
  "environmentName": "malloy-samples",
  "packageName": "faa",
  "modelPath": "carriers_with_parameters.malloy",
  "query": "run: spotlight_carrier -> by_name",
  "givens": {
    "carrier": "AA"
  }
}
```

## Notebook UI

When a notebook's model declares givens, the Publisher UI automatically renders a Parameters panel above the notebook content. The widget chosen depends on the declared Malloy type:

| Malloy type                        | Widget                                     |
| ---------------------------------- | ------------------------------------------ |
| `string`, `filter<string>`         | Text input with × clear                    |
| `string[]`                         | Multi-value autocomplete with chip removal |
| `number`                           | Numeric input with × clear                 |
| `boolean`                          | Checkbox                                   |
| `date`, `timestamp`, `timestamptz` | Date picker with native clear              |

`#(description="...")` annotations render as MUI helper text beneath the input. A **Reset** button appears next to the "Parameters" heading whenever any input has a non-default value.

Setting a value re-executes all notebook cells with the new givens applied.

## Migration Recipes

The table below maps `#(filter)` annotation patterns to their `given:` equivalents. The legacy `#(filter)` path continues to work for now but is deprecated; new models should declare givens directly.

| `#(filter)` annotation                                                  | `given:` equivalent                                                                                                                                            |
| ----------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `#(filter) name=Mfr dimension=Mfr type=in` (string)                     | `given: Mfr :: filter<string>` + `where: Mfr ~ $Mfr` (or `given: Mfr :: string[] is null` + `where: Mfr in $Mfr` — Malloy currently rejects empty-array defaults)                                                                                                          |
| `#(filter) name=Major dimension=Major type=equal`                       | `given: Major :: string is null` + `where: Major = $Major`                                                                                                     |
| `#(filter) name=Subject dimension=Subject type=like`                    | Not directly expressible. Use `given: Subject :: filter<string>` and `where: Subject ~ $Subject`, or keep `#(filter)` until Malloy adds a native `like` story. |
| `#(filter) name=After dimension="Report Date" type=greater_than` (date) | `given: After :: date is null` + `where: \`Report Date\` > $After`                                                                                             |
| `#(filter) name=Before dimension="Report Date" type=less_than` (date)   | `given: Before :: date is null` + `where: \`Report Date\` < $Before`                                                                                           |

### Coexistence

A model that mixes `#(filter)` and `given:` is fully supported. Both injection paths run independently and compose. Use this transitionally if migrating a large model in stages, but plan to consolidate on `given:` once Malloy supports every comparator your model needs.

## Worked Example

The bundled `malloy-samples` environment ships a standalone `faa-givens-demo` package containing `carriers_with_parameters.malloy` (a `string` carrier code and a `timestamp` cutoff) and `carriers_with_parameters.malloynb` (two views over the FAA `flights` table). Open the notebook in the Publisher UI:

```
http://localhost:4000/malloy-samples/faa-givens-demo/carriers_with_parameters.malloynb
```

The Parameters panel auto-renders with the declared defaults; change `carrier` from `WN` to `AA` and the cells re-execute with the new value.

The sample lives upstream at [credibledata/malloy-samples](https://github.com/credibledata/malloy-samples/tree/main/faa-givens-demo). It is intentionally a separate package so older publisher versions (which can't parse `given:` syntax) fail loading only the demo, not the existing `faa` analytics package.
