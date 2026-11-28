<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

## Legacy: reading an existing `#(filter)` model

`#(filter)` is deprecated. Do not add one, and do not copy one from an existing model into a new source. This section is here so you can read, call, and migrate a model that already has them.

Publisher parses the annotation, lists the filters in the API, and **injects a `where:` clause server-side** when a caller passes a value (`filterParams` / `filter_params`). The annotation sits above the `source:` line:

```malloy
#(filter) [name=NAME] dimension=DIMENSION type=TYPE [implicit] [required]
```

| Part        | Meaning                                                                                                                             |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `name`      | The API parameter key. Defaults to the dimension name.                                                                              |
| `dimension` | The dimension the filter targets.                                                                                                   |
| `type`      | `equal` (`=`), `in` (any of several values), `like` (`~ '%value%'`), `greater_than` (`>`, exclusive), `less_than` (`<`, exclusive). |
| `required`  | The server returns 400 when a query supplies no value.                                                                              |
| `implicit`  | Hidden from the UI and the API filter list.                                                                                         |

Publisher formats the value from the dimension's type (`'value'`, bare `true`/`false`, `@YYYY-MM-DD`), so a caller passes it unquoted.

`#(filter)` is not a security boundary. A caller skips it with `bypassFilters: true` in the query request body, or by writing their own query text. Use givens with `#(authorize)` / `#(access_filter)` for access control.

**To migrate** a source, replace each annotation using the table above, then delete it. An optional filter (`type=in`, `like`, `equal`, a range) moves to the reports that offer it, declared and bound there, and the source is left unfiltered; a `required` one stays in the model as a no-default given. `type=greater_than` and `type=less_than` are exclusive, so a plain comparison you convert one to is `>` or `<`, not `>=` or `<=`. `docs/givens.md` § "Coming from `#(filter)`" has a worked conversion.
