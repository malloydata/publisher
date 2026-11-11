# load-shapes

Import shapes the Publisher's package load has to get right (malloydata/publisher#1566, PRs #1575 and #1579).
Every `check_*` query has a fixed answer, listed below; malloydata/publisher's `packages/server/src/package_load/load_shapes.spec.ts` pins them on this package (its `tests/fixtures/load-shapes`).

| Folder | Shape |
|---|---|
| `diamond/` | a imports b and base, b imports base (`a.malloy`); left and right both import base, top imports both. Importers refine the same source differently. |
| `chain/` | six levels, each importing the one below. |
| `nested/` | three folders deep, `../`, `../../` and `./` imports. |
| `.lib/` + `dotfile/` | a file in a dot-folder (not a package model) imported by two models; it warns, and both must report it. |
| `spelled/` | a model at `my models/bäse.malloy`, imported as written and percent-encoded. |
| `styles/` | `--! styles` merged in the order Malloy reads imports. |
| `selective/` | `import { items } from` and an upper-case `IMPORT`. |
| `notebooks/` | a notebook importing across folders. |

## Expected results

Every model compiles. Each `check_*` query returns one row:

| Model :: query | Row |
|---|---|
| `diamond/a.malloy :: check_ab` | `{"row_count": 6, "v_total": 210, "a_scaled": 630, "b_scaled": 420}` |
| `diamond/top.malloy :: check_diamond` | `{"row_count": 6, "left_total": 420, "right_total": 1050}` |
| `chain/c5.malloy :: check_chain` | `{"row_count": 6, "s5_total": 300}` |
| `nested/marts/deep/report.malloy :: check_nested_report` | `{"order_count": 3, "amount_total": 600, "item_v": 30}` |
| `nested/index.malloy :: check_nested` | `{"order_count": 3, "amount_total": 600}` |
| `dotfile/first.malloy :: check_dotfile_first` | `{"n": 1}` |
| `dotfile/second.malloy :: check_dotfile_second` | `{"n": 1}` |
| `spelled/one.malloy :: check_spelled_one` | `{"row_count": 6, "v_total": 210}` |
| `spelled/two.malloy :: check_spelled_two` | `{"row_count": 6, "v_total": 210}` |
| `styles/root.malloy :: check_styles` | `{"n": 1}` |
| `selective/pick.malloy :: check_selective` | `{"row_count": 6, "v_total": 210}` |
| `selective/upper.malloy :: check_upper` | `{"row_count": 6, "v_total": 210}` |

- Only `dotfile/first.malloy` and `dotfile/second.malloy` carry a warning located in `.lib/shared.malloy` (`null-typed-field-definition`), one each.
- Data styles: `styles/root.malloy` and `styles/c.malloy` → `bar_chart`, `styles/b.malloy` → `line_chart`.
