<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# RLS Roles and Visibility (Step 8)

> Map Power BI row-level security roles and hidden objects onto Malloy access modifiers and the `#(authorize)` / `#(access_filter)` annotations. Expect most roles to need a decision rather than a translation.

Read the access control section of `skill:malloy-model` for the annotation grammar before writing any gate. This file is about what Power BI hands you and how much of it fits.

## 1. What a Power BI Role Looks Like

```
role SalesRegion
    modelPermission: read

    tablePermission SalesRep = [Email] = USERPRINCIPALNAME()
```

A role is a name, a model permission, and a DAX filter expression per table. A user is assigned to roles in the Power BI service; a user in no role sees everything (or nothing, depending on workspace permissions), and a user in two roles sees the **union** of what each allows.

## 1a. Say Out Loud That This Is a Posture Change

Power BI RLS is enforced by the service against an **authenticated principal**. The user cannot choose their own role; `USERPRINCIPALNAME()` returns who they actually are.

Malloy givens are **caller-asserted**. Anyone who can reach the query API can claim `{"REGION": "everything"}`. Neither `#(authorize)` nor `#(access_filter)` is a real boundary unless it sits behind a trusted tier that sets givens from its own verified context and never from the caller.

So translating an RLS role into a gate, on its own, **weakens the control**. That is a deployment decision the user has to make knowingly, not a detail to leave in a footnote. State it in the migration report next to every translated role, and never report a role as "translated" when the posture dropped without saying so. This is the same class of omission as the propagation gap in section 3, and it is easier to miss because the translation looks complete.

## 2. The Grammar Gap Is the Main Finding

A Power BI `tablePermission` is **arbitrary DAX**. A Malloy gate body is a **narrow grammar**: one or more terms joined only by `and`, each term either `field_path <op> $GIVEN` for `#(access_filter)` or `'literal' <op> $GIVEN` for `#(authorize)`. No `or`, no `not`, no `!=`, no ordering comparisons, no function calls.

The operator is **fixed by the given's declared arity** (`in` for a list, `=` for a scalar); `<`, `>`, `<=`, `>=` and `!=` are refused outright.

That gap is not a defect on either side. Power BI evaluates DAX per query; the Malloy gate is parsed and decided by the publisher before the query compiles, which is what lets a refused caller get a 403 instead of a zero. But it means **translation is a narrowing exercise**, and the honest answer for many roles is that they do not fit as written.

Classify each role's filter:

| DAX filter | Fits? | Translation |
|---|---|---|
| `[Email] = USERPRINCIPALNAME()` | Yes | `#(access_filter) email = $USER_EMAIL` |
| `[OrgId] IN VALUES(...)` against a caller attribute | Yes | `#(access_filter) org_id in $ORG_IDS` |
| `[Region] = "East"` (static, one role per region) | Not as a filter | A constant, not a given. See section 4. |
| `[A] = X && [B] = Y` | Yes | Two and-joined terms, or two stacked notes |
| `[A] = X \|\| [B] = Y` | **No** | `or` is refused anywhere it appears. Two extension sources over a locked base, one per population; see section 4. |
| `NOT([Region] = "East")` | **No** | No negation. Restate as the positive set. |
| `[Region] <> "East"` | **No** | `!=` is refused. Same. |
| `UPPER([Region]) = USERNAME()` | **No** | No function calls. Normalize the column upstream. |
| `LOOKUPVALUE(...)` over a security table | **No** | Dynamic RLS via a bridge; see section 5. |
| `PATHCONTAINS(Users[Path], USERNAME())` | **No** | Parent-child hierarchy; flatten upstream. |

## 3. Filter Propagation Does Not Carry Across

**This is the gap most likely to be missed.** In Power BI, an RLS filter on a dimension table **propagates through relationships** to the fact table. Filtering `DimRegion` restricts `FactSales` automatically, because that is what relationships do.

A Malloy gate applies to **the source a query enters through**. A gate on a source reached only via `join_*` **never fires, at any depth**. Not "propagates differently": it does not fire at all, so anything ungated that joins a gated source hands that source's rows to every caller.

So a role whose DAX filters one small dimension table may be protecting a dozen fact tables. Translating it as one annotation on that dimension leaves every fact source open to anyone who queries the fact directly. For each role, enumerate **every table the filter reaches through the relationship graph**, and gate each source that a query can enter through.

**The field-path remedy has a hard limit.** Reaching the filtering column from the fact source through the join works **only across `join_one` hops**. A path crossing `join_many` or `join_cross` is refused with a fanout error, and a segment that cannot be resolved fails closed. Power BI's bidirectional and many-to-many RLS shapes land exactly there, so for those the answer is a restructure, not a longer field path.

Get this wrong and the result is not an error. It is data served to someone who should not see it.

## 4. Static Roles Are Not Row-Level Filters

A very common Power BI pattern is one role per value: `role East` filtering `[Region] = "East"`, `role West` filtering `[Region] = "West"`, and so on. The filter is a constant; the variable is which role the user is in.

That is not `#(access_filter)` material, because a Malloy row-level term must reference a given. Two options:

1. **Give the caller the value.** If the caller's region can be carried as a given, the whole family of roles collapses to one `#(access_filter) region = $REGION`. This is almost always the better model and the migration is a good moment to propose it.
2. **One extension source per population, over a locked base.** Where the populations genuinely differ in kind rather than in a value, model each as its own extension with its own gate. This is also the answer for a role set that needs `or`.

   **The base must be locked or the pattern grants everyone everything.** A source with no gate is an open entry point, so extensions alone protect nothing: put `#(authorize) false` on the base and let each extension declare its own rule. An extension that declares no gate *inherits* the base's lock rather than lifting it, and `#(authorize) true` is the only spelling that re-opens it (legal beside an `#(access_filter)`, so a population can be admitted and still row-scoped).

   ```malloy
   #(authorize) false
   source: sales_base is conn.table('dbo.FactSales') extend {}

   #(authorize) true
   #(access_filter) region in $REGIONS
   source: sales is sales_base extend {}

   #(authorize) 'admin' in $GROUPS
   source: sales_admin is sales_base extend {}
   ```

   That is also how a Power BI role that sees everything translates: another extension source, never an exception that skips the gate.

   Every given a gate references must be declared on the entry model's own surface and **must carry no default** (a default would admit a caller on a value the gate's line never shows), and the file needs `##! experimental.givens`.

## 5. Dynamic RLS via a Security Table

The other common pattern: a `UserSecurity` table mapping user principal names to the values they may see, joined into the model, with a filter like `LOOKUPVALUE(UserSecurity[Region], UserSecurity[Email], USERPRINCIPALNAME())`.

The function call does not fit the gate grammar. What fits is the resolved result: if the caller's permitted values can be resolved into a given before the query, the gate becomes `#(access_filter) region in $REGIONS` and the security table stops being part of the model.

Where that resolution happens is a deployment question, not a modeling one. Flag it for the user with the security table's contents described, and do not invent a mechanism.

## 6. Gate Versus Filter Is a Real Choice

Power BI conflates them: a user with no access to any row sees an empty report. Malloy separates them deliberately.

- `#(authorize)` asks **may this caller reach this source at all**, and a denial is a **403**.
- `#(access_filter)` asks **which rows**, and a caller matching nothing gets a **200 with no rows**.

`modelPermission: read` on a role is closer to `#(authorize)`: the question of whether this population may touch this model at all. The `tablePermission` filters are `#(access_filter)`.

Writing a term on the wrong annotation is refused at load, both directions, which is a feature: a constant predicate grafted as a row filter would serve a refused caller an empty result instead of a denial, and an empty result is indistinguishable from "no data matched."

## 7. Hidden Objects and Perspectives

Separate from security, and do not conflate them. `isHidden: true` in Power BI is a **display** decision, not a permission: a hidden column is still queryable by anyone who can reach the model.

Classify each hidden object by reason before mapping it:

**Do NOT map `isHidden` to `internal:` mechanically.** `internal` fields are **not reachable through join paths**, and Power BI hides essentially every surrogate key by default, so the mechanical mapping marks the join keys internal and breaks the joins that depend on them.

| Reason it is hidden | Malloy |
|---|---|
| Key column / foreign key | **Keep public.** The joins need it. |
| Intermediate input to a calculation | `# hidden` |
| Clutter, superseded, deprecated | `# hidden`, or drop it |
| Genuinely sensitive | **Not `isHidden`.** See object-level security below. |

`# hidden` is a render tag: it belongs on fields a user should not see in a picker, not on anything that needs to stay reachable. Reserve `internal:` for fields nothing outside the source should reach at all.

### Object-Level Security Is the Real One

Power BI does have a genuine object-level permission, and it lives in the same `role` block as the row filters:

```
role Restricted
    modelPermission: read

    tablePermission Salary =
    columnPermission Salary[Amount] = none
```

`metadataPermission` / `columnPermission` set to `none` is a real restriction, unlike `isHidden`. **That** is what maps to `private:` / `internal:`. A role reader that only looks at `tablePermission` filter expressions drops OLS silently, which is a permission removed without anyone deciding to remove it.

A column hidden merely with `isHidden` and no OLS was never actually protected in Power BI. The migration is when someone should hear that.

**Perspectives** are a curated subset of the model someone already thought about. They are the best available evidence for what each audience needs and are worth reading before proposing access modifiers.

## 8. Reporting

For each role, report: role name, tables filtered, the DAX, whether it fits the gate grammar, the proposed annotation or the decision needed, and **every source that needs gating once propagation is accounted for**.

State plainly which roles did not translate. A role silently dropped is a permission silently removed.
