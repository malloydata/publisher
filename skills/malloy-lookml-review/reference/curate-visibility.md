# LookML Visibility → Malloy Access Modifiers (Step 8)

> Classify LookML visibility mechanisms and map them to Malloy access modifiers. This runs during Step 8 (CURATE) when the prior-art notes have a Visibility Seeds section.

## LookML Visibility Mechanisms

| LookML Mechanism | Semantics | Strength |
|---|---|---|
| `hidden: yes` | Hidden from Explore field picker, but still queryable via URL/API | Cosmetic: UI decluttering |
| `fields` (explore-level) | Excluded from an explore's available field pool. Not queryable. Can use `ALL_FIELDS*` with `-field` exclusions. | Structural: field genuinely inaccessible |
| `fields` (join-level) | Restricts which fields from a joined view enter the explore. Include-list only. | Structural: field never enters the pool |
| `fields_hidden_by_default: yes` | All view fields hidden unless individually opted in with `hidden: no` | Cosmetic: bulk hide |
| `required_access_grants` | User-attribute-based restriction. True security mechanism. | Security: access controlled |

## Classification → Malloy Treatment

For each hidden/excluded field, determine the **reason** and map to the correct Malloy treatment:

| Classification | LookML Signal | Malloy Treatment |
|---|---|---|
| **Intermediate calculation** | `hidden: yes` + referenced by other fields via `${field}` | `# hidden` tag: keep accessible, hide from display |
| **Join key / FK** | `hidden: yes` + used in `sql_on:` conditions | Keep public, no `#(doc)`: needed for joins |
| **UI clutter** | `hidden: yes` + not referenced elsewhere | Assess: if truly unused, `internal:` candidate; if just verbose, `# hidden` |
| **Explore-scoped exclusion** | Excluded via `fields` parameter | `internal:` candidate: field was structurally inaccessible |
| **Bulk hidden** | `fields_hidden_by_default: yes` on view | Treat each field individually based on its role |
| **Access-restricted** | `required_access_grants` | `private:` candidate: flag for user decision |
| **Not hidden** | No visibility restriction | Public + `#(doc)` candidate |

## Key Distinction

LookML `hidden: yes` ≈ Malloy `# hidden` (cosmetic). LookML `fields` exclusion ≈ Malloy `internal:` (structural).

**Do NOT map `hidden: yes` directly to `internal:`.** That over-restricts fields that may be needed as intermediate calculations or join keys.

## Pattern: Restricted-field masking (`include { private: } + pick`)

When a sensitive field should be *present in a redacted/bucketed form* rather than dropped, don't reach for an access modifier alone: modifiers hide or expose, they don't transform. Keep the raw column **`private:`** and derive a masked dimension from it in the source's immediate `extend {}`: a `private:` field is referenceable from the immediate extension, so the raw value stays out of the queryable surface while the mask is public. The mask must key on **data the model can see**, a row-level column or a source `parameter`, not on the viewer:

```malloy
##! experimental.access_modifiers
source: revenue is conn.table('…') include {
  private: raw_amount              // raw stays out of the queryable surface…
} extend {
  dimension: amount is             // …but is referenceable here, in the immediate extension
    pick null when is_confidential          // is_confidential is a ROW column, not the viewer
    else raw_amount
  // or bucket instead of null-out:
  //   pick 'under 50k' when raw_amount < 50000
  //   pick '50k–100k'  when raw_amount < 100000
  //   else '100k+'
}
```

Give the mask a **different name** from the private raw column (`amount` vs `raw_amount`), reusing the name is a redefinition. If the mask must carry the raw column's *exact* original name, that forces a `rename:`, which pushes you onto the `extend { rename }` path that does **not** compose with `include {}` (see `skill:malloy-gotchas-modeling`); there the raw column can only be `# hidden` (dropped from display but still queryable), so prefer a distinct mask name and keep the raw `private:`.

**This is NOT per-viewer access control.** LookML `required_access_grants` gates on the *viewing user's* attributes; Malloy models have no per-viewer context, so a model-layer `pick` can only redact based on row data or a parameter. To gate a field by *caller identity/role* (the real `required_access_grants` equivalent), use `#(authorize)` on the source, driven by trusted attributes in Malloy Publisher (see `skill:malloy-model` § Access Control). Masking and `#(authorize)` are independent layers; use the mask for "everyone sees a coarsened value," `#(authorize)` for "only some callers see the field at all."

## Pattern: Long→wide custom-field pivot

See `build-derived-tables.md` → "Long→wide entity-values pivot" for turning an entity-attribute-value (EAV) custom-field table into wide columns with **filtered aggregates**, the right move when LookML modeled N per-attribute joins.

## Process

1. Read the Visibility Seeds notes captured during discovery
2. For each field, apply the classification rules above
3. Present the classified list to the user for confirmation before applying access modifiers
