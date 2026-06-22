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

## Process

1. Read the Visibility Seeds notes captured during discovery
2. For each field, apply the classification rules above
3. Present the classified list to the user for confirmation before applying access modifiers
