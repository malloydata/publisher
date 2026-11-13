<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Access Modifiers: Curate the Interface

Add `##! experimental.access_modifiers` at the top of base source files.

**Every column must be explicitly declared** in `include {}`, listed under a single `public:` block with `#(doc)` tags, or under `internal:`. Do not use `public: *`.

**`internal` fields are NOT accessible through join paths.** They ARE readable by any caller's query that names them in its own `extend` or `include { public: f }`, so `internal` is curation, not protection: use `private` or `#(authorize)` for data a caller must not read. Most fields should stay public.

**GOTCHA:** `include { except: a, b }` without a `public:` block makes remaining fields **private**, not public.

**What to mark `internal` (verify via index query + confirm with user first):**
- Empty/garbage columns
- Raw JSON/array blobs (when all useful fields extracted as dimensions)
- Duplicate columns where only one is correct
- Raw columns replaced by a derived dimension

**What to mark `private` (only after user confirms):** Data a caller must not read: national IDs, raw card numbers, passwords and tokens, and PII or compensation (email, phone, address, salary) unless an `#(authorize)` gate on the source already limits who can query it. These are the columns `malloy-review` rule G-03 blocks on when left `internal`.

**A `private` field can only be used in the source's own `extend {}`.** Define every measure, dimension or join that reads it there. A later `source: x is <this source> extend {...}` that references it fails to compile with `'<field>' is private`. Measures defined before the field went private keep working. A row-level gate (`#(access_filter)`) on the source can read it too, so a tenant column can be private and still scope the rows.

**Include positioning:**
- `include { } extend { }`: New definitions in extend are PUBLIC
- `extend { } include { }`: Include covers everything
