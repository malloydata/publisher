# Access Modifiers: Curate the Interface

Add `##! experimental.access_modifiers` at the top of base source files.

**Every column must be explicitly declared** in `include {}`, listed under a single `public:` block with `#(doc)` tags, or under `internal:`. Do not use `public: *`.

**`internal` fields are NOT accessible through join paths.** Most fields should stay public.

**GOTCHA:** `include { except: a, b }` without a `public:` block makes remaining fields **private**, not public.

**What to mark `internal` (verify via index query + confirm with user first):**
- Empty/garbage columns
- Raw JSON/array blobs (when all useful fields extracted as dimensions)
- Duplicate columns where only one is correct
- Raw columns replaced by a derived dimension

**What to mark `private` (only after user confirms, very rare):** Highly sensitive data only (SSNs, raw credit cards, passwords).

**Include positioning:**
- `include { } extend { }`: New definitions in extend are PUBLIC
- `extend { } include { }`: Include covers everything
