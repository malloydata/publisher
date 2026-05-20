# Publisher app — Playwright E2E

End-to-end tests for the React SPA in `packages/app`.

## Running tests

All commands from `packages/app/`.

### Against a running publisher (fastest loop)

Start the publisher once, leave it running, then iterate on specs:

```bash
# In one terminal — pick one:
bun run start:init         # built publisher on :4000
bun run dev                # Vite dev server on :5173, proxies /api/v0 → :4000

# In another terminal:
PLAYWRIGHT_USE_WEBSERVER=0 PUBLISHER_URL=http://localhost:4000 bunx playwright test
# or against the dev server for instant SDK edits:
PLAYWRIGHT_USE_WEBSERVER=0 PUBLISHER_URL=http://localhost:5173 bunx playwright test
```

### Watch it run (headed)

```bash
PLAYWRIGHT_USE_WEBSERVER=0 PUBLISHER_URL=http://localhost:5173 bunx playwright test --headed
```

### Let Playwright spawn the server

Default behavior — slower because it builds & boots the publisher:

```bash
bun run test:playwright
```

### Run a single spec or test

```bash
bunx playwright test tests/playwright/packages.spec.ts
bunx playwright test -g "create env → add package from git"
```

### Debug a failure

```bash
bunx playwright test --debug -g "<title substring>"
bunx playwright show-report            # open the HTML report (after a run)
bunx playwright show-trace test-results/<folder>/trace.zip
```

## Files

```
tests/playwright/
├── global-setup.ts              # polls /api/v0/status until operationalState=serving
├── helpers/
│   ├── fixtures.ts              # DEFAULT_ENV, PACKAGES, tmpName()
│   ├── navigation.ts            # gotoHome / openEnvironment / openPackage
│   └── publisherStatus.ts       # fetches /api/v0/status; applies frozenConfig → mutable=false
├── environments.spec.ts         # Home + env CRUD + mutability parity
├── packages.spec.ts             # package list + full create-from-git → open → delete lifecycle
├── package-models.spec.ts       # .malloy list, open, source combobox, run query → assert rows
├── package-notebooks.spec.ts    # .malloynb list, open → workbook route, content rendered
└── package-databases.spec.ts    # embedded DBs + schema dialog + connection CRUD
```

## Mutable vs frozen publishers

The UI hides create/edit/delete controls when the server reports `frozenConfig: true`
or `mutable: false` (see `packages/sdk/src/components/ServerProvider.tsx`). Tests adapt:

- CRUD suites: `test.skip(!mutable, …)` via `getPublisherStatus(baseURL)` in `beforeAll`.
- Mutability-parity tests assert the control count matches the status flag, so they pass in both modes.

## CI

Runs on every `pull_request` to `main` via `.github/workflows/app-playwright.yml`:

1. Build SDK + server + app.
2. Install Chromium via `bunx playwright install --with-deps chromium`.
3. Run the suite (`playwright.config.ts` spawns `start:init`; `global-setup.ts` waits for `serving`).
4. Upload `playwright-report/` always; upload `test-results/` (traces, videos) on failure.

In CI the config sets `retries: 1`, `trace: retain-on-failure`, `video: retain-on-failure`,
and adds the `html` reporter on top of `list`.

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `PUBLISHER_URL` | `http://localhost:4000` | Base URL under test |
| `PLAYWRIGHT_USE_WEBSERVER` | unset (on) | Set to `0` to reuse an already-running server |
| `CI` | unset | When set, enables retries + HTML reporter + trace/video artifacts |

## Writing new tests — notes that bite

- **Package tiles** are `<div>`, not `<a>`. Use `page.getByText(pkg, { exact: true })` to click.
- **Source combobox** is an MUI Autocomplete — assert with `toHaveValue('people')`, not `toContainText`.
- `imdb.malloy` is a substring of `imdb.malloynb`; always pass `{ exact: true }` on treeitems.
- Action buttons carry stable aria-labels: `"Environment actions for <name>"`,
  `"Package actions for <name>"`, `"Edit connection <name>"`, `"Delete connection <name>"`.
- The sticky `Publisher API` header link can intercept pointer events on the env-card overflow
  button — use `.dispatchEvent("click")` to fire the React handler directly.
- Package sections render `Fetching …` placeholders — wait for a row/treeitem, not just the section heading.
- Adding a package from a git URL can take up to a minute; allow `{ timeout: 60_000 }` on the
  tile-visible assertion.

## Related

- `e2e/tests/workspace.playwright.spec.ts` — legacy suite using older "Project" copy.
  New coverage belongs here; `e2e/` will be retired.
