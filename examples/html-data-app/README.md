# html-data-app — example in-package data app

Drop an `index.html` next to your `.malloy` files; Publisher serves it at
`/environments/<env>/packages/<pkg>/index.html`. Live reload, embeddable
iframe, no build step. See
[`docs/embedded-data-apps.md`](../../docs/embedded-data-apps.md) for the
full design.

## Files

- `publisher.json` — package manifest.
- `carriers.malloy` — the semantic model.
- `carriers.parquet` — the data.
- `index.html` — Chart.js dashboard with KPI tiles, two charts, a data
  table, and three dropdown filters. Calls `Publisher.query()` to talk to
  the Publisher API.
- `embed-test.html` — host page that demonstrates `Publisher.embed()`
  iframing the dashboard.

## Try it

From the repo root:

```bash
# Make a workspace directory containing this package
mkdir -p /tmp/publisher-demo
cp -R examples/html-data-app /tmp/publisher-demo/
cat > /tmp/publisher-demo/publisher.config.json <<'JSON'
{
  "frozenConfig": false,
  "environments": [
    {
      "name": "demo",
      "packages": [
        { "name": "html-data-app", "location": "./html-data-app" }
      ],
      "connections": []
    }
  ]
}
JSON

# Start Publisher in watch mode for the `demo` environment.
# --watch-env makes Publisher SYMLINK the package into publisher_data/
# instead of copying, so edits to your source dir are picked up live.
SERVER_ROOT=/tmp/publisher-demo \
   bun run packages/server/src/server.ts --watch-env demo
```

Then open <http://localhost:4000/environments/demo/packages/html-data-app/>.

Edit `/tmp/publisher-demo/html-data-app/carriers.malloy` (e.g. tweak a `limit:`)
or `index.html`, save, and the open browser tab auto-reloads with the change.

The embed demo is at <http://localhost:4000/environments/demo/packages/html-data-app/embed-test.html>.

## Without watch mode

If you start Publisher without `--watch-env`, edits to your source dir won't
auto-reload — Publisher serves a copy of the package taken at startup. That's
the right default for production, but the wrong default for development. Use
`--watch-env <envName>` (or `PUBLISHER_WATCH=name1,name2`) to switch to the
in-place symlink mount for local-dir packages.
