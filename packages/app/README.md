# Publisher Server Development

In development, there are 2 servers- the Node.JS API server and the React Dev server.
In production, there's just 1 server- the Node server, and it serves the UI statically.
The development server will hot-load changes. Changes to the Node.JS will cause the server to restart, changes to the React UI should be hot-loaded. Though note that BOTH systems sometimes fail to detect/implement changes, so don't be shy about restarting them.

First, generate the OpenAPI implementation files. From the root publisher directory run:

```
bun generate-api-types
```

This is done also done as part of the production server & sdk builds.

To run in development, you'll want 2 terms (you can obv run them in the BG, but in dev mode they
can get gorked and you'll want to be able to kill/restart them):

```
bun run start:dev
```

```
bun run start:dev:react
```

### Linking Renderer code

The SDK depends on `@malloydata-render`. Often changes involve edits to both the Malloy render code and the app/SDK. It's useful to have the server run and depend on a local copy of the `render` code rather than the NPM package. TO set this up:

```
[clone the @malloydata/malloy repo]
cd MALLOY_ROOT/packages/malloy-render
bun link

cd PUBLISHER_ROOT/packages/sdk
bun link @malloydata/render
```

The publisher react server will now depend on your local copy of Malloy render. However, to push the changes, you must **build** the Malloy renderer:

```
cd MALLOY_ROOT/packages/malloy-render
npm run build
```

[Note: you'll need to build from the `malloy` root once to create all the deps the renderer needs]
