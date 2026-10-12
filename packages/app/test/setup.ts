// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DOM environment for `bun test` in the Console, the SDK's setup
 * (packages/sdk/test/setup.ts) verbatim: registered via `preload` in
 * bunfig.toml, outside `src/` so the build never sees it. Run specs from this
 * package (`bun run test:app` at the root), since bun reads `bunfig.toml` from
 * the working directory. The `test` script runs `bun test ./src` because
 * `tests/` holds the Playwright suite, which bun must not pick up.
 *
 * The specs import `@malloy-publisher/sdk` as the Console does, from its built
 * `dist/`, so `bun run build:sdk` comes first.
 */
import { GlobalRegistrator } from "@happy-dom/global-registrator";
import { afterEach } from "bun:test";

GlobalRegistrator.register();

// Imported dynamically, AFTER register(): Testing Library binds to `document`
// at module scope.
const { cleanup } = await import("@testing-library/react");

afterEach(cleanup);
