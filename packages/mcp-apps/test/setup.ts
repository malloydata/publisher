// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DOM environment for `bun test` in this package.
 *
 * Registered via `preload` in bunfig.toml so it runs once before any spec. Same
 * approach as packages/sdk/test/setup.ts, and lives outside `src/` for the same
 * reason: the widget build treats everything under `src/` as widget source.
 *
 * Only collapse_wrapper.spec.ts needs it. The rest of this package's specs are
 * pure functions and were written that way deliberately.
 */
import { GlobalRegistrator } from "@happy-dom/global-registrator";

GlobalRegistrator.register();
