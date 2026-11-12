// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every server suite runs with package versioning on. `bun test` preloads this
 * file (bunfig.toml) before any spec, so the settings each spec reads, and
 * every server a spec starts, see `PUBLISHER_PACKAGE_VERSIONING=on`. This is
 * the one place the suites turn it on. A spec of the server with versioning
 * off (an in-place install, the deprecated package PATCH) sets
 * `PUBLISHER_PACKAGE_VERSIONING=off` in the settings it installs
 * (test_helpers/settings_env.ts).
 */
process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
