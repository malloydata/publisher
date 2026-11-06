// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { afterAll, beforeAll } from "bun:test";
import { installVariables } from "../../src/test_helpers/settings_env";

/**
 * Turn package versioning on (`PUBLISHER_PACKAGE_VERSIONING=on`) for the suite
 * this is called in, and put the settings back after it. A publish reads the
 * setting when it runs, so an in-process server started by the suite publishes
 * versions while it is on. Call it before the suite's own `beforeAll`.
 */
export function versioningOnForSuite(): void {
   let restore: (() => void) | undefined;
   beforeAll(() => {
      restore = installVariables({ PUBLISHER_PACKAGE_VERSIONING: "on" });
   });
   afterAll(() => restore?.());
}
