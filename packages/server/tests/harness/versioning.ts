// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { afterAll, beforeAll } from "bun:test";

/**
 * Turn package versioning on (`PUBLISHER_PACKAGE_VERSIONING=on`) for the suite
 * this is called in, and put the setting back after it. The flag is read when
 * a publish runs, so an in-process server started by the suite publishes
 * versions while it is on. Call it before the suite's own `beforeAll`.
 */
export function versioningOnForSuite(): void {
   let saved: string | undefined;
   beforeAll(() => {
      saved = process.env.PUBLISHER_PACKAGE_VERSIONING;
      process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
   });
   afterAll(() => {
      if (saved === undefined) delete process.env.PUBLISHER_PACKAGE_VERSIONING;
      else process.env.PUBLISHER_PACKAGE_VERSIONING = saved;
   });
}
