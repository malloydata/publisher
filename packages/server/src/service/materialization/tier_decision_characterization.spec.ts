// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Characterization of where a table lives (its tier), as each site that
 * decides it today answers for the same inputs: `PERSIST_STORAGE_MODE`, the
 * instruction's destination, and the source's declared `storage=`. The
 * approved refactor decides the tier once; this table is what that one
 * decision must reproduce at each site, or the diff that shows where the
 * sites disagreed.
 *
 * Sites read through their exported functions:
 * - plan: `addressDestination` (the destination folded into an address, with
 *   PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION on);
 * - auto-run: `resolveStorageDestination` (the destination it instructs);
 * - run: `buildTierOf` (the tier the build and its telemetry take, the same
 *   expression as source_build's and instructed_build's private checks);
 * - run: `assertInstructedDestinationWritable` (the per-source refusal).
 * - bind: `storageWritable` of the mode, the one decision the manifest
 *   committer and the environment's store rebinds (`environment.ts`
 *   `rebindServeBindingsFromLocalStore`, `bindStoreEntries`) take before
 *   binding a manifest's `storage=` entries (`storage` bound or not).
 */

import { afterEach, describe, expect, it } from "bun:test";
import { installSettings } from "../../test_helpers/settings_env";
import {
   fakeSource,
   makeInstruction,
} from "../../test_helpers/materialization_test_fixtures";
import { getPersistStorageMode } from "../../config";
import { storageWritable } from "./domain/placement";
import { addressDestination } from "./plan/addressing";
import { assertInstructedDestinationWritable } from "./run/materialization_instructed_run";
import { resolveStorageDestination } from "./run/build_upstreams";
import { buildTierOf } from "./run/run_telemetry";

const MODES = ["off", "write-only", "on"] as const;

let restore: (() => void) | undefined;
afterEach(() => {
   restore?.();
   restore = undefined;
});

/** Every site's answer for one mode, instructed destination and declaration. */
function decisions(
   mode: (typeof MODES)[number],
   instructed: string | undefined,
   declared: string | undefined,
): string {
   restore?.();
   restore = installSettings({
      env: {
         PERSIST_STORAGE_MODE: mode,
         PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION: "true",
      },
   }).restore;
   const source = fakeSource({
      name: "s",
      sourceEntityId: "addr-s",
      ...(declared ? { annotationFields: { storage: declared } } : {}),
   });
   const instruction = makeInstruction(
      instructed ? { destination: instructed } : {},
   );
   let guard: string;
   try {
      assertInstructedDestinationWritable(source, instruction, {
         tables: {},
      } as never);
      guard = "ok";
   } catch (err) {
      guard = /PERSIST_STORAGE_MODE is off/.test((err as Error).message)
         ? "refused: mode off"
         : /instructed with no storage destination/.test((err as Error).message)
           ? "refused: no destination"
           : `refused: ${(err as Error).message}`;
   }
   return [
      `address ${addressDestination(source) ?? "-"}`,
      `auto-run ${resolveStorageDestination(source) ?? "-"}`,
      `build ${buildTierOf(instruction)}`,
      `guard ${guard}`,
      `bind ${storageWritable(getPersistStorageMode()) ? "storage" : "database only"}`,
   ].join(", ");
}

/** mode | instructed destination | declared storage= -> each site's answer. */
const TIERS: Record<string, string> = {
   "off | - | -":
      "address -, auto-run -, build database, guard ok, bind database only",
   "off | - | lake":
      "address -, auto-run -, build database, guard refused: no destination, bind database only",
   "off | lake | -":
      "address -, auto-run -, build database, guard refused: mode off, bind database only",
   "off | lake | lake":
      "address -, auto-run -, build database, guard refused: mode off, bind database only",
   "write-only | - | -":
      "address -, auto-run -, build database, guard ok, bind storage",
   "write-only | - | lake":
      "address lake, auto-run lake, build database, guard refused: no destination, bind storage",
   "write-only | lake | -":
      "address -, auto-run -, build storage, guard ok, bind storage",
   "write-only | lake | lake":
      "address lake, auto-run lake, build storage, guard ok, bind storage",
   "on | - | -":
      "address -, auto-run -, build database, guard ok, bind storage",
   "on | - | lake":
      "address lake, auto-run lake, build database, guard refused: no destination, bind storage",
   "on | lake | -":
      "address -, auto-run -, build storage, guard ok, bind storage",
   "on | lake | lake":
      "address lake, auto-run lake, build storage, guard ok, bind storage",
};

describe("the tier decision at each site", () => {
   for (const mode of MODES) {
      for (const instructed of [undefined, "lake"]) {
         for (const declared of [undefined, "lake"]) {
            const label = `${mode} | ${instructed ?? "-"} | ${declared ?? "-"}`;
            it(label, () => {
               expect(decisions(mode, instructed, declared)).toBe(TIERS[label]);
            });
         }
      }
   }

   it("folds no destination into an address with the switch off, whatever the mode", () => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION: "false",
         },
      }).restore;
      const source = fakeSource({
         name: "s",
         sourceEntityId: "addr-s",
         annotationFields: { storage: "lake" },
      });
      expect(addressDestination(source)).toBeUndefined();
      expect(resolveStorageDestination(source)).toBe("lake");
   });
});
