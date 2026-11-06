// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it, spyOn } from "bun:test";
import { printHelpIfAsked } from "./cli_args";
import { settings } from "./settings";
import { withSettings } from "./test_helpers/settings_env";

/**
 * Flags are read by the settings loader. Each flag's own rows (its value, a
 * flag with no value, a flag over its variable, the bundled-config default)
 * are in settings/contract.spec.ts and settings/settings.spec.ts; these are
 * the combinations of a flag and a variable those rows leave out.
 */
describe("flags read with the variables", () => {
   it("turns each API off with its --no- switch, over a variable that turns it on", () => {
      withSettings(
         {
            env: {
               PUBLISHER_SERVING_API_ENABLED: "true",
               PUBLISHER_ADMIN_API_ENABLED: "true",
            },
            argv: ["--no-serving-api-enabled", "--no-admin-api-enabled"],
         },
         () => {
            expect(settings.get("servingApiEnabled")).toBe(false);
            expect(settings.get("adminApiEnabled")).toBe(false);
         },
      );
   });

   it("appends repeated --watch-env to an existing PUBLISHER_WATCH", () => {
      withSettings(
         {
            env: { PUBLISHER_WATCH: "a" },
            argv: ["--watch-env", "b", "--watch-env", "c"],
         },
         () => {
            expect(settings.get("watchEnvironments")).toEqual(["a", "b", "c"]);
         },
      );
   });
});

describe("printHelpIfAsked", () => {
   const spies: Array<{ mockRestore: () => void }> = [];

   afterEach(() => {
      while (spies.length) spies.pop()!.mockRestore();
   });

   const watch = () => {
      const log = spyOn(console, "log").mockImplementation(() => {});
      const exit = spyOn(process, "exit").mockImplementation(
         (() => undefined) as never,
      );
      spies.push(log, exit);
      return { log, exit };
   };

   it.each([["--help"], ["-h"]])(
      "prints the usage and exits 0 on %s",
      (flag) => {
         const { log, exit } = watch();
         printHelpIfAsked(["--port", "4100", flag]);
         expect(log).toHaveBeenCalledWith("Usage: malloy-publisher [options]");
         expect(exit).toHaveBeenCalledTimes(1);
         expect(exit).toHaveBeenCalledWith(0);
      },
   );

   it("neither prints nor exits without --help, so the server starts", () => {
      const { log, exit } = watch();
      printHelpIfAsked([]);
      printHelpIfAsked(["--port", "4100", "--init", "--help-me"]);
      expect(log).not.toHaveBeenCalled();
      expect(exit).not.toHaveBeenCalled();
   });
});
