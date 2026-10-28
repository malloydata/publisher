// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { setting } from "../setting";
import { boolean, list, string } from "../types";

/** The configuration design's "Launch settings" table: none has a file key. */
export const launch = [
   setting({
      key: "root",
      type: string(),
      default: ".",
      kind: "launch",
      description:
         "Directory served from, resolved against the working directory.",
      aliases: { env: ["SERVER_ROOT"], flags: ["--server_root"] },
   }),
   setting({
      key: "config",
      type: string(),
      kind: "launch",
      description:
         "Path to publisher.config.json; default <root>/publisher.config.json.",
      aliases: { env: ["PUBLISHER_CONFIG_PATH"], flags: ["--config"] },
   }),
   setting({
      key: "useBundledDefault",
      type: boolean(),
      default: (resolving) =>
         !resolving.isSet("root") &&
         !resolving.isSet("config") &&
         resolving.env("NODE_ENV") !== "test",
      kind: "launch",
      description:
         "Serve the bundled sample config when the server root has none; on when neither root nor config is given.",
      aliases: {
         env: ["PUBLISHER_USE_BUNDLED_DEFAULT"],
         today: { reads: "exactly-true", defaultFromFlags: true },
      },
   }),
   setting({
      key: "initializeStorage",
      type: boolean(),
      default: false,
      kind: "launch",
      description: "Wipe persisted storage and re-sync it from the config.",
      aliases: {
         env: ["INITIALIZE_STORAGE"],
         flags: ["--init"],
         today: { reads: "exactly-true" },
      },
   }),
   setting({
      key: "watchEnvironments",
      type: list(),
      default: [],
      kind: "launch",
      description:
         "Environments whose local packages are mounted in place for live reload; only the first auto-reloads.",
      aliases: {
         env: ["PUBLISHER_WATCH"],
         flags: ["--watch-env"],
         today: { flagsAppend: true },
      },
   }),
];
