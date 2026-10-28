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
      notes: "Where Publisher keeps its own storage (`publisher_data/`, `publisher.db`), and where it looks for `publisher.config.json` when `--config` is not passed.",
      defaultText: "`.` (cwd)",
      aliases: { env: ["SERVER_ROOT"], flags: ["--server_root"] },
   }),
   setting({
      key: "config",
      type: string(),
      kind: "launch",
      description:
         "Path to publisher.config.json; default <root>/publisher.config.json.",
      notes: "Path to the `publisher.config.json` to read, instead of the one in the server root. A path that does not exist is reported at startup, and the server starts with no environments.",
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
      notes: "Set to `true` to fall back to the sample config bundled inside the installed package when neither `--config` is passed nor a `publisher.config.json` exists at the server root. The server sets this itself on a zero-flag start (so a bare `npx @malloy-publisher/server` boots the samples); passing `--config` or `--server_root` leaves it unset. Because the bundled config lives inside the install, relative package locations resolve against the server root in this mode rather than the config's directory.",
      defaultText:
         "on unless a `--server_root` or `--config` flag is given; never in tests",
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
      notes: "Set to `true` (or pass `--init`) to **wipe persisted storage** (`publisher_data/`) and re-sync it from the config on boot. A first boot with empty storage loads the config automatically, so you only need this to reset state or pick up config changes. Also exposed as the `start:init` / `start:dev:init` scripts.",
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
      notes: "Dev only. Mount the named environment's local-dir packages in place (a symlink, not a copy) and watch them, so edits to your source recompile that package and live-reload any open pages. Repeat the flag or use a comma-separated list to mount several in place; only the first one auto-reloads. Leave unset in production, where packages are copied and stay decoupled from their source.",
      aliases: {
         env: ["PUBLISHER_WATCH"],
         flags: ["--watch-env"],
         today: { flagsAppend: true },
      },
   }),
];
