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
         "Path to publisher.config.json; default: the one in the server root.",
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
         "Serve the bundled sample config when the server root has none; on when neither {root} nor {config} is given.",
      notes: "Set to `true` to fall back to the sample config bundled inside the installed package when neither `--config` is passed nor a `publisher.config.json` exists at the server root. The server sets this itself on a zero-flag start (so a bare `npx @malloy-publisher/server` boots the samples); passing `--config` or `--server_root` leaves it unset. Because the bundled config lives inside the install, relative package locations resolve against the server root in this mode rather than the config's directory.",
      defaultText: "on unless `root` or `config` is given; never in tests",
      aliases: {
         env: ["PUBLISHER_USE_BUNDLED_DEFAULT"],
         today: { reads: "exactly-true" },
      },
   }),
   setting({
      key: "initializeStorage",
      type: boolean(),
      default: false,
      kind: "launch",
      description: "Wipe persisted storage and re-sync it from the config.",
      notes: "Set to `true` (or pass `--init`) to **wipe persisted storage** (`publisher_data/`) and re-sync it from the config on boot. A first boot with empty storage loads the config automatically, so you only need this to reset state or pick up config changes. Also exposed as the `start:init` / `start:dev:init` scripts.",
      shortFlags: ["--init"],
      aliases: {
         env: ["INITIALIZE_STORAGE"],
         today: { reads: "exactly-true" },
      },
   }),
   setting({
      key: "check",
      type: boolean(),
      default: false,
      kind: "launch",
      description:
         "Load and compile every package the config names, print each problem, and exit: 0 clean, 1 problems found, 2 could not check. Binds no port.",
      notes: "For CI. Reads the config as written (the `--config` file, or `publisher.config.json` in the server root; never the bundled samples and never stored state), loads every package it names into a temporary server root it deletes afterwards, prints one line per problem to stdout (`<file>:<line>:<col>: error: <message> [<env>/<package>]`), then `PUBLISHER_CHECK_OK`, `PUBLISHER_CHECK_FAILED` or `PUBLISHER_CHECK_ERROR` to stderr. Exits `0` when everything loaded, `1` when anything did not, and `2` when it could not check (no config, a config or setting that does not parse, storage that would not open). Binds no port, writes nothing to the server root, and writes no `.mcp.json`. See [Check mode](#check-mode-for-ci).",
   }),
   setting({
      key: "checkFailOnWarnings",
      type: boolean(),
      default: false,
      kind: "launch",
      description:
         "With {check}, also exit 1 when a package that loaded reports a warning.",
      notes: "With `check`, also fail (exit `1`) when a package that loaded carries a `warn`-severity entry in its `warnings` (a render tag the renderer cannot apply, a refused `#@ persist`, a report that does not resolve, and the like). Off, those are printed as `warning:` lines and counted, and do not fail the check. A finding with severity `error` fails the check either way. Has no effect without `check`.",
   }),
   setting({
      key: "watchEnvironments",
      type: list(),
      default: [],
      kind: "launch",
      description:
         "Environments whose local packages are mounted in place for live reload; only the first auto-reloads.",
      notes: "Dev only. Mount the named environment's local-dir packages in place (a symlink, not a copy) and watch them, so edits to your source recompile that package and live-reload any open pages. Repeat the flag or use a comma-separated list to mount several in place; only the first one auto-reloads. Leave unset in production, where packages are copied and stay decoupled from their source.",
      shortFlags: ["--watch-env"],
      aliases: { env: ["PUBLISHER_WATCH"] },
   }),
];
