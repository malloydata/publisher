// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { helpText } from "./settings/reference";

/**
 * `--help`, `-h`: print the flags and exit. The text is generated from the
 * settings registry, so it lists every flag the server reads. Every other flag
 * is a setting's, read with the variables and the config file by the settings
 * loader (settings/settings.ts), so nothing here writes `process.env`.
 */
export function printHelpIfAsked(args: readonly string[]): void {
   if (!args.some((arg) => arg === "--help" || arg === "-h")) return;
   console.log(helpText());
   process.exit(0);
}
