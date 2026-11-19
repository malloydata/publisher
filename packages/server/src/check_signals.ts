// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Check mode's "could not check" stderr line, in a module that loads nothing
 * else: server.ts prints it when the settings fail to parse, before the
 * logger (which reads settings) may load.
 */

import { redactPgSecrets, redactSignedUrlParams } from "./pg_helpers";

/**
 * Text check mode prints, with a Postgres password and a signed URL's
 * secrets masked. Every line it writes passes through here: a finding, an
 * annotation and the summary can quote a setting's value or a driver's text,
 * and an annotation shows on a public pull request.
 */
export function redactCheckText(text: string): string {
   return redactSignedUrlParams(redactPgSecrets(text));
}

/** Check mode's exit code when it could not check at all. */
export const CHECK_COULD_NOT_RUN = 2;

/**
 * `PUBLISHER_CHECK_ERROR error="…"`: check mode could not look at the
 * packages (no config, a config or setting that does not parse, storage that
 * would not open). Redacted as `PUBLISHER_INIT_FAILED` is.
 */
export function checkErrorLine(message: string): string {
   const flat = message
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0)
      .join(" / ");
   return `PUBLISHER_CHECK_ERROR error=${JSON.stringify(redactCheckText(flat))}\n`;
}

/**
 * Whether this startup asked for check mode, even when its settings did not
 * parse: `--check` on the command line, or a `check` setting that read as on
 * or itself failed to read.
 */
export function checkRequested(
   argv: readonly string[],
   readCheck: () => boolean,
): boolean {
   try {
      return readCheck() || argv.includes("--check");
   } catch {
      return true;
   }
}

/**
 * A message as a bare GitHub Actions `::error::` workflow command, with `%`,
 * CR and LF escaped as GitHub reads them. For a failure with no file.
 */
export function githubErrorCommand(message: string): string {
   return `::error title=Malloy check::${redactCheckText(message)
      .replace(/%/g, "%25")
      .replace(/\r/g, "%0D")
      .replace(/\n/g, "%0A")}\n`;
}
