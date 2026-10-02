// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as fs from "fs";
import { ServerConfigurationError } from "../errors";

/**
 * Refuse a GOOGLE_APPLICATION_CREDENTIALS that names a directory, before
 * google-auth reads it.
 *
 * google-auth reports a directory as "does not exist, or it is not a file",
 * with no errno, which sends an operator looking for a file that is missing.
 * A directory is what a container runtime puts at a bind target whose host
 * path does not exist, so this is the usual shape of a mis-mounted key.
 *
 * Every other outcome is left to google-auth, which already names it: a
 * missing file, and one the server cannot read (EACCES).
 */
export function assertGoogleCredentialsIsNotADirectory(): void {
   const keyFile = process.env.GOOGLE_APPLICATION_CREDENTIALS;
   if (!keyFile) return;
   let isDirectory = false;
   try {
      isDirectory = fs.statSync(keyFile).isDirectory();
   } catch {
      return;
   }
   if (isDirectory) {
      throw new ServerConfigurationError(
         `GOOGLE_APPLICATION_CREDENTIALS names a directory, not a key file: '${keyFile}'. ` +
            `A bind mount whose source path does not exist creates a directory there; ` +
            `mount the key file itself.`,
      );
   }
}
