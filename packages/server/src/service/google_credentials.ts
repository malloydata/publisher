// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as fs from "fs";
import { ServerConfigurationError } from "../errors";

/**
 * Refuse a GOOGLE_APPLICATION_CREDENTIALS that names a directory, before
 * google-auth reads it.
 *
 * google-auth reports a directory as "does not exist, or it is not a file",
 * which misleads; a directory is what a container runtime creates at a bind
 * target whose host path is missing, the usual mis-mounted key. Other
 * failures (missing file, EACCES) are left to google-auth, which names them.
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
