// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Storage } from "@google-cloud/storage";
import * as fs from "fs";
import * as path from "path";
import { EnvironmentNotFoundError } from "../../errors";
import { logger } from "../../logger";
import {
   assertSafeEnvironmentPath,
   safeJoinUnderRoot,
} from "../../path_safety";
import { assertGoogleCredentialsIsNotADirectory } from "../google_credentials";
import type { PackageFetchOptions, PackageSource } from "./types";
import { downloadAndUnzip } from "./zip";

/** A `gs://bucket/prefix` location (a `.zip` key is one archive). */
export function isGCSURL(location: string): boolean {
   return location.startsWith("gs://");
}

/** Every object under a GCS prefix, or one archive extracted. */
export class GcsPackageSource implements PackageSource {
   /** `client` builds the GCS client on first use, so construction does no I/O. */
   constructor(private readonly client: () => Storage) {}

   async fetch(
      gcsPath: string,
      absoluteDirPath: string,
      { environmentName }: PackageFetchOptions,
   ): Promise<void> {
      const isCompressedFile = gcsPath.endsWith(".zip");
      assertSafeEnvironmentPath(absoluteDirPath);
      assertGoogleCredentialsIsNotADirectory();
      const trimmedPath = gcsPath.slice(5);
      const [bucketName, ...prefixParts] = trimmedPath.split("/");
      const prefix = prefixParts.join("/");
      const [files] = await this.client().bucket(bucketName).getFiles({
         prefix,
      });
      if (files.length === 0) {
         throw new EnvironmentNotFoundError(
            `Environment ${environmentName} not found in ${gcsPath}`,
         );
      }
      if (isCompressedFile) {
         // One archive, extracted beside itself into the target, then removed.
         const archivePath = `${absoluteDirPath}.zip`;
         await downloadAndUnzip(archivePath, async () => {
            await fs.promises.mkdir(path.dirname(archivePath), {
               recursive: true,
            });
            await Promise.all(
               files
                  .filter((file) => !file.name.endsWith("/"))
                  .map(async (file) =>
                     fs.promises.writeFile(archivePath, await file.download()),
                  ),
            );
         });
         logger.info(
            `Downloaded GCS zip file ${gcsPath} to ${absoluteDirPath}`,
         );
         return;
      }
      await fs.promises.rm(absoluteDirPath, {
         recursive: true,
         force: true,
      });
      await fs.promises.mkdir(absoluteDirPath, { recursive: true });
      await Promise.all(
         files.map(async (file) => {
            // Strip a leading `/` (prefix without trailing slash), which
            // `safeJoinUnderRoot` would reject as absolute.
            const relativeFilePath = file.name
               .replace(prefix, "")
               .replace(/^\/+/, "");
            const absoluteFilePath = safeJoinUnderRoot(
               absoluteDirPath,
               relativeFilePath,
            );
            if (file.name.endsWith("/")) {
               return;
            }
            await fs.promises.mkdir(path.dirname(absoluteFilePath), {
               recursive: true,
            });
            return fs.promises.writeFile(
               absoluteFilePath,
               await file.download(),
            );
         }),
      );
      logger.info(`Downloaded GCS directory ${gcsPath} to ${absoluteDirPath}`);
   }
}
