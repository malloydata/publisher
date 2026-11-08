// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { GetObjectCommand, type S3 } from "@aws-sdk/client-s3";
import * as fs from "fs";
import * as path from "path";
import { Writable } from "stream";
import { EnvironmentNotFoundError } from "../../errors";
import { logger } from "../../logger";
import {
   assertSafeEnvironmentPath,
   safeJoinUnderRoot,
} from "../../path_safety";
import type { PackageFetchOptions, PackageSource } from "./types";
import { unzipEnvironment } from "./zip";

/** An `s3://bucket/prefix` location (a `.zip` key is one archive). */
export function isS3URL(location: string): boolean {
   return location.startsWith("s3://");
}

/** Every object under an S3 prefix, or one archive extracted. */
export class S3PackageSource implements PackageSource {
   /** `client` builds the S3 client on first use, so construction does no I/O. */
   constructor(private readonly client: () => S3) {}

   async fetch(
      s3Path: string,
      absoluteDirPath: string,
      { environmentName }: PackageFetchOptions,
   ): Promise<void> {
      const isCompressedFile = s3Path.endsWith(".zip");
      assertSafeEnvironmentPath(absoluteDirPath);
      const trimmedPath = s3Path.slice(5);
      const [bucketName, ...prefixParts] = trimmedPath.split("/");
      const prefix = prefixParts.join("/");
      const s3Client = this.client();

      if (isCompressedFile) {
         const zipFilePath = `${absoluteDirPath}.zip`;
         await fs.promises.mkdir(path.dirname(zipFilePath), {
            recursive: true,
         });

         const command = new GetObjectCommand({
            Bucket: bucketName,
            Key: prefix,
         });
         const item = await s3Client.send(command);
         if (!item.Body) {
            throw new EnvironmentNotFoundError(
               `Environment ${environmentName} not found in ${s3Path}`,
            );
         }
         const file = fs.createWriteStream(zipFilePath);
         item.Body.transformToWebStream().pipeTo(Writable.toWeb(file));
         await new Promise<void>((resolve, reject) => {
            file.on("error", reject);
            file.on("finish", resolve);
         });

         await unzipEnvironment(zipFilePath);
         logger.info(`Downloaded S3 zip file ${s3Path} to ${absoluteDirPath}`);
         return;
      }

      const objects = await s3Client.listObjectsV2({
         Bucket: bucketName,
         Prefix: prefix,
      });
      await fs.promises.rm(absoluteDirPath, { recursive: true, force: true });
      await fs.promises.mkdir(absoluteDirPath, { recursive: true });

      if (!objects.Contents || objects.Contents.length === 0) {
         throw new EnvironmentNotFoundError(
            `Environment ${environmentName} not found in ${s3Path}`,
         );
      }
      await Promise.all(
         objects.Contents?.map(async (object) => {
            const key = object.Key;
            if (!key) {
               return;
            }
            // Strip a leading `/`, as for GCS.
            const relativeFilePath = key
               .replace(prefix, "")
               .replace(/^\/+/, "");
            if (!relativeFilePath || relativeFilePath.endsWith("/")) {
               return;
            }
            const absoluteFilePath = safeJoinUnderRoot(
               absoluteDirPath,
               relativeFilePath,
            );
            await fs.promises.mkdir(path.dirname(absoluteFilePath), {
               recursive: true,
            });
            const command = new GetObjectCommand({
               Bucket: bucketName,
               Key: key,
            });
            const item = await s3Client.send(command);
            if (!item.Body) {
               return;
            }
            const file = fs.createWriteStream(absoluteFilePath);
            item.Body.transformToWebStream().pipeTo(Writable.toWeb(file));
            await new Promise<void>((resolve, reject) => {
               file.on("error", reject);
               file.on("finish", resolve);
            });
         }),
      );
      logger.info(`Downloaded S3 directory ${s3Path} to ${absoluteDirPath}`);
   }
}
