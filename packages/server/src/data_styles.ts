// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { URLReader } from "@malloydata/malloy";
import { DataStyles } from "@malloydata/render";
import { logger } from "./logger";

function compileDataStyles(styles: string): DataStyles {
   try {
      return JSON.parse(styles) as DataStyles;
   } catch (error) {
      throw new Error(`Error compiling data styles: ${error}`);
   }
}

/**
 * Data styles named by `--! styles <file>` lines. A file that cannot be read
 * is logged and contributes nothing, so the author sees no error.
 */
async function dataStylesForFile(
   url: string,
   text: string,
   urlReader: URLReader,
): Promise<DataStyles> {
   const PREFIX = "--! styles ";
   let styles: DataStyles = {};
   for (const line of text.split("\n")) {
      if (line.startsWith(PREFIX)) {
         const fileName = line.trimEnd().substring(PREFIX.length);
         let stylesText;
         try {
            stylesText = (await urlReader.readURL(
               new URL(fileName, url),
            )) as string;
         } catch (error) {
            logger.error(`Error loading data style '${fileName}': ${error}`);
            stylesText = "{}";
         }
         styles = { ...styles, ...compileDataStyles(stylesText) };
      }
   }

   return styles;
}

// Malloy has no model-metadata mechanism for data styles, so this wraps the
// `URLReader` to accumulate the styles of every file it reads.
export class HackyDataStylesAccumulator implements URLReader {
   private urlReader: URLReader;
   private dataStyles: DataStyles = {};

   constructor(urlReader: URLReader) {
      this.urlReader = urlReader;
   }

   async readURL(url: URL): Promise<string> {
      const contents = await this.urlReader.readURL(url);
      this.dataStyles = {
         ...this.dataStyles,
         ...(await dataStylesForFile(
            url.toString(),
            contents as string,
            this.urlReader,
         )),
      };

      return contents as string;
   }

   getHackyAccumulatedDataStyles(): DataStyles {
      return this.dataStyles;
   }
}
