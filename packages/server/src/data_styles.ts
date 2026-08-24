// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { URLReader } from "@malloydata/malloy";
import { DataStyles } from "@malloydata/render";
import { logger } from "./logger";

export function compileDataStyles(styles: string): DataStyles {
   try {
      return JSON.parse(styles) as DataStyles;
   } catch (error) {
      throw new Error(`Error compiling data styles: ${error}`);
   }
}

// TODO replace this with actual JSON metadata import functionality, when it exists
export async function dataStylesForFile(
   url: string,
   text: string,
   urlReader: URLReader,
): Promise<DataStyles> {
   const PREFIX = "--! styles ";
   let styles: DataStyles = {};
   for (const line of text.split("\n")) {
      if (line.startsWith(PREFIX)) {
         const fileName = line.trimEnd().substring(PREFIX.length);
         // TODO instead of failing silently when the file does not exist, perform this after the WebView has been
         //      created, so that the error can be shown there.
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

// TODO Come up with a better way to handle data styles. Perhaps this is
//      an in-language understanding of model "metadata". For now,
//      we abuse the `URLReader` API to keep track of requested URLs
//      and accumulate data styles for those files.
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
