// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import {
   artifactTagText,
   translateToParse,
   type TokenStreamShape,
} from "./notebook";

// One case table for this reader and the SDK's `artifactTag`, whose spec reads the same file.
const FIXTURE = path.resolve(
   __dirname,
   "../../../sdk/src/components/DashboardBuilder/testing/artifactTagParity.json",
);
const { cases } = JSON.parse(fs.readFileSync(FIXTURE, "utf8")) as {
   cases: {
      name: string;
      source: string;
      tag: string[] | null;
      server?: string[];
   }[];
};

const lines = (text: string) =>
   text
      .split(/\r\n|\r|\n/)
      .map((l) => l.trim())
      .filter((l, i, all) => i < all.length - 1 || l !== "");

/** The tag as Malloy's lexer reads it: a `##` note, or a `##|` block that closes, spelled `artifact`. */
function lexerTag(source: string): string[] | null {
   const stream = translateToParse(source).parse?.tokenStream as
      | TokenStreamShape
      | undefined;
   const name = (type: number) =>
      stream?.tokenSource?.vocabulary?.getSymbolicName(type);
   const tokens = stream?.getTokens?.() ?? [];
   const text = (i: number) =>
      source.slice(tokens[i].startIndex, tokens[i].stopIndex + 1);
   for (let i = 0; i < tokens.length; i++) {
      const kind = name(tokens[i].type);
      if (kind === "DOC_ANNOTATION" && /^##[ \t]*artifact\b/.test(text(i)))
         return lines(text(i));
      if (kind !== "DOC_BLOCK_ANNOTATION_BEGIN") continue;
      const body: string[] = [];
      let j = i;
      while (name(tokens[j].type) !== "BLOCK_ANNOTATION_END") {
         body.push(text(j));
         if (++j >= tokens.length || name(tokens[j].type) === "EOF")
            return null;
      }
      if (/^##\|\s*artifact\b/.test(text(i))) return lines(body.join(""));
      i = j;
   }
   return null;
}

describe("the artifact tag read off raw text agrees with the SDK and the lexer", () => {
   for (const { name, source, tag, server } of cases)
      it(name, () => {
         expect(lexerTag(source)).toEqual(tag);
         const found = artifactTagText(source);
         expect(found === undefined ? null : lines(found)).toEqual(
            server ?? tag,
         );
      });
});
