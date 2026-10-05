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
import { motlyParseErrors, motlyTag } from "./motly";

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

/** Whether the tag parser reads `note` as setting a top-level `artifact`; a note that does not parse counts when it opens with `artifact`, so its error is reported rather than the file read as untagged. */
const setsArtifact = (note: string) =>
   motlyParseErrors([note]).length > 0
      ? /^##(?:\|\s*|[ \t]*)artifact\b/.test(note)
      : Boolean(motlyTag([note])?.tag("artifact"));

/** The tag as Malloy reads it: the first `##` note, or `##|` block that closes, that sets `artifact`. */
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
      if (kind === "DOC_ANNOTATION" && setsArtifact(text(i)))
         return lines(text(i));
      if (kind !== "DOC_BLOCK_ANNOTATION_BEGIN") continue;
      const body: string[] = [];
      let j = i;
      while (name(tokens[j].type) !== "BLOCK_ANNOTATION_END") {
         body.push(text(j));
         if (++j >= tokens.length || name(tokens[j].type) === "EOF")
            return null;
      }
      if (setsArtifact(body.join(""))) return lines(body.join(""));
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
