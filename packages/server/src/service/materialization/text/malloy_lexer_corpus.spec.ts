// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What the rewriters' lexer makes of each text in the lexer corpus, pinned,
 * so a change to the one lexer core shows exactly which texts the rewriters
 * now read differently. Regenerate only for an intended change, with
 * UPDATE_GOLDENS=1, and say which entries moved and why.
 */

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { LEXER_CORPUS } from "../../lexer_corpus";
import {
   lexAll,
   statementHead,
   statementStarts,
} from "../../malloy_lexer_core";

const GOLDEN = path.join(__dirname, "malloy_lexer_corpus.golden.json");

function reading(text: string): unknown {
   const tokens = lexAll(text);
   if (!tokens) return "unreadable";
   return {
      tokens: tokens.map((t) => `${t.kind}@${t.depth}:${t.text}`),
      statements: statementStarts(tokens).map(
         (k) => `${statementHead(tokens, k).text} ${tokens[k].text}`,
      ),
   };
}

describe("the rewriters' lexer over the lexer corpus", () => {
   it("reads every text as pinned", () => {
      const actual = Object.fromEntries(
         Object.entries(LEXER_CORPUS).map(([k, t]) => [k, reading(t)]),
      );
      const text = `${JSON.stringify(actual, null, 2)}\n`;
      if (process.env.UPDATE_GOLDENS === "1") fs.writeFileSync(GOLDEN, text);
      expect(JSON.parse(text)).toEqual(
         JSON.parse(fs.readFileSync(GOLDEN, "utf8")),
      );
   });
});
