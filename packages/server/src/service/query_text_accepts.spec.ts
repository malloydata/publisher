// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What the caller-text reader makes of each text in the lexer corpus, pinned:
 * a change to the one lexer core must leave every reader of untrusted query
 * text accepting and rejecting exactly what it did. Regenerate only for an
 * intended change, with UPDATE_GOLDENS=1.
 */

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { LEXER_CORPUS } from "./lexer_corpus";
import {
   buildDerivationBaseMap,
   buildIsEdgeMap,
   buildJoinBaseMap,
   buildSourceAliasMap,
   collectIdentifierNames,
   extractRunTargetSourceNames,
   malloyIdentifiers,
   scanIdentifiers,
   stripMalloyCommentsAndLiterals,
} from "./query_text";

const GOLDEN = path.join(__dirname, "query_text_accepts.golden.json");

const sorted = <T>(xs: Iterable<T>) => [...xs].map(String).sort();
const mapOf = (m: Map<string, unknown>) =>
   Object.fromEntries(
      [...m]
         .map(([k, v]) => [k, v instanceof Set ? sorted(v) : v])
         .sort((a, b) => String(a[0]).localeCompare(String(b[0]))),
   );

function readings(text: string): Record<string, unknown> {
   const run = `${text}\nrun: s -> { select: * }`;
   return {
      stripped: stripMalloyCommentsAndLiterals(text),
      identifiers: malloyIdentifiers(text),
      scanned: sorted(scanIdentifiers(text)),
      names: sorted(collectIdentifierNames(text)),
      runTargets: extractRunTargetSourceNames(run),
      aliases: mapOf(buildSourceAliasMap(text)),
      derivations: mapOf(buildDerivationBaseMap(text)),
      joins: mapOf(buildJoinBaseMap(text)),
      isEdges: mapOf(buildIsEdgeMap(text)),
   };
}

describe("the caller-text reader over the lexer corpus", () => {
   it("reads every text exactly as before", () => {
      const actual = Object.fromEntries(
         Object.entries(LEXER_CORPUS).map(([k, t]) => [k, readings(t)]),
      );
      const text = `${JSON.stringify(actual, null, 2)}\n`;
      if (process.env.UPDATE_GOLDENS === "1") fs.writeFileSync(GOLDEN, text);
      expect(JSON.parse(text)).toEqual(
         JSON.parse(fs.readFileSync(GOLDEN, "utf8")),
      );
   });
});
