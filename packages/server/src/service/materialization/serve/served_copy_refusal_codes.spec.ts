// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every reason the served copy leaves a source (or its whole copy) unserved,
 * pinned with the package warning each one becomes: its code, severity,
 * subject and message. The list is total over `ServedCopyProblem["reason"]`,
 * so a reason added or removed fails to compile here, and the bind label set
 * that counts these reasons is read off the same list. The reasons a re-root
 * itself returns are pinned beside them, with the one that is silent by
 * design (`not_query_shaped`: a declaration that is not a query is left as
 * written and reported nowhere).
 */

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import {
   PackageMaterialization,
   type PackageMaterializationOwner,
} from "../package_materialization";
import type { ServedCopyProblem } from "./served_copy_serve";
import { SERVED_COPY_WARNING_CODE } from "./served_copy_serve";

type Reason = ServedCopyProblem["reason"];

/** One problem per reason, as the served copy reports it. */
const PROBLEMS = {
   compile_failed: { reason: "compile_failed", detail: "line 1: boom" },
   cross_connection_unbuilt: { reason: "cross_connection_unbuilt" },
   column_types: { reason: "column_types", sourceName: "s" },
   no_declaration: { reason: "no_declaration", sourceName: "s" },
   range_not_found: { reason: "range_not_found", sourceName: "s" },
   definition_not_parsed: { reason: "definition_not_parsed", sourceName: "s" },
   pair_join_not_found: { reason: "pair_join_not_found", sourceName: "s" },
   pair_not_compiled: { reason: "pair_not_compiled", sourceName: "s" },
   fields_not_carried: { reason: "fields_not_carried", sourceName: "s" },
   read_terms_not_kept: { reason: "read_terms_not_kept", sourceName: "s" },
   cross_connection_join: {
      reason: "cross_connection_join",
      sourceName: "s",
      join: "j",
   },
   inline_join_kept: {
      reason: "inline_join_kept",
      sourceName: "s",
      join: "j",
      detail: "line 3: j.k is not defined",
   },
   reroot_not_compiled: { reason: "reroot_not_compiled", sourceName: "s" },
   classification_not_kept: {
      reason: "classification_not_kept",
      sourceName: "s",
      leaks: ["`s` carries [pii] but exposes `j.email`."],
      coverage: ["`j.email` is not classified."],
   },
} satisfies { [R in Reason]: ServedCopyProblem & { reason: R } };

const WARNINGS: Record<
   Reason,
   { code: string | undefined; subject: string | null; message: string }[]
> = {
   compile_failed: [
      {
         code: "served-copy-not-served",
         subject: null,
         message:
            "This model's served copy does not compile, so its stored sources answer live.",
      },
   ],
   cross_connection_unbuilt: [
      {
         code: "served-copy-not-served",
         subject: null,
         message:
            "This model's served copy re-roots a source onto its storage destination, which moves a join of it across connections, and the join cannot be left out; the copy compiles once both sides are built into one destination, and serves live until then.",
      },
   ],
   column_types: [
      {
         code: "served-copy-not-served",
         subject: "s",
         message:
            "This source's stored column types differ from the types the model declares, or one cannot be declared on its table, so the served copy leaves it as written and it answers live.",
      },
   ],
   no_declaration: [
      {
         code: "served-copy-not-served",
         subject: "s",
         message:
            "This source's declaration was not found in the compiled model, so the served copy leaves it as written and it answers live.",
      },
   ],
   range_not_found: [
      {
         code: "served-copy-not-served",
         subject: "s",
         message:
            "This source's declaration could not be re-rooted on its table, so the served copy leaves it as written and it answers live.",
      },
   ],
   definition_not_parsed: [
      {
         code: "served-copy-not-served",
         subject: "s",
         message:
            "This source's declaration could not be re-rooted on its table, so the served copy leaves it as written and it answers live.",
      },
   ],
   pair_join_not_found: [
      {
         code: "served-copy-not-served",
         subject: "s",
         message:
            "This source inlines a join_many that is not declared on its own in its own trailing `extend` (a source it is built from declares it, or its `join_many:` statement declares other joins too), so its pair table cannot be read with the join as a record and the served copy leaves it as written.",
      },
   ],
   pair_not_compiled: [
      {
         code: "served-copy-not-served",
         subject: "s",
         message:
            "This source's pair table (an inlined join_many) could not be read as the source with its join as a record (its text reads a measure or view of the joined source through the join, which the record does not carry), so the served copy leaves it as written.",
      },
   ],
   fields_not_carried: [
      {
         code: "served-copy-not-served",
         subject: "s",
         message:
            "This source inherits fields (a measure, view or join declared on the source it is built from) that its stored table does not carry, so the served copy leaves it as written.",
      },
   ],
   read_terms_not_kept: [
      {
         code: "served-copy-not-served",
         subject: "s",
         message:
            "This source filters by a given in a place its stored table cannot keep, or is filtered and reached here only through a join, so the served copy leaves it as written rather than answer every caller alike.",
      },
   ],
   cross_connection_join: [
      {
         code: "served-copy-not-served",
         subject: "s",
         message:
            "This source is stored in a destination its join `j` cannot reach: the joined source reads another connection. Queries that use the join are answered live; the others read the stored table. To serve them too, inline the join (inline=), materialize the joined source into the same destination, or keep this source database.",
      },
   ],
   inline_join_kept: [
      {
         code: "served-copy-not-served",
         subject: "s",
         message:
            "This source inlines its join `j` (inline=), but its served copy does not compile with the join answered from the stored columns, so the join is kept as written and every query through it reads the joined source through the join: j.k is not defined. A definition that reads the joined source past one hop (a join of the joined source), or reads one of its measures or views, is the usual cause.",
      },
   ],
   reroot_not_compiled: [
      {
         code: "served-copy-not-served",
         subject: "s",
         message:
            "This source's re-root on its stored table does not compile in the served copy (the server log has the compiler's message). The served copy leaves it as written, so it answers live, and the model's other stored sources still serve. This is a Publisher bug; please report it.",
      },
   ],
   classification_not_kept: [
      {
         code: "served-copy-not-served",
         subject: "s",
         message:
            "This source's re-root changed what it exposes or how a field is classified, which the load's classification checks flag on the served copy, so its queries are not answered from the stored tables (a Publisher bug; please report it): `s` carries [pii] but exposes `j.email`. `j.email` is not classified.",
      },
      {
         code: "classification-coverage",
         subject: "s",
         message: "`j.email` is not classified.",
      },
   ],
};

function warningsFor(problems: ServedCopyProblem[]) {
   const model = {
      servedCopyProblems: () => problems,
   };
   const materialization = new PackageMaterialization({
      models: () => new Map([["m.malloy", model]]),
      packageName: () => "pkg",
   } as unknown as PackageMaterializationOwner);
   return materialization.servedCopyWarnings();
}

describe("the served copy's refusal codes", () => {
   it("has fourteen reasons", () => {
      expect(Object.keys(PROBLEMS)).toEqual([
         "compile_failed",
         "cross_connection_unbuilt",
         "column_types",
         "no_declaration",
         "range_not_found",
         "definition_not_parsed",
         "pair_join_not_found",
         "pair_not_compiled",
         "fields_not_carried",
         "read_terms_not_kept",
         "cross_connection_join",
         "inline_join_kept",
         "reroot_not_compiled",
         "classification_not_kept",
      ]);
   });

   it("warns for each reason, in its own words", () => {
      const warnings = Object.fromEntries(
         Object.entries(PROBLEMS).map(([reason, problem]) => [
            reason,
            warningsFor([problem]).map((w) => ({
               code: w.code,
               subject: w.subject ?? null,
               message: w.message,
            })),
         ]),
      );
      if (process.env.REFUSAL_CODES_DUMP) {
         fs.writeFileSync(
            process.env.REFUSAL_CODES_DUMP,
            JSON.stringify(warnings, null, 3),
         );
         return;
      }
      expect(warnings).toEqual(WARNINGS);
   });

   it("codes every reason's warning the same", () => {
      for (const problem of Object.values(PROBLEMS)) {
         for (const w of warningsFor([problem])) {
            if (w.message?.includes("is not classified")) continue;
            expect(w.code).toBe(SERVED_COPY_WARNING_CODE);
            expect(w.severity).toBe("warn");
         }
      }
   });

   it("words a compile failure that names a source as that source's kept blocks", () => {
      // An extension table that does not compile over its kept blocks: the
      // copy is compiled again with its own blocks only.
      expect(
         warningsFor([
            {
               reason: "compile_failed",
               sourceName: "s",
               detail: "line 1: boom",
            },
         ]).map((w) => ({
            code: w.code,
            subject: w.subject ?? null,
            message: w.message,
         })),
      ).toEqual([
         {
            code: "served-copy-not-served",
            subject: "s",
            message:
               "This source's table does not compile with its kept blocks, so the served copy re-roots it with its own blocks only, or leaves it as written.",
         },
      ]);
   });

   it("advises inlining a cross-connection join only when it is not inlined and kept", () => {
      const both = warningsFor([
         PROBLEMS.inline_join_kept,
         PROBLEMS.cross_connection_join,
      ]);
      expect(both.map((w) => w.message)).toEqual([
         WARNINGS.inline_join_kept[0].message,
         WARNINGS.cross_connection_join[0].message.replace(
            "inline the join (inline=), materialize the joined source into the same destination, or keep this source database.",
            "fix what keeps the inlined join from being answered from its stored columns (the warning on this join says what), materialize the joined source into the same destination, or keep this source database.",
         ),
      ]);
   });
});
