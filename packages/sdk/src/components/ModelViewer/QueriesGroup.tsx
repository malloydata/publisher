// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useMemo } from "react";
import { givensOfNamedQuery } from "./givenUsage";
import { namedQueryRunQuery } from "./malloyRunText";
import { ModelSection } from "./ModelSection";
import {
   SectionOpenCommandContext,
   useSectionExpand,
} from "./sectionOpenCommand";
import { namedQueryCode } from "./namedQueryCode";
import { useParsedModelText } from "./modelTextParse";
import type { ExplorerField, ExplorerQuery } from "./schemaModel";
import type { RunContext } from "./useRunQuery";
import { ViewRow } from "./ViewRow";

/**
 * A model's named queries as a section of rows, drawn and run exactly as a
 * source's views are (ViewRow): opening one runs it, under Result | Code. Its
 * code is the `query:` declaration read from the
 * model file; where the file isn't served, Code stays in place, disabled, saying
 * why.
 */
export function QueriesGroup({
   queries,
   sourceText,
   modelPath,
   run,
}: {
   queries: ReadonlyArray<ExplorerQuery>;
   sourceText?: string;
   modelPath: string;
   run?: RunContext;
}) {
   const { parsed, pending } = useParsedModelText(
      sourceText,
      queries.length > 0,
   );
   const cards = useMemo(
      () =>
         queries.map((query) => {
            const code = namedQueryCode(parsed, query.name);
            return {
               query,
               field: {
                  name: query.name,
                  kind: "view",
                  doc: query.doc,
                  tags: query.tags,
               } satisfies ExplorerField,
               code: { code, loading: pending },
               // With no source to gate on, the givens its own text reads,
               // not every given the model declares.
               runGivens:
                  query.sourceName || !run?.givens
                     ? undefined
                     : givensOfNamedQuery(code, run.givens, run.givensBySource),
            };
         }),
      [queries, parsed, pending, run?.givens, run?.givensBySource],
   );
   // A query opens on its result, so Expand runs every query in the section.
   const expand = useSectionExpand({ noun: ["query", "queries"] });
   return (
      <ModelSection
         label="Queries"
         count={queries.length}
         testId="model-viewer-queries"
         action={expand.action}
      >
         <SectionOpenCommandContext.Provider value={expand.command}>
            {cards.map(({ query, field, code, runGivens }) => (
               <ViewRow
                  key={query.name}
                  field={field}
                  modelPath={modelPath}
                  sourceName={query.sourceName ?? ""}
                  fieldCode={code}
                  run={run}
                  runQuery={namedQueryRunQuery(query.name)}
                  runGivens={runGivens}
               />
            ))}
         </SectionOpenCommandContext.Provider>
      </ModelSection>
   );
}
