// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box } from "@mui/material";
import { useMemo, type ReactNode } from "react";
import { makeGetFieldCode } from "./fieldCode";
import type { EntitiesByField } from "./fieldEntities";
import { FieldGroup } from "./FieldGroup";
import type { JoinLink } from "./joinAccess";
import { ModelSection } from "./ModelSection";
import { fieldDeclarations } from "./modelTextDeclarations";
import { useParsedModelText } from "./modelTextParse";
import { SchemaJoin } from "./SchemaJoin";
import type { ExplorerSource } from "./schemaModel";
import {
   SectionOpenCommandContext,
   useSectionExpand,
} from "./sectionOpenCommand";
import type { SourceAccess } from "./sourceAccess";
import { useModelViewerExtensions } from "./extensions";
import type { RunContext } from "./useRunQuery";
import { ViewRow } from "./ViewRow";

interface SchemaViewProps {
   source: ExplorerSource;
   modelPath: string;
   /** A host's entities for the source (per-field code and provenance), when
    *  it passes any. Undefined until loaded, and when there are none. */
   entitiesByName?: EntitiesByField;
   entitiesLoading?: boolean;
   entitiesError?: boolean;
   /** The model file's text, for the code of a field the entities don't carry
    *  (all of them, without a host's entities): see `fieldDeclarations`. */
   modelText?: string;
   /** Join name → whether the joined source has a page, and where. */
   joinLinks?: Map<string, JoinLink>;
   /** Join name → the JOINED source's own fields, so a joined source offers
    *  the same per-field definitions as it would on its own page. Keyed by
    *  join alias because that is what the rendered join is named. */
   joinEntities?: Map<string, EntitiesByField>;
   /** Join name → its declaration, verbatim from the host's entities. */
   joinCodes?: Map<string, string>;
   /** Join name → the joined source's access rules, where it declares any. */
   joinAccess?: ReadonlyMap<
      string,
      { sourceName: string; access: SourceAccess }
   >;
   /** Whether the reader can change the model (an editor sees the join's
    *  gate warning). */
   showRules?: boolean;
   /** A host's descriptions of the base table's columns (column → text), for
    *  the dimensions this source inherits rather than defines. */
   columnDescriptions?: ReadonlyMap<string, string>;
   /** Where to run this source's views against the served model. */
   run?: RunContext;
   /** The model's named queries, as their own section after the views: the two
    *  are the page's results, so they read together before the fields. */
   queries?: ReactNode;
   /** The givens this source reads, as the page's first section: they set every
    *  run below them (ModelGivensSection). */
   givens?: ReactNode;
}

/**
 * Single-source browse view, as always-open sections: the givens it reads, the
 * views, the model's queries, the measures, calculations and dimensions, each a
 * row that opens in place, then each join as an expandable row.
 */
export function SchemaView({
   source,
   modelPath,
   entitiesByName,
   entitiesLoading,
   entitiesError,
   modelText,
   joinLinks,
   joinEntities,
   joinCodes,
   joinAccess,
   showRules = false,
   columnDescriptions,
   run,
   queries,
   givens,
}: SchemaViewProps) {
   const rowProps = { run, modelPath, sourceName: source.name };
   // The views a host lists, where it narrows them (`showField`); the field
   // sections narrow their own.
   const { showField } = useModelViewerExtensions();
   const views = useMemo(
      () =>
         showField
            ? source.views.filter((field) =>
                 showField({
                    modelPath,
                    sourceName: source.name,
                    field,
                    joined: false,
                 }),
              )
            : source.views,
      [showField, source, modelPath],
   );
   // Every section takes the same Expand. A view opens on its result, so Expand
   // runs every view in the section; a join opens on its fields.
   const viewsExpand = useSectionExpand({ noun: ["view", "views"] });
   const joinsExpand = useSectionExpand({ noun: ["join", "joins"] });

   // The file is parsed only for a row the entities left without code (every
   // row, without a host's entities), so a page whose entities cover it all
   // doesn't load the parser.
   const needsText = useMemo(
      () =>
         !entitiesLoading &&
         [
            ...source.views,
            ...source.measures,
            ...source.calculations,
            ...source.dimensions,
         ].some((f) => !entitiesByName?.get(f.name)),
      [entitiesLoading, entitiesByName, source],
   );
   const { parsed, pending } = useParsedModelText(modelText, needsText);
   // Read once per source and text, not per row.
   const declarations = useMemo(
      () => fieldDeclarations(parsed, source.name),
      [parsed, source.name],
   );
   const getFieldCode = useMemo(
      () =>
         makeGetFieldCode({
            entities: entitiesByName,
            loading: !!entitiesLoading,
            error: !!entitiesError,
            columnDescriptions,
            declarations,
            declarationsLoading: pending,
         }),
      [
         entitiesByName,
         entitiesLoading,
         entitiesError,
         columnDescriptions,
         declarations,
         pending,
      ],
   );

   return (
      <Box sx={{ paddingBottom: "24px" }}>
         {givens}
         {/* Most-answered-first: a view is a question the source already
             answers, a measure is one it can compute, a dimension is the raw
             material, and a join is a neighbouring source — so the reader meets
             the source's own vocabulary before its columns. */}
         <ModelSection
            label="Views"
            count={views.length}
            action={viewsExpand.action}
         >
            <SectionOpenCommandContext.Provider value={viewsExpand.command}>
               {views.map((view) => (
                  <ViewRow
                     key={view.name}
                     field={view}
                     modelPath={modelPath}
                     sourceName={source.name}
                     fieldCode={getFieldCode(view)}
                     run={run}
                  />
               ))}
            </SectionOpenCommandContext.Provider>
         </ModelSection>
         {queries}
         <FieldGroup
            label="Measures"
            noun={["measure", "measures"]}
            items={source.measures}
            getFieldCode={getFieldCode}
            {...rowProps}
         />
         <FieldGroup
            label="Calculations"
            noun={["calculation", "calculations"]}
            items={source.calculations}
            getFieldCode={getFieldCode}
            {...rowProps}
         />
         <FieldGroup
            label="Dimensions"
            noun={["dimension", "dimensions"]}
            items={source.dimensions}
            getFieldCode={getFieldCode}
            {...rowProps}
         />
         <ModelSection
            label="Joins"
            count={source.joins.length}
            action={joinsExpand.action}
         >
            <SectionOpenCommandContext.Provider value={joinsExpand.command}>
               {source.joins.map((join) => (
                  <SchemaJoin
                     key={join.name}
                     join={join}
                     run={run}
                     modelPath={modelPath}
                     sourceName={source.name}
                     link={joinLinks?.get(join.name)}
                     entitiesByName={joinEntities?.get(join.name)}
                     code={joinCodes?.get(join.name)}
                     target={joinAccess?.get(join.name)}
                     showRules={showRules}
                     entitiesLoading={entitiesLoading}
                     entitiesError={entitiesError}
                  />
               ))}
            </SectionOpenCommandContext.Provider>
         </ModelSection>
      </Box>
   );
}
