// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Button } from "@mui/material";
import { useMemo, useState } from "react";
import { useModelViewerExtensions } from "./extensions";
import type { FieldCodeLookup } from "./fieldCode";
import {
   expandableFirst,
   hasDefinitionToShow,
   isIndexedDimension,
} from "./fieldExpansion";
import { FieldRow } from "./FieldRow";
import { ModelSection } from "./ModelSection";
import type { ExplorerField } from "./schemaModel";
import {
   SectionOpenCommandContext,
   useSectionExpand,
} from "./sectionOpenCommand";
import type { RunContext } from "./useRunQuery";

interface FieldGroupProps {
   /** "Measures", "Calculations" or "Dimensions". */
   label: string;
   /** What the count counts, singular and plural. */
   noun: readonly [string, string];
   items: ExplorerField[];
   /** Where a dimension's sample values are read. */
   run?: RunContext;
   modelPath: string;
   sourceName: string;
   /** Per-field Malloy-definition lookup; absent when this group's rows have
    *  no code to show. */
   getFieldCode?: (field: ExplorerField) => FieldCodeLookup | undefined;
   /** A page section (h2), or a joined source's subsection (h3). */
   level?: "h2" | "h3";
}

/**
 * How many rows a section draws before a "Show all" for the rest: a source can
 * pass thousands of table columns through, and drawing every row up front held
 * the page for seconds before its heading appeared.
 */
export const FIELD_ROW_CAP = 200;

/**
 * One field kind (a source's measures, calculations or dimensions) as an always-open section
 * of compact rows: the first {@link FIELD_ROW_CAP}, then the rest on a click.
 */
export function FieldGroup({
   label,
   noun,
   items,
   run,
   modelPath,
   sourceName,
   getFieldCode,
   level,
}: FieldGroupProps) {
   // The rows a host lists, where it narrows them (`showField`).
   const { showField } = useModelViewerExtensions();
   const listed = useMemo(
      () =>
         showField
            ? items.filter((field) =>
                 showField({
                    modelPath,
                    sourceName,
                    field,
                    joined: level === "h3",
                 }),
              )
            : items,
      [items, showField, modelPath, sourceName, level],
   );
   const [showAll, setShowAll] = useState(false);
   // Expand / Collapse: one click opens every row, for a reader checking each
   // definition. Offered whenever a row here has something to open, however many
   // rows there are: the rule a row uses to open (FieldRow), a field with code or
   // a dimension whose values it lists.
   const anyOpens = listed.some(
      (field) =>
         (isIndexedDimension(field) && level !== "h3") ||
         hasDefinitionToShow(getFieldCode?.(field)),
   );
   const { command, action: expand } = useSectionExpand({
      noun,
      disabledReason: anyOpens
         ? undefined
         : `None of these ${noun[1]} has anything to show`,
   });
   const action = level !== "h3" ? expand : undefined;
   // Rows that open to something lead the section; see expandableFirst.
   const ordered = useMemo(
      () => (getFieldCode ? expandableFirst(listed, getFieldCode) : listed),
      [listed, getFieldCode],
   );
   const shown =
      showAll || ordered.length <= FIELD_ROW_CAP
         ? ordered
         : ordered.slice(0, FIELD_ROW_CAP);

   return (
      <ModelSection
         label={label}
         count={listed.length}
         level={level}
         action={action}
      >
         <SectionOpenCommandContext.Provider value={command}>
            <Box sx={{ display: "flex", flexDirection: "column", gap: "3px" }}>
               {shown.map((field) => (
                  <FieldRow
                     key={field.name}
                     field={field}
                     run={run}
                     modelPath={modelPath}
                     sourceName={sourceName}
                     fieldCode={getFieldCode?.(field)}
                     // A joined source's field: its values belong to that source.
                     joined={level === "h3"}
                  />
               ))}
               {shown.length < ordered.length && (
                  <Button
                     size="small"
                     onClick={() => setShowAll(true)}
                     sx={{ alignSelf: "flex-start", textTransform: "none" }}
                  >
                     Show all {ordered.length.toLocaleString()} {noun[1]}
                  </Button>
               )}
            </Box>
         </SectionOpenCommandContext.Provider>
      </ModelSection>
   );
}
