// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Collapse, Link } from "@mui/material";
import { useMemo } from "react";
import { AccessPills } from "./AccessChips";
import { makeGetFieldCode } from "./fieldCode";
import type { EntitiesByField } from "./fieldEntities";
import { FieldGroup } from "./FieldGroup";
import { RowLine } from "./FieldRow";
import { IconOpen } from "./icons";
import type { JoinLink } from "./joinAccess";
import { RowGlyph } from "./RowGlyph";
import { RowLayerSwitch, useRowLayer, type RowLayer } from "./rowLayers";
import type { ExplorerJoin } from "./schemaModel";
import { useSectionOpen } from "./sectionOpenCommand";
import { joinGateWarning, type SourceAccess } from "./sourceAccess";
import { StatusPill } from "./StatusPill";
import { PANE_CARD_SX } from "./surface";
import { CaptionText } from "./text";
import { fontSizes } from "./theme";
import { ink } from "./tokens";
import { QueryCode } from "./ui";
import type { RunContext } from "./useRunQuery";

/** An open join's two layers: the joined source's fields, or the join's Malloy. */
const JOIN_LAYERS: RowLayer[] = ["fields", "code"];

/** The trailing "open the joined source" arrow, link or button alike. */
const OPEN_ARROW_SX = {
   display: "inline-flex",
   alignItems: "center",
   color: "text.secondary",
   borderRadius: 1,
   padding: "2px",
   flexShrink: 0,
   "&:hover": { color: ink },
} as const;

interface SchemaJoinProps {
   join: ExplorerJoin;
   run?: RunContext;
   modelPath: string;
   sourceName: string;
   /** Whether the joined SOURCE has a page of its own, and where. A package
    *  can serve a source only as a join partner (see joinAccess.ts), and the
    *  row must not offer a door that isn't there — those say so in words
    *  instead. Absent, or `unknown`, → neither arrow nor note: not yet
    *  established is not the same as established absent. */
   link?: JoinLink;
   /** The JOINED source's own fields. A joined source is the same source seen
    *  from another page, so its fields expand to definitions exactly as they
    *  do there. Absent (e.g. the target's model hasn't loaded) → the rows
    *  render with nothing to open rather than claiming "no
    *  definition". */
   entitiesByName?: EntitiesByField;
   entitiesLoading?: boolean;
   entitiesError?: boolean;
   /** The join's declaration, verbatim from a host's entities
    *  ("customer is customers with customer_id"), drawn in the page's code card. */
   code?: string;
   /** The joined source, where it declares an access rule: its Restricted or
    *  Row filtered pill rides on this row. */
   target?: { sourceName: string; access: SourceAccess };
   /** Whether the reader can change the model, so an open join to a gated
    *  source warns that its gate does not apply here. */
   showRules?: boolean;
}

/**
 * A join as a row of the page, opened and closed as every other row is (RowLine):
 * its name, its field count, its doc, and at the right its relationship and the
 * arrow to the joined source's own page. Open, it shows the joined source's
 * fields — the same sections and rows the source's own use, one level in, framed
 * as one block — with Fields | Code on its line, as an open view's Result | Code,
 * and its declaration in the page's one code card (QueryCode) one click over.
 *
 * Opening is the default gesture (the join is part of this source). Navigating
 * away to the joined source's own page is the deliberate one, so it gets a
 * trailing arrow rather than being armed on the name.
 */
export function SchemaJoin({
   join,
   run,
   modelPath,
   sourceName,
   link,
   entitiesByName,
   entitiesLoading,
   entitiesError,
   code,
   target,
   showRules = false,
}: SchemaJoinProps) {
   // Open or shut by the reader, and by its section's Expand / Collapse.
   const [open, setOpen] = useSectionOpen();
   const { layers, layer: view, setLayer: setView } = useRowLayer(JOIN_LAYERS);

   const getFieldCode = useMemo(
      () =>
         entitiesByName
            ? makeGetFieldCode({
                 entities: entitiesByName,
                 loading: !!entitiesLoading,
                 error: !!entitiesError,
              })
            : undefined,
      [entitiesByName, entitiesLoading, entitiesError],
   );

   const fieldCount = join.dimensions.length + join.measures.length;

   const trailing = (
      <Box sx={{ display: "inline-flex", alignItems: "center", gap: 1 }}>
         {open && (
            // Fields | Code, as an open view's Result | Code: what it opens to first,
            // and its Malloy one click over.
            <RowLayerSwitch
               aria-label={`Show for ${join.name}`}
               layers={layers}
               value={view}
               onChange={setView}
               disabledReasons={{
                  code: code
                     ? undefined
                     : "No Malloy code to show for this join",
               }}
            />
         )}
         {link?.kind === "link" && link.href && (
            <Link
               href={link.href}
               aria-label={`Open ${join.name}`}
               title={`Open ${join.name}`}
               sx={OPEN_ARROW_SX}
            >
               <IconOpen size={14} />
            </Link>
         )}
         {link?.kind === "link" && !link.href && link.onOpen && (
            // The same arrow, as an in-page selection rather than a
            // navigation (see JoinLink.onOpen).
            <Link
               component="button"
               type="button"
               onClick={link.onOpen}
               aria-label={`Open ${join.name}`}
               title={`Open ${join.name}`}
               sx={OPEN_ARROW_SX}
            >
               <IconOpen size={14} />
            </Link>
         )}
         {link?.kind === "join-only" && (
            <Box
               component="span"
               sx={{
                  fontSize: fontSizes.micro,
                  fontStyle: "italic",
                  color: "text.secondary",
                  flexShrink: 0,
               }}
            >
               only accessible via join
            </Box>
         )}
      </Box>
   );

   return (
      <Box
         data-testid="model-viewer-join-row"
         data-name={join.name}
         sx={open ? { pb: 1.25 } : undefined}
      >
         <RowLine
            glyph={<RowGlyph kind="join" />}
            name={join.name}
            typeLabel={`${fieldCount} ${fieldCount === 1 ? "field" : "fields"}`}
            // A field row's layout: what the join is beside its name — its
            // relationship, a quiet token like a field's type — and the joined
            // source's access state on the line under it.
            stackPills
            descriptors={
               <StatusPill
                  tone="quiet"
                  voice="token"
                  sx={{ alignSelf: "center", flexShrink: 0 }}
               >
                  join_{join.relationship}
               </StatusPill>
            }
            doc={join.doc}
            open={open}
            expandable
            onToggle={() => setOpen((o) => !o)}
            trailing={trailing}
            pills={target && <AccessPills access={target.access} />}
         />
         <Collapse in={open} timeout="auto" unmountOnExit>
            {/* Hangs under the join's name, as every row's opening does. */}
            <Box sx={{ mx: "10px", mt: "2px" }}>
               {view === "code" && code ? (
                  <QueryCode testId="model-viewer-join-code" code={code} />
               ) : (
                  // The joined source's fields as a contained block — framed as a
                  // result or a code card is, on the page with no fill — so they read
                  // as this join's contents, not more rows of the page. Measures before
                  // dimensions, as on the source itself (SchemaView).
                  <Box
                     data-testid="model-viewer-join-fields"
                     sx={{
                        ...PANE_CARD_SX,
                        bgcolor: "transparent",
                        p: 1.5,
                        pb: 0,
                     }}
                  >
                     {target && showRules && (
                        // A gate fires only on the source a query enters through, so a
                        // join publishes the joined source's rows under this source's
                        // rules: the footgun an editor needs told. Inside the card, over
                        // the fields it is about.
                        <CaptionText
                           component="div"
                           data-testid="model-viewer-join-gate-warning"
                           sx={{ mb: 1 }}
                        >
                           {joinGateWarning(target.sourceName, target.access)}
                        </CaptionText>
                     )}
                     <FieldGroup
                        label="Measures"
                        noun={["measure", "measures"]}
                        level="h3"
                        items={join.measures}
                        run={run}
                        modelPath={modelPath}
                        sourceName={sourceName}
                        getFieldCode={getFieldCode}
                     />
                     <FieldGroup
                        label="Dimensions"
                        noun={["dimension", "dimensions"]}
                        level="h3"
                        items={join.dimensions}
                        run={run}
                        modelPath={modelPath}
                        sourceName={sourceName}
                        getFieldCode={getFieldCode}
                     />
                  </Box>
               )}
            </Box>
         </Collapse>
      </Box>
   );
}
