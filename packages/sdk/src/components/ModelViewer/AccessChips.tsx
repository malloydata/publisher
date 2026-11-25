// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Tooltip } from "@mui/material";
import { StatusPill } from "./StatusPill";
import { Fact } from "./Facts";
import { HeaderChip } from "./headerCard";
import {
   isRestricted,
   isRowFiltered,
   restrictedToWords,
   RESTRICTED_MEANING,
   ROW_FILTERED_MEANING,
   type SourceAccess,
} from "./sourceAccess";

/**
 * A source's access declarations as page-header chips, on the status row under
 * its name: "Restricted" for an `#(authorize)` rule, "Row filtered" for an
 * `#(access_filter)`. Declarations-layer state (ux-design-guide §4 "Reference
 * surfaces"), so neutral: the chip says what the source declares, never whether
 * this reader passes — only the publisher knows that. Each is a tab stop whose
 * accessible name is its tooltip (§7); in the source header each opens the
 * card on who can query.
 */
export function AccessChips({ access }: { access: SourceAccess | undefined }) {
   return (
      <>
         {isRestricted(access) && (
            <AccessChip label="Restricted" meaning={RESTRICTED_MEANING} />
         )}
         {isRowFiltered(access) && (
            <AccessChip label="Row filtered" meaning={ROW_FILTERED_MEANING} />
         )}
      </>
   );
}

function AccessChip({ label, meaning }: { label: string; meaning: string }) {
   // In the source header it opens the card on who can query (HeaderChip).
   return (
      <HeaderChip
         layer="access"
         title={`${label}: ${meaning}`}
         testId={`source-access-chip-${label === "Restricted" ? "restricted" : "row-filtered"}`}
      >
         <StatusPill tone="neutral" size="header">
            {label}
         </StatusPill>
      </HeaderChip>
   );
}

/**
 * The same declarations as row pills — a source in the picker, a join to a
 * gated source — in the Declarations layer's token voice. Not focusable: a row
 * is one button, and its accessible name carries the same words
 * ({@link accessRowLabel}).
 */
export function AccessPills({ access }: { access: SourceAccess | undefined }) {
   return (
      <>
         {isRestricted(access) && (
            <Tooltip title={`Restricted: ${RESTRICTED_MEANING}`}>
               <StatusPill
                  voice="token"
                  tone="neutral"
                  sx={{ alignSelf: "center" }}
               >
                  Restricted
               </StatusPill>
            </Tooltip>
         )}
         {isRowFiltered(access) && (
            <Tooltip title={`Row filtered: ${ROW_FILTERED_MEANING}`}>
               <StatusPill
                  voice="token"
                  tone="neutral"
                  sx={{ alignSelf: "center" }}
               >
                  Row filtered
               </StatusPill>
            </Tooltip>
         )}
      </>
   );
}

/** The words a row's accessible name adds for its access pills ("", ", restricted"). */
export function accessRowLabel(access: SourceAccess | undefined): string {
   return [
      isRestricted(access) && "restricted",
      isRowFiltered(access) && "row filtered",
   ]
      .filter(Boolean)
      .map((w) => `, ${w}`)
      .join("");
}

/**
 * The access facts in the source's card, as rows of the facts grid the
 * binding's facts use. Readers get sentences; an editor (who can change the
 * model) also gets the Malloy rule and filter verbatim (§4a.2: expressions are
 * writer vocabulary).
 */
export function AccessFacts({
   access,
   showRules,
}: {
   access: SourceAccess;
   showRules: boolean;
}) {
   const restricted = isRestricted(access);
   const filtered = isRowFiltered(access);
   if (!restricted && !filtered) return null;
   return (
      <Box
         data-testid="source-access-facts"
         sx={{
            display: "grid",
            gridTemplateColumns: "max-content 1fr",
            columnGap: "16px",
            rowGap: "6px",
            alignItems: "baseline",
         }}
      >
         {restricted && (
            <Fact label="Restricted to" value={restrictedToWords(access)} />
         )}
         {/* One row per rule: several all apply, and joining them into one line
             would read as a single expression. */}
         {restricted &&
            showRules &&
            access.authorize
               .filter((e) => e.trim() !== "true")
               .map((e, i) => (
                  <Fact key={`a${i}`} label="Access rule" value={e} mono />
               ))}
         {filtered && (
            <Fact label="Rows" value="Rows are filtered for each reader" />
         )}
         {filtered &&
            showRules &&
            access.accessFilter.map((e, i) => (
               <Fact key={`f${i}`} label="Row filter" value={e} mono />
            ))}
      </Box>
   );
}
