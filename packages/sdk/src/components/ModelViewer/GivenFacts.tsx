// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Tooltip } from "@mui/material";
import { SANS_FONT_FAMILY, fontSizes } from "./theme";
import { StatusPill } from "./StatusPill";
import { PANE_CARD_SX, PANE_INSET } from "./surface";
import { renderGivenDefault } from "../given/utils";
import type { GivenInfo } from "./modelGivens";
import { Fact } from "./Facts";

/** What a "Given" pill's tooltip says: which givens the row reads. */
export function givenPillMeaning(givens: ReadonlyArray<GivenInfo>): string {
   const names = givens.map((g) => g.name).join(", ");
   return `Reads the ${givens.length === 1 ? "given" : "givens"} ${names}, set in the Givens section above`;
}

/**
 * The Declarations-layer pill on a field, view or query whose definition reads a
 * given (`$NAME`): its result depends on a value the reader sets. Not a tab stop
 * of its own — it sits inside the row's button, whose accessible name carries
 * the word, and what it reads is in the row's opened card.
 */
export function GivenPill({ givens }: { givens: ReadonlyArray<GivenInfo> }) {
   if (givens.length === 0) return null;
   return (
      <Tooltip title={givenPillMeaning(givens)}>
         {/* A descriptor of the field, like its type: same voice and tone. */}
         <StatusPill
            voice="token"
            tone="quiet"
            sx={{ alignSelf: "center", flexShrink: 0 }}
         >
            given
         </StatusPill>
      </Tooltip>
   );
}

/** A given's default as a reader reads it, or that it has none. */
function defaultWords(g: GivenInfo): string {
   const rendered = renderGivenDefault(g.type, g.default);
   if (rendered === undefined) return "none — a value is required";
   return rendered === "" ? "(empty)" : rendered;
}

/**
 * The givens a row reads, in the card an open row's content takes: each one's
 * name, type, default, and the label and description its declaration gives it.
 */
export function GivenFacts({
   givens,
   framed = true,
}: {
   givens: ReadonlyArray<GivenInfo>;
   /** False: one section of a card someone else frames (an open field row's). */
   framed?: boolean;
}) {
   if (givens.length === 0) return null;
   return (
      <Box
         data-testid="model-viewer-given-card"
         sx={{
            ...(framed ? PANE_CARD_SX : {}),
            p: PANE_INSET,
            display: "grid",
            gridTemplateColumns: "max-content 1fr",
            columnGap: "16px",
            rowGap: "6px",
            alignItems: "baseline",
            fontFamily: SANS_FONT_FAMILY,
            fontSize: fontSizes.small,
            color: "text.primary",
         }}
      >
         {givens.map((g) => (
            <Box key={g.name} sx={{ display: "contents" }}>
               <Fact label="Given" value={g.name} mono />
               {g.label && <Fact label="Label" value={g.label} />}
               <Fact label="Type" value={g.type} mono />
               <Fact label="Default" value={defaultWords(g)} />
               {g.description && (
                  <Fact label="Description" value={g.description} />
               )}
            </Box>
         ))}
      </Box>
   );
}
