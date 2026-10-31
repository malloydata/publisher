// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, CircularProgress } from "@mui/material";
import type { ExplorerField } from "./schemaModel";
import { fontSizes, TEXT_TERTIARY } from "./theme";
import {
   useDimensionSamples,
   type SampleValuesFetcher,
} from "./useDimensionSamples";
import type { RunContext } from "./useRunQuery";
import { ValueChip } from "./ValueChip";

/**
 * A dimension's sample values: its most common values, most common first,
 * fetched lazily (see useDimensionSamples). What the dimension MEANS, so its
 * own card on the open row.
 *
 * <p>Mounts only inside an open row, so the fetch is paid for on a reader's
 * click rather than while browsing.
 */
export function SampleValues({
   run,
   fetchSamples,
   modelPath,
   sourceName,
   field,
   generation,
}: {
   run: RunContext;
   fetchSamples?: SampleValuesFetcher;
   modelPath: string;
   sourceName: string;
   field: ExplorerField;
   /** The host build the values come from (see useDimensionSamples). */
   generation?: string;
}) {
   const { samples, total, unavailable, loading, failed } = useDimensionSamples(
      {
         run,
         fetchSamples,
         modelPath,
         sourceName,
         field,
         generation,
      },
   );
   return (
      <Box data-testid="model-viewer-values" sx={{ fontSize: fontSizes.small }}>
         <Box
            component="div"
            sx={{
               color: TEXT_TERTIARY,
               fontSize: fontSizes.caption,
               mb: "6px",
            }}
         >
            {total != null
               ? `The most common of ${total.toLocaleString()} ${total === 1 ? "value" : "values"}`
               : "The most common values"}
         </Box>
         {loading ? (
            <CircularProgress size={12} sx={{ color: "text.secondary" }} />
         ) : unavailable ? (
            // Why there are none: a given a run needs first, or a host's reason.
            <Box
               component="span"
               data-testid="model-viewer-values-unavailable"
               sx={{ color: "text.secondary" }}
            >
               {unavailable}
            </Box>
         ) : failed ? (
            // A failed read is an unknown, not an empty list.
            <Box
               component="span"
               sx={{ fontStyle: "italic", color: "text.secondary" }}
            >
               Sample values couldn&apos;t be loaded.
            </Box>
         ) : samples.length === 0 ? (
            <Box
               component="span"
               sx={{ fontStyle: "italic", color: "text.secondary" }}
            >
               No values found.
            </Box>
         ) : (
            <Box sx={{ display: "flex", flexWrap: "wrap", gap: "6px" }}>
               {samples.map((value) => (
                  <ValueChip key={value}>{value}</ValueChip>
               ))}
            </Box>
         )}
      </Box>
   );
}
