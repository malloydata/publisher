// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Alert,
   AlertTitle,
   Box,
   Button,
   Stack,
   Typography,
} from "@mui/material";
import { useState } from "react";
import type { PackageWarningsInner } from "../../client";
import { MONO_FONT_FAMILY } from "../styles";

/**
 * What the server found wrong with a package when it loaded it, on the
 * package page.
 *
 * Without it a broken file simply moved: a report whose artifact tag the
 * server no longer reads stopped listing under Reports and listed under
 * Models instead, with nothing on the page saying why. Errors (a file that
 * should have been a report and is not, a tile that cannot load) are shown;
 * `warn` findings, which still work, are counted and folded away.
 */
export function PackageProblems({
   warnings,
}: {
   warnings: PackageWarningsInner[] | undefined;
}) {
   const [showNotes, setShowNotes] = useState(false);
   const errors = (warnings ?? []).filter((w) => w.severity === "error");
   const notes = (warnings ?? []).filter((w) => w.severity !== "error");
   if (errors.length === 0 && notes.length === 0) return null;
   return (
      <Stack sx={{ gap: 1, mb: 4 }}>
         {errors.length > 0 && (
            <Alert severity="warning">
               <AlertTitle>
                  {errors.length === 1
                     ? "1 problem in this package"
                     : `${errors.length} problems in this package`}
               </AlertTitle>
               <FindingList findings={errors} />
            </Alert>
         )}
         {notes.length > 0 && (
            <Box>
               <Button
                  size="small"
                  aria-expanded={showNotes}
                  onClick={() => setShowNotes((open) => !open)}
                  sx={{ px: 0.5, color: "text.secondary" }}
               >
                  {showNotes ? "Hide" : "Show"} {notes.length}{" "}
                  {notes.length === 1 ? "note" : "notes"}
               </Button>
               {showNotes && (
                  <Box sx={{ pl: 0.5, pt: 0.5 }}>
                     <FindingList findings={notes} />
                  </Box>
               )}
            </Box>
         )}
      </Stack>
   );
}

function FindingList({ findings }: { findings: PackageWarningsInner[] }) {
   return (
      <Stack component="ul" sx={{ m: 0, p: 0, listStyle: "none", gap: 0.75 }}>
         {findings.map((finding, index) => (
            <Typography
               component="li"
               key={`${finding.model ?? ""}:${finding.code ?? ""}:${index}`}
               variant="body2"
            >
               {finding.model && (
                  <Box
                     component="span"
                     sx={{
                        fontFamily: MONO_FONT_FAMILY,
                        fontSize: "0.85em",
                        mr: 1,
                     }}
                  >
                     {finding.model}
                  </Box>
               )}
               {finding.message}
            </Typography>
         ))}
      </Stack>
   );
}
