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
import type { PackageWarning } from "../../client";
import { MONO_FONT_FAMILY } from "../styles";

/**
 * A renamed warning code's former name, which a server repeats beside the new
 * one, same message, for one release: by former code, the code it repeats.
 * `persist-source-refused` is 0.14.0's; the others are 0.15.0's.
 */
const RETAINED_ALIAS_OF: Readonly<Record<string, string>> = {
   "persist-source-refused": "persist-not-built",
   "preaggregate-not-built": "rollup-not-built",
   "preaggregate-join-one-key-repeated": "rollup-join-one-key-repeated",
   "stored-native-schema-missing": "database-native-type-missing",
};

/**
 * `warnings` with each retained-alias repeat dropped when the warning it
 * repeats is beside it, so each problem shows once. An older server's
 * former code, with no new code beside it, is the only copy and still shows.
 */
export function withoutRetainedAliases(
   warnings: readonly PackageWarning[],
): PackageWarning[] {
   const key = (code: string | undefined, w: PackageWarning) =>
      `${code}\u0000${w.subject ?? ""}\u0000${w.message ?? ""}`;
   const present = new Set(warnings.map((w) => key(w.code, w)));
   return warnings.filter((w) => {
      const now = w.code !== undefined ? RETAINED_ALIAS_OF[w.code] : undefined;
      return now === undefined || !present.has(key(now, w));
   });
}

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
   warnings: PackageWarning[] | undefined;
}) {
   const [showNotes, setShowNotes] = useState(false);
   const shown = withoutRetainedAliases(warnings ?? []);
   const errors = shown.filter((w) => w.severity === "error");
   const notes = shown.filter((w) => w.severity !== "error");
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

function FindingList({ findings }: { findings: PackageWarning[] }) {
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
