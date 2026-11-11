// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";
import { Link as RouterLink } from "react-router-dom";
import { MONO_FONT_FAMILY } from "../../../theme/colors";
import { PAGE_FRAME_SX } from "../pageFrame";

/** What the builder says when it is asked to edit a `.malloynb`. */
export const OLDER_NOTEBOOK =
   "This is an older notebook. It can't be edited here. Convert it to a report to edit it.";

/**
 * `?edit` on a `.malloynb`: the builder edits `.malloy` reports only, and the
 * older notebook format is read, not edited. Said in place of a not-found for
 * a `.malloy` file of the same name, with the way to the notebook and to the
 * conversion an agent can run.
 */
export default function OlderNotebookNotice({
   environmentName,
   packageName,
   path,
}: {
   environmentName: string;
   packageName: string;
   /** The notebook's package path, `sales/q3.malloynb`. */
   path: string;
}) {
   const mono = { fontFamily: MONO_FONT_FAMILY };
   const view = `/${[environmentName, packageName, ...path.split("/")]
      .map(encodeURIComponent)
      .join("/")}`;
   return (
      <Box sx={PAGE_FRAME_SX}>
         <Typography variant="h6" sx={{ fontWeight: "fontWeightMedium" }}>
            {OLDER_NOTEBOOK}
         </Typography>
         <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            <Box component="span" sx={mono}>
               {path}
            </Box>{" "}
            is read only. An agent with the Malloy skills converts it: ask it to
            rewrite this notebook as a{" "}
            <Box component="span" sx={mono}>
               .malloy
            </Box>{" "}
            report (the{" "}
            <Box component="span" sx={mono}>
               malloy-reports
            </Box>{" "}
            skill has the recipe), then open the new report here to edit it.
         </Typography>
         <Button component={RouterLink} to={view} sx={{ mt: 2 }}>
            View the notebook
         </Button>
      </Box>
   );
}
