// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AddIcon from "@mui/icons-material/Add";
import { Button } from "@mui/material";

/**
 * The control that adds one thing to a section: a filled pill with a plus,
 * right of the section's heading.
 *
 * Two rules, and both are why this exists rather than another hand-written
 * `<Button>`. The look is the theme's contained primary at the default size, so
 * every add on every page is the same 40px dark pill rather than five near
 * copies that drifted. And the LABEL names the thing alone — "Package",
 * "Connection", "Dashboard" — because the plus already says add; the verb went
 * back into the accessible name, where a control called "Package" would
 * otherwise say nothing about what it does.
 */
export function AddButton({
   label,
   onClick,
   disabled,
   icon,
}: {
   /** The thing being added, as it should read on the button: "Package". */
   label: string;
   onClick: () => void;
   disabled?: boolean;
   /** Replaces the plus, for an add that wants its own glyph. */
   icon?: React.ReactNode;
}) {
   return (
      <Button
         variant="contained"
         color="primary"
         startIcon={icon ?? <AddIcon />}
         onClick={onClick}
         disabled={disabled}
         aria-label={`Add ${label.toLowerCase()}`}
      >
         {label}
      </Button>
   );
}
