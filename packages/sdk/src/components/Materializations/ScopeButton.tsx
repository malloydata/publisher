// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import CheckIcon from "@mui/icons-material/Check";
import LayersOutlinedIcon from "@mui/icons-material/LayersOutlined";
import {
   Button,
   ListItemIcon,
   ListItemText,
   Menu,
   MenuItem,
   Tooltip,
} from "@mui/material";
import { useState } from "react";

export type Scope = "package" | "version";

const WHAT_IT_MEANS: Record<Scope, string> = {
   package: "Materialized artifacts are shared across versions",
   version: "Materialized artifacts are per published version",
};

/**
 * The package's persist scope, as one button.
 *
 * It is a menu rather than a pair of toggles because the two values are a
 * choice, not a filter, and because the choice is usually made once and then
 * left alone — the section's heading row is not the place for a permanent
 * two-button control. What each value means is in the menu, where it is read at
 * the moment of choosing rather than sitting on the page forever.
 */
export default function ScopeButton({
   scope,
   disabled,
   disabledReason,
   isSubmitting,
   onChange,
}: {
   scope: Scope;
   disabled?: boolean;
   /** Why the control is disabled, shown on hover. */
   disabledReason?: string;
   isSubmitting?: boolean;
   onChange: (scope: Scope) => Promise<unknown>;
}) {
   const [anchor, setAnchor] = useState<HTMLElement | null>(null);

   const choose = async (next: Scope) => {
      setAnchor(null);
      if (next !== scope) await onChange(next).catch(() => undefined);
   };

   const button = (
      <span>
         <Button
            variant="outlined"
            startIcon={<LayersOutlinedIcon />}
            onClick={(event) => setAnchor(event.currentTarget)}
            disabled={disabled || isSubmitting}
            aria-haspopup="menu"
            // The value is in the name, so the current scope is legible without
            // opening the menu and a test can assert it.
            aria-label={`Scope: ${scope}`}
         >
            Scope
         </Button>
      </span>
   );

   return (
      <>
         {disabled && disabledReason ? (
            <Tooltip title={disabledReason}>{button}</Tooltip>
         ) : (
            button
         )}
         <Menu
            anchorEl={anchor}
            open={Boolean(anchor)}
            onClose={() => setAnchor(null)}
            anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
            transformOrigin={{ vertical: "top", horizontal: "right" }}
         >
            {(["package", "version"] as const).map((value) => (
               <MenuItem
                  key={value}
                  selected={value === scope}
                  onClick={() => void choose(value)}
               >
                  <ListItemIcon sx={{ minWidth: 32 }}>
                     {value === scope && <CheckIcon fontSize="small" />}
                  </ListItemIcon>
                  <ListItemText
                     primary={value}
                     secondary={WHAT_IT_MEANS[value]}
                  />
               </MenuItem>
            ))}
         </Menu>
      </>
   );
}
