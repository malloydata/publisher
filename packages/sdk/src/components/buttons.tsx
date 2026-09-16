// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AddIcon from "@mui/icons-material/Add";
import { Button, Tooltip } from "@mui/material";
import * as React from "react";

/**
 * The Console's buttons, by the job they do. Four roles, and every button on
 * every page is one of them:
 *
 * 1. **Primary** — the one thing a surface is for: adding a package, a
 *    dashboard, a materialization; the confirm in a dialog. A filled dark pill
 *    at the default size. {@link AddButton} is the section-heading form of it.
 * 2. **Secondary** — a control beside the primary one, changing something that
 *    already exists: Scope, Schedule, Filter. An outlined pill, the same size
 *    as the primary, so a heading row reads as one set of controls rather than
 *    a big button and some small ones. {@link SecondaryButton}.
 * 3. **Quiet** — a dense bar of equals, where an outline per item would draw a
 *    grid: the dashboard builder's toolbar, a dialog's Cancel. A small text
 *    button, written inline.
 * 4. **Icon** — a row's overflow menu or a compact toggle. A small icon
 *    button, written inline.
 *
 * The roles exist because the drift they replace was real: five hand-written
 * copies of the same add button, and secondary controls that were `size`
 * "small" beside a default-size primary on the same row, so the row read as
 * two families of control.
 */

/**
 * The control that adds one thing to a section: a filled pill with a plus,
 * right of the section's heading.
 *
 * The LABEL names the thing alone — "Package", "Connection", "Dashboard" —
 * because the plus already says add; the verb went back into the accessible
 * name, where a control called "Package" would otherwise say nothing about
 * what it does.
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
         sx={{ whiteSpace: "nowrap" }}
      >
         {label}
      </Button>
   );
}

/**
 * A secondary control: outlined, and the same size as the primary it sits
 * beside. Takes its own icon and, when it is disabled, the reason — a control
 * that greys out without saying why is a control a reader has to guess at.
 */
export function SecondaryButton({
   label,
   icon,
   onClick,
   disabled,
   disabledReason,
   ariaLabel,
   ariaHasPopup,
}: {
   label: string;
   icon?: React.ReactNode;
   onClick: (event: React.MouseEvent<HTMLButtonElement>) => void;
   disabled?: boolean;
   /** Shown on hover when disabled. */
   disabledReason?: string;
   /** Overrides the accessible name, which is otherwise the label. */
   ariaLabel?: string;
   ariaHasPopup?: "menu" | "dialog";
}) {
   const button = (
      // A disabled button fires no pointer events, so the tooltip needs a
      // wrapper that does.
      <span>
         <Button
            variant="outlined"
            startIcon={icon}
            onClick={onClick}
            disabled={disabled}
            aria-label={ariaLabel}
            aria-haspopup={ariaHasPopup}
            // A pill that wraps to two lines stops reading as a pill.
            sx={{ whiteSpace: "nowrap" }}
         >
            {label}
         </Button>
      </span>
   );
   return disabled && disabledReason ? (
      <Tooltip title={disabledReason}>{button}</Tooltip>
   ) : (
      button
   );
}
