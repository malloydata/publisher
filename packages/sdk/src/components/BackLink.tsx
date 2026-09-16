// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import { Link } from "@mui/material";
import * as React from "react";

/**
 * The way up, at the top of a page: an arrow and the name of the thing you came
 * from.
 *
 * Every page that has a parent shows one, in this one shape. The breadcrumb in
 * the app's header bar is a different affordance and does not replace it: it is
 * chrome the eye skips, it cannot say what the parent IS (it renders a model
 * path as one opaque chip), and it is absent on the pages the app shell does
 * not parameterise.
 *
 * The click is the host's, so a Console navigates its router and an embedder
 * does whatever it does. Give it an `href` as well wherever the parent has a
 * URL: that is what makes it a link rather than a widget that looks like one —
 * focusable from the keyboard, announced as a link, and openable in a new tab
 * with a middle-click or a modified click, which reach the handler with their
 * event like any other navigation in the SDK. Without an `href` it still has to
 * be reachable, so it renders as a button rather than as an anchor with nothing
 * to point at, which is focusable by neither keyboard nor assistive technology.
 */
export function BackLink({
   label,
   href,
   onClick,
}: {
   /** The parent, named: "Back to {label}". */
   label: string;
   /** The parent's URL, when it has one. */
   href?: string;
   onClick: (event: React.MouseEvent) => void;
}) {
   return (
      <Link
         {...(href === undefined
            ? { component: "button" as const, type: "button" as const }
            : { href })}
         onClick={onClick}
         underline="none"
         aria-label={`Back to ${label}`}
         sx={{
            display: "inline-flex",
            alignItems: "center",
            gap: 0.5,
            cursor: "pointer",
            color: "text.secondary",
            fontSize: "0.875rem",
            mb: 2,
            // A `component="button"` link brings the platform's button chrome
            // with it; this is a link either way.
            border: 0,
            p: 0,
            background: "none",
            font: "inherit",
            "&:hover": { color: "primary.main" },
         }}
      >
         <ArrowBackIcon sx={{ fontSize: 18 }} />
         Back to {label}
      </Link>
   );
}
