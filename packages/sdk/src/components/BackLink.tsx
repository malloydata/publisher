// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import { Link } from "@mui/material";
import * as React from "react";

/**
 * The way up, at the top of a page: an arrow and the name of the thing you came
 * from.
 *
 * Every page that has a parent shows one, in this one shape, because the
 * alternative is what was here before — the package page had it, the
 * environment page above it did not, the model and notebook pages had no way up
 * at all, and one dead-end page said the same words as prose in the middle of a
 * sentence. The breadcrumb in the app's header bar is a different affordance
 * and does not replace it: it is chrome the eye skips, it cannot say what the
 * parent IS (it renders a model path as one opaque chip), and it is absent on
 * the pages the app shell does not parameterise.
 *
 * The click is the host's, so a Console navigates its router and an embedder
 * does whatever it does; a middle-click or a modified click reaches the handler
 * with its event, the same as any other navigation in the SDK.
 */
export function BackLink({
   label,
   onClick,
}: {
   /** The parent, named: "Back to {label}". */
   label: string;
   onClick: (event: React.MouseEvent) => void;
}) {
   return (
      <Link
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
            "&:hover": { color: "primary.main" },
         }}
      >
         <ArrowBackIcon sx={{ fontSize: 18 }} />
         Back to {label}
      </Link>
   );
}
