// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Typography, type TypographyProps } from "@mui/material";
import { forwardRef } from "react";

/** Secondary body text: a row's explanation, an empty state's line. */
export const BodyText = forwardRef<HTMLSpanElement, TypographyProps>(
   function BodyText(props, ref) {
      return (
         <Typography
            ref={ref}
            variant="body2"
            {...props}
            sx={{ color: "text.secondary", ...props.sx }}
         />
      );
   },
);

/** Caption text: a note under a result, a meta line. */
export const CaptionText = forwardRef<HTMLSpanElement, TypographyProps>(
   function CaptionText(props, ref) {
      return (
         <Typography
            ref={ref}
            variant="caption"
            {...props}
            sx={{ color: "text.secondary", ...props.sx }}
         />
      );
   },
);
