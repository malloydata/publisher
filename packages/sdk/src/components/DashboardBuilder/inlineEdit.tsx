// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Theme } from "@mui/material";
import type { SystemStyleObject } from "@mui/system";
import type { ResolvedTheme } from "../../theme/types";

/** A thin pencil, drawn as a mask so it takes the text's own colour. */
const PENCIL = `url("data:image/svg+xml;utf8,${encodeURIComponent(
   '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="black" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20h9"/><path d="M16.5 3.5a2.12 2.12 0 0 1 3 3L7 19l-4 1 1-4Z"/></svg>',
)}")`;

/** Where the pencil goes: after a line of text, or after the first block of prose. */
export type PencilAt = "line" | "prose";

/**
 * How text that is edited where it is read says so: a thin pencil straight
 * after the text, in the text's own colour and in proportion to its size,
 * faint at rest and clearer under the pointer. The same mark in the same place
 * for every field — a title, a subtitle, a description — and nothing else: no
 * wash, no outline, no gutter. The keyboard gets a focus ring, since a pencil
 * does not say where focus is.
 *
 * Drawn as a pseudo-element, so it adds no node and moves no layout; for prose
 * it follows the first paragraph or heading, which is the line the reader is
 * looking at when they reach for it.
 */
export const editableSx = (
   theme: ResolvedTheme,
   at: PencilAt = "line",
): SystemStyleObject<Theme> => {
   // The pseudo-element's selector, at rest and while the field is hovered
   // or focused: on the field itself for a line; for prose, on its first
   // paragraph or heading, or on the lone span markdown renders a single line
   // of prose as, with no paragraph around it.
   const blocks = [
      ":is(p, h1, h2, h3, h4, h5, h6):first-child::after",
      "> * > span:only-child::after",
   ];
   const within = (prefix: string) =>
      blocks.map((block) => `${prefix} ${block}`).join(", ");
   const host = at === "line" ? "&::after" : within("&");
   const lit =
      at === "line"
         ? "&:hover::after, &:focus-visible::after"
         : `${within("&:hover")}, ${within("&:focus-visible")}`;
   return {
      cursor: "text",
      borderRadius: "2px",
      [host]: {
         content: '""',
         display: "inline-block",
         width: "0.8em",
         height: "0.8em",
         ml: "0.4em",
         verticalAlign: "-0.05em",
         backgroundColor: "currentColor",
         maskImage: PENCIL,
         WebkitMaskImage: PENCIL,
         maskSize: "contain",
         WebkitMaskSize: "contain",
         maskRepeat: "no-repeat",
         WebkitMaskRepeat: "no-repeat",
         opacity: 0.35,
         transition: "opacity 120ms",
         pointerEvents: "none",
      },
      [lit]: {
         opacity: 0.8,
      },
      "&:focus-visible": {
         outline: `2px solid ${theme.accent}`,
         outlineOffset: 2,
      },
   };
};
