// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The widest a page's content grows, centred beyond it: the model explorer's width. */
export const PAGE_MAX_WIDTH_PX = 1200;

/**
 * The frame a page under a package sits in: the model explorer's, so a model,
 * a report, the report builder and the messages between them share their
 * edges and moving between them does not shift the page.
 */
export const PAGE_FRAME_SX = {
   p: 3,
   maxWidth: PAGE_MAX_WIDTH_PX,
   mx: "auto",
} as const;

/**
 * {@link PAGE_FRAME_SX} for a page that draws its own title, a report in
 * either mode. Its title lands where the model's and the package's do, 48px
 * under the top bar: the Model component adds the second 24px itself.
 */
export const TITLED_PAGE_FRAME_SX = { ...PAGE_FRAME_SX, pt: 6 } as const;
