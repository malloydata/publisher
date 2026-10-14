// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Stable class names on the parts of the report reader and builder, for a
 * host to style what the theme tokens (`ui`, the instance theme, the host's
 * MUI palette) do not reach: a global stylesheet, or a styled wrapper,
 * selecting `.publisher-tile` rather than an MUI class that changes between
 * versions.
 *
 * These names are API: they are kept across releases, and a part that goes
 * away is said so in the release notes.
 */
export const reportClasses = {
   /** A report page, read or edited: header, filter row and tiles. */
   report: "publisher-report",
   /** The title and description over a report. */
   header: "publisher-report-header",
   /** The report's title. */
   title: "publisher-report-title",
   /** The row of filter buttons (pinned under the header while scrolling). */
   filterRow: "publisher-filter-row",
   /** One filter's button. */
   filter: "publisher-filter",
   /** The popover a filter button opens. */
   filterPopover: "publisher-filter-popover",
   /** A tile's card: a query result, or a block of prose (`data-tile-kind`). */
   tile: "publisher-tile",
   /** A tile's title and subtitle. */
   tileHeading: "publisher-tile-heading",
   /** A cell that can be clicked to filter (`# drill`). */
   drillCell: "publisher-drill",
   /** The builder, around the page it edits. */
   builder: "publisher-builder",
   /** The builder's controls as one bar (`BuilderBar`). */
   builderBar: "publisher-builder-bar",
   /** The bar's save status. */
   saveStatus: "publisher-save-status",
   /** A tile as the builder draws it: selection, handle and menu around the card. */
   builderTile: "publisher-builder-tile",
   /** The grip on a tile's right edge that sets its width. */
   widthHandle: "publisher-width-handle",
   /** The builder's menus (Add, a tile's ⋯, the save status). */
   menu: "publisher-menu",
   /** Every builder window; each also carries its own name below. */
   dialog: "publisher-dialog",
   addTileDialog: "publisher-add-tile-dialog",
   filterDialog: "publisher-filter-dialog",
   clickableCellsDialog: "publisher-clickable-cells-dialog",
} as const;
