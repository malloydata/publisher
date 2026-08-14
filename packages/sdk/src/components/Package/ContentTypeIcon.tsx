import SvgIcon, { SvgIconProps } from "@mui/material/SvgIcon";
import { MALLOY_ACCENT, MALLOY_BRAND } from "../styles";

export type ContentType =
   | "report"
   | "model"
   | "data"
   | "materialization"
   | "dataApp"
   | "dashboard";

/**
 * The backplate color behind each icon. One per content type and no repeats: on
 * a page that lists all six, a shared color reads as a shared kind, and the
 * glyphs are small enough that color does most of the telling apart.
 *
 * The logo's three go to the three things a package has always held, its models
 * and the notebooks and dashboards built on them, and the accents to the rest,
 * so the brand still leads.
 *
 * Exhaustive by type rather than by a default, so adding a `ContentType` is a
 * compile error here instead of a row that silently paints itself the same as
 * its neighbour.
 */
export const CONTENT_TINT: Record<ContentType, string> = {
   dashboard: MALLOY_BRAND.orange,
   report: MALLOY_BRAND.teal,
   model: MALLOY_BRAND.darkBlue,
   dataApp: MALLOY_ACCENT.violet,
   data: MALLOY_ACCENT.moss,
   materialization: MALLOY_ACCENT.magenta,
};

interface ContentTypeIconProps extends Omit<SvgIconProps, "fontSize"> {
   type: ContentType;
   /** Pixel size of the rendered icon. */
   size?: number;
}

/**
 * Inline SVGs that visually match Central Icons'
 * round-outlined-radius-2-stroke-1 family
 * (IconFileChart, Icon3dBoxTop, IconTable). Reimplemented as plain SVG
 * so the SDK does not pick up the paid @central-icons-react dependency.
 */
export default function ContentTypeIcon({
   type,
   size = 18,
   sx,
   ...rest
}: ContentTypeIconProps) {
   return (
      <SvgIcon
         {...rest}
         viewBox="0 0 24 24"
         sx={{
            width: size,
            height: size,
            fill: "none",
            stroke: "currentColor",
            strokeWidth: 1.5,
            strokeLinecap: "round",
            strokeLinejoin: "round",
            ...sx,
         }}
      >
         {type === "report" && <FileChartPath />}
         {type === "model" && <BoxTopPath />}
         {type === "data" && <TablePath />}
         {type === "materialization" && <StackPath />}
         {type === "dataApp" && <BrowserWindowPath />}
         {type === "dashboard" && <DashboardGridPath />}
      </SvgIcon>
   );
}

/** File outline with a folded top-right corner and three chart bars inside. */
function FileChartPath() {
   return (
      <>
         <path d="M14 3 H6 a2 2 0 0 0 -2 2 v14 a2 2 0 0 0 2 2 h12 a2 2 0 0 0 2 -2 V9 z" />
         <path d="M14 3 v4 a2 2 0 0 0 2 2 h4" />
         <path d="M8 17 v-3" />
         <path d="M12 17 v-6" />
         <path d="M16 17 v-4" />
      </>
   );
}

/** Cube viewed from above with visible top face — matches Icon3dBoxTop. */
function BoxTopPath() {
   return (
      <>
         <path d="M12 3 L4 7 v10 l8 4 l8 -4 V7 z" />
         <path d="M4 7 l8 4 l8 -4" />
         <path d="M12 11 v10" />
      </>
   );
}

/** Rounded grid icon — matches IconTable. */
function TablePath() {
   return (
      <>
         <rect x="3.5" y="3.5" width="17" height="17" rx="2" />
         <path d="M3.5 9.5 H20.5" />
         <path d="M9.5 3.5 V20.5" />
      </>
   );
}

/** Stacked layers icon for materialized output tables. */
function StackPath() {
   return (
      <>
         <path d="M12 3 L21 7.5 L12 12 L3 7.5 Z" />
         <path d="M3 12 L12 16.5 L21 12" />
         <path d="M3 16.5 L12 21 L21 16.5" />
      </>
   );
}

/**
 * Panels of unequal size in one frame: the grid of tiles a dashboard lays out.
 *
 * The neighbour to stay distinct from is the TABLE glyph, not the data app's
 * browser window. Both this and the table are a rounded rect with the same
 * `M3.5 9.5` divider, so with only the one vertical stroke the two differed by
 * that stroke's position alone and read as the same icon at 18px, with the
 * Dashboards and Databases sections on one page. The second divider is what
 * makes this one read as a grid rather than as columns.
 */
function DashboardGridPath() {
   return (
      <>
         <rect x="3.5" y="3.5" width="17" height="17" rx="2" />
         <path d="M3.5 9.5 H20.5" />
         <path d="M12 9.5 V20.5" />
         <path d="M12 15 H20.5" />
      </>
   );
}

/** Browser-window outline with an address-bar separator and content lines. */
function BrowserWindowPath() {
   return (
      <>
         <rect x="3.5" y="4.5" width="17" height="15" rx="2" />
         <path d="M3.5 9.5 H20.5" />
         <path d="M7 13.5 H17" />
         <path d="M7 16.5 H13" />
      </>
   );
}
