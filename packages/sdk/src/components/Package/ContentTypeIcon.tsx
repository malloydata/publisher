import SvgIcon, { SvgIconProps } from "@mui/material/SvgIcon";

type ContentType = "report" | "model" | "data";

interface ContentTypeIconProps extends Omit<SvgIconProps, "fontSize"> {
   type: ContentType;
   /** Pixel size of the rendered icon. */
   size?: number;
}

/**
 * Inline SVGs that visually match Central Icons'
 * round-outlined-radius-2-stroke-1 family
 * (IconFileChart, Icon3dBoxTop, IconTable) used by the Credible app.
 * Reimplemented as plain SVG so the open-source SDK does not pick up
 * the paid @central-icons-react dependency.
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
