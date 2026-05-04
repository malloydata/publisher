import SvgIcon, { SvgIconProps } from "@mui/material/SvgIcon";

export default function SidebarToggleIcon(props: SvgIconProps) {
   return (
      <SvgIcon
         {...props}
         viewBox="0 0 24 24"
         sx={{ fill: "none", ...props.sx }}
      >
         <rect
            x="3.25"
            y="4.75"
            width="17.5"
            height="14.5"
            rx="2.25"
            stroke="currentColor"
            strokeWidth="1.5"
         />
         <line
            x1="8.5"
            y1="5"
            x2="8.5"
            y2="19"
            stroke="currentColor"
            strokeWidth="1.5"
         />
      </SvgIcon>
   );
}
