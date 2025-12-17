import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import Typography from "@mui/material/Typography";

export interface LoadingProps {
   /**
    * The text to display below the spinner
    */
   text?: string;
   /**
    * The size of the CircularProgress component
    * @default 20
    */
   size?: number | string;
   /**
    * The color of the CircularProgress component
    * @default "primary"
    */
   color?:
      | "primary"
      | "secondary"
      | "error"
      | "info"
      | "success"
      | "warning"
      | "inherit";
   /**
    * The thickness of the circular progress
    * @default 3.6
    */
   thickness?: number;
   /**
    * Whether to center the component
    * @default true
    */
   centered?: boolean;
   /**
    * Custom spacing between spinner and text
    * @default 2
    */
   spacing?: number;
   /**
    * Typography variant for the text
    * @default "body1"
    */
   textVariant?:
      | "h1"
      | "h2"
      | "h3"
      | "h4"
      | "h5"
      | "h6"
      | "subtitle1"
      | "subtitle2"
      | "body1"
      | "body2"
      | "caption"
      | "overline";
}

export function Loading({
   text,
   size = 20,
   color = "primary",
   thickness = 3.6,
   centered = true,
   spacing = 2,
   textVariant = "body2",
}: LoadingProps) {
   const content = (
      <Box
         sx={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            gap: spacing,
         }}
      >
         <CircularProgress size={size} color={color} thickness={thickness} />
         {text && (
            <Typography variant={textVariant} color="text.secondary">
               {text}
            </Typography>
         )}
      </Box>
   );

   if (centered) {
      return (
         <Box
            sx={{
               display: "flex",
               justifyContent: "center",
               alignItems: "center",
               minHeight: "200px",
               width: "100%",
            }}
         >
            {content}
         </Box>
      );
   }

   return content;
}
