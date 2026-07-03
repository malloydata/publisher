import { usePublisherTheme, type ThemeMode } from "@malloy-publisher/sdk";
import BrightnessAutoIcon from "@mui/icons-material/BrightnessAuto";
import DarkModeIcon from "@mui/icons-material/DarkMode";
import LightModeIcon from "@mui/icons-material/LightMode";
import { IconButton, Tooltip } from "@mui/material";

type Choice = ThemeMode | "auto";

const NEXT_CHOICE: Record<Choice, Choice> = {
   light: "dark",
   dark: "auto",
   auto: "light",
};

const LABEL: Record<Choice, string> = {
   light: "Light mode (click for dark)",
   dark: "Dark mode (click for auto)",
   auto: "Auto mode (follows OS, click for light)",
};

/**
 * Three-state mode toggle: light → dark → auto → light.
 *
 * Reads the viewer's stored choice (which may be "auto") from the
 * SDK's ThemeContext, not the resolved mode. Otherwise a viewer who
 * picked "auto" on a dark-mode OS would see the moon icon and lose
 * the affordance that distinguishes "follow OS" from "explicit dark".
 *
 * Hides itself when the operator has set `allowUserToggle: false`.
 */
export function ThemeToggle() {
   const { mode, userChoice, setMode, allowUserToggle } = usePublisherTheme();
   if (!allowUserToggle) return null;

   const current: Choice = userChoice ?? mode;
   const next = NEXT_CHOICE[current];

   const Icon =
      current === "auto"
         ? BrightnessAutoIcon
         : current === "dark"
           ? DarkModeIcon
           : LightModeIcon;

   return (
      <Tooltip title={LABEL[current]}>
         <IconButton
            aria-label={LABEL[current]}
            onClick={() => setMode(next)}
            size="small"
         >
            <Icon fontSize="small" />
         </IconButton>
      </Tooltip>
   );
}
