// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PALETTE, type ThemeMode } from "@malloy-publisher/sdk";
import { createTheme } from "@mui/material/styles";
import { colors, greyScale, SANS_FONT_FAMILY } from "./colors";

export const layout = {
   headerHeight: 56,
};

const SUBTLE_SHADOW = "0 1px 3px rgba(0, 0, 0, 0.1)";

// Dark-mode palette tokens. The slate family matches the in-app Theme
// Editor's dark defaults so the chrome and the rendered viz surfaces
// share a visual vocabulary.
const DARK_BACKGROUND = "#0f172a";
const DARK_SURFACE = "#1e293b";
const DARK_TEXT_PRIMARY = "#f1f5f9";
const DARK_TEXT_SECONDARY = "#cbd5e1";
const DARK_DIVIDER = "#334155";

/**
 * Create the MUI theme for the Publisher app shell in the requested mode.
 * Driven by the SDK's `usePublisherTheme()` mode (light or dark; "auto"
 * is resolved upstream). Component-level overrides switch on `isDark`
 * rather than re-reading the palette so the file is grep-able per token.
 */
export const createPublisherTheme = (mode: ThemeMode = "light") => {
   const isDark = mode === "dark";
   const background = isDark ? DARK_BACKGROUND : colors.white;
   const surface = isDark ? DARK_SURFACE : colors.white;
   const textPrimary = isDark ? DARK_TEXT_PRIMARY : colors.black;
   const textSecondary = isDark ? DARK_TEXT_SECONDARY : colors.grey.mid;
   const divider = isDark ? DARK_DIVIDER : colors.grey.light;

   // Contained primary buttons read their bg from primary.main and their text
   // from contrastText, so this is the colour of every confirming action in
   // the Console. The palette's anchor blue, which is also its first chart
   // series: the button that saves a dashboard and the first line on it are
   // the same hue, and the page has one accent rather than a neutral button
   // beside coloured content.
   //
   // Dark mode inverts the pair rather than shifting the blue. A mid blue on a
   // slate page is only 3.5:1 against the page and puts white text at 3.7:1,
   // under the 4.5:1 a label needs — and its hover state was worse than its
   // resting one. A bright fill with a near-black label reads at 7:1 on both
   // counts, and is what a dark theme wants anyway: the button is the lit
   // thing on the page, not a darker patch of it.
   const primaryMain = isDark ? "#60a5fa" : PALETTE.blue;
   const primaryHover = isDark ? "#93c5fd" : "#1d4ed8";
   const primaryContrast = isDark ? "#0f172a" : "#ffffff";

   return createTheme({
      cssVariables: { nativeColor: true },
      palette: {
         mode,
         primary: {
            main: primaryMain,
            light: colors.grey.light,
            dark: primaryHover,
            contrastText: primaryContrast,
         },
         secondary: {
            main: colors.grey.mid,
            light: colors.grey.light,
            dark: colors.black,
         },
         grey: greyScale,
         success: colors.semantic.success,
         warning: colors.semantic.warning,
         error: colors.semantic.error,
         info: colors.semantic.info,
         background: {
            default: background,
            paper: surface,
         },
         text: {
            primary: textPrimary,
            secondary: textSecondary,
         },
         divider,
      },
      typography: {
         fontFamily: SANS_FONT_FAMILY,
         h1: { fontWeight: 500, letterSpacing: "-0.025em" },
         h2: { fontWeight: 500, letterSpacing: "-0.025em" },
         h3: { fontWeight: 500, letterSpacing: "-0.025em" },
         h4: { fontWeight: 500, letterSpacing: "-0.025em" },
         h5: { fontWeight: 500, letterSpacing: "-0.025em" },
         h6: { fontWeight: 500, letterSpacing: "-0.025em" },
         subtitle1: { fontWeight: 500, letterSpacing: "-0.025em" },
         subtitle2: { fontWeight: 500, letterSpacing: "-0.025em" },
         body1: { letterSpacing: "-0.025em" },
         body2: { letterSpacing: "-0.025em" },
         button: {
            fontWeight: 500,
            letterSpacing: "-0.025em",
            textTransform: "none",
         },
      },
      shape: {
         borderRadius: 4,
      },
      components: {
         MuiButtonBase: {
            defaultProps: {
               disableRipple: true,
               disableTouchRipple: true,
            },
         },
         MuiCssBaseline: {
            styleOverrides: {
               "*": {
                  scrollbarWidth: "thin",
                  scrollbarColor: isDark
                     ? `#475569 #1e293b`
                     : `${greyScale[300]} ${greyScale[100]}`,
                  "&::-webkit-scrollbar": {
                     width: "8px",
                     height: "8px",
                  },
                  "&::-webkit-scrollbar-track": {
                     background: isDark ? "#1e293b" : greyScale[100],
                     borderRadius: "4px",
                  },
                  "&::-webkit-scrollbar-thumb": {
                     background: isDark ? "#475569" : greyScale[300],
                     borderRadius: "4px",
                     "&:hover": {
                        background: isDark ? "#64748b" : greyScale[400],
                     },
                  },
                  "&::-webkit-scrollbar-corner": {
                     background: isDark ? "#1e293b" : greyScale[100],
                  },
               },
               "div[data-radix-popper-content-wrapper]": {
                  zIndex: "1300 !important",
               },
            },
         },
         MuiButton: {
            styleOverrides: {
               root: {
                  borderRadius: 20,
                  textTransform: "none",
                  fontWeight: 500,
                  boxShadow: "none",
                  "&:hover": {
                     boxShadow: SUBTLE_SHADOW,
                  },
               },
               contained: {
                  "&:hover": {
                     boxShadow: "0 2px 4px rgba(0, 0, 0, 0.1)",
                  },
                  // A disabled filled button defaults to a WHITE wash, which
                  // on a dark dialog is lighter than the surface it sits on —
                  // the one control on the page drawing attention to itself is
                  // the one that cannot be pressed. Recede instead: a fill
                  // barely off the surface, and a label that reads as
                  // unavailable rather than as absent.
                  "&.Mui-disabled": {
                     backgroundColor: isDark
                        ? "rgba(255, 255, 255, 0.06)"
                        : greyScale[200],
                     color: isDark ? "#64748b" : greyScale[500],
                  },
               },
               // Outlined buttons take their text + border color from
               // primary.main by default. Our primary.main is a low-key
               // slate that all but disappears against the dark page, so
               // override outlined buttons to use the higher-contrast
               // text/divider tokens instead.
               outlined: {
                  color: textPrimary,
                  borderColor: isDark ? "#475569" : greyScale[300],
                  "&:hover": {
                     borderColor: isDark ? "#64748b" : greyScale[400],
                     backgroundColor: isDark
                        ? "rgba(255, 255, 255, 0.06)"
                        : "rgba(0, 0, 0, 0.04)",
                  },
               },
            },
         },
         MuiCard: {
            styleOverrides: {
               root: {
                  borderRadius: 4,
                  boxShadow: SUBTLE_SHADOW,
                  border: "1px solid",
                  borderColor: divider,
                  backgroundColor: surface,
               },
            },
         },
         MuiChip: {
            styleOverrides: {
               root: {
                  borderRadius: 4,
                  fontWeight: 500,
               },
            },
         },
         MuiTextField: {
            styleOverrides: {
               root: {
                  "& .MuiOutlinedInput-root": {
                     borderRadius: 4,
                  },
                  "& .MuiInputLabel-outlined": {
                     backgroundColor: surface,
                     paddingLeft: 4,
                     paddingRight: 4,
                     "&.MuiInputLabel-shrink": {
                        transform: "translate(14px, -9px) scale(0.75)",
                     },
                  },
               },
            },
         },
         MuiPaper: {
            styleOverrides: {
               root: {
                  // MUI lightens `background.paper` in dark mode by painting a
                  // white gradient over it, scaled by elevation. It makes the
                  // value of `background.paper` a lie: a Dialog at elevation
                  // 24 came out several steps lighter than the fields and
                  // label notches inside it, which paint the honest value —
                  // so every field read as a hole and every floating label sat
                  // on a grey chip of its own. Off, so one token means one
                  // colour wherever it is used.
                  backgroundImage: "none",
               },
            },
         },
         MuiDialog: {
            styleOverrides: {
               paper: {
                  borderRadius: 4,
                  // Stated rather than inherited, so a dialog's surface cannot
                  // drift from the fields drawn on it.
                  backgroundColor: surface,
                  boxShadow:
                     "0px 20px 25px -5px rgba(0, 0, 0, 0.1), 0px 10px 10px -5px rgba(0, 0, 0, 0.04)",
               },
            },
         },
         MuiSelect: {
            styleOverrides: {
               select: {
                  fontFamily: SANS_FONT_FAMILY,
                  fontSize: "0.875rem",
                  // The VALUE, in the text colour a typed value gets. In
                  // secondary grey — which is what this was — a filled select
                  // read as a disabled one, and a form of them read as a form
                  // nobody could fill in.
                  color: textPrimary,
               },
            },
         },
         MuiInputLabel: {
            styleOverrides: {
               root: {
                  "&.MuiInputLabel-outlined": {
                     backgroundColor: surface,
                     paddingLeft: 4,
                     paddingRight: 4,
                  },
               },
            },
         },
         MuiOutlinedInput: {
            styleOverrides: {
               notchedOutline: {
                  border: "none",
               },
               root: {
                  // The card edge, not `divider`: a field is a box on the page
                  // the same way a dashboard card is, and it is already the
                  // construction an outlined Button uses (same two values,
                  // just above). On `divider` a field rested a full step
                  // lighter than the card holding it and the button beside it
                  // — three boxes in a row outlined three different ways.
                  // Kept in step with the SDK's `theme.cardBorder`
                  // (#cbd5e1 / #475569), which is these same two points on the
                  // slate ramp.
                  border: `1px solid ${isDark ? greyScale[600] : greyScale[300]}`,
                  borderRadius: 8,
                  transition: "border-color 120ms ease-in",
                  backgroundColor: surface,
                  // Rest darkened, so hover and focus each move up a rung to
                  // stay told apart from it — a field whose hover state is its
                  // resting state has no hover state.
                  "&:hover": {
                     borderColor: isDark ? "#64748b" : greyScale[400],
                  },
                  "&.Mui-focused": {
                     borderColor: isDark ? greyScale[400] : greyScale[500],
                     outline: "none",
                  },
               },
            },
         },
         MuiMenuItem: {
            styleOverrides: {
               root: {
                  fontFamily: SANS_FONT_FAMILY,
                  fontSize: "0.875rem",
               },
            },
         },
         MuiInputBase: {
            styleOverrides: {
               root: {
                  fontFamily: SANS_FONT_FAMILY,
               },
            },
         },
         MuiMenu: {
            styleOverrides: {
               paper: {
                  borderRadius: 4,
                  boxShadow:
                     "0px 10px 15px -3px rgba(0, 0, 0, 0.1), 0px 4px 6px -2px rgba(0, 0, 0, 0.05)",
                  border: `1px solid ${divider}`,
                  backgroundColor: surface,
                  color: textPrimary,
               },
            },
         },
         MuiBreadcrumbs: {
            styleOverrides: {
               root: {
                  "& .MuiBreadcrumbs-separator": {
                     margin: "0 6px",
                     color: greyScale[400],
                  },
               },
            },
         },
         MuiDivider: {
            styleOverrides: {
               root: {
                  borderColor: divider,
               },
            },
            defaultProps: {
               sx: {
                  my: 1,
               },
            },
         },
         MuiListItemButton: {
            styleOverrides: {
               root: {
                  borderRadius: 28,
                  margin: "2px 8px",
                  padding: "8px 16px",
                  "&:hover": {
                     backgroundColor: isDark
                        ? "rgba(255, 255, 255, 0.08)"
                        : colors.offWhite,
                  },
                  "&.Mui-selected": {
                     backgroundColor: isDark
                        ? "rgba(255, 255, 255, 0.12)"
                        : colors.offWhite,
                     color: textPrimary,
                     "& .MuiListItemIcon-root": {
                        color: textPrimary,
                     },
                     "& .MuiListItemText-primary": {
                        color: textPrimary,
                     },
                     "&:hover": {
                        backgroundColor: isDark
                           ? "rgba(255, 255, 255, 0.16)"
                           : colors.grey.light,
                     },
                  },
               },
            },
         },
         MuiAppBar: {
            styleOverrides: {
               root: {
                  backgroundColor: surface,
                  color: textPrimary,
                  boxShadow: SUBTLE_SHADOW,
               },
            },
         },
      },
   });
};

// Backwards-compat default export: the static light theme. The new
// PublisherMuiThemeProvider wires `createPublisherTheme(mode)` through
// `usePublisherTheme()` so the live mode toggle takes effect. Anywhere
// that still imports `default` (e.g. legacy embeds) keeps working.
const theme = createPublisherTheme("light");
export default theme;
