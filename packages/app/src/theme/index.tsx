import { createTheme } from "@mui/material/styles";
import { colors, greyScale, SANS_FONT_FAMILY } from "./colors";

export const layout = {
   headerHeight: 56,
};

const SUBTLE_SHADOW = "0 1px 3px rgba(0, 0, 0, 0.1)";

const theme = createTheme({
   cssVariables: { nativeColor: true },
   palette: {
      mode: "light",
      primary: {
         main: colors.black,
         light: colors.grey.light,
         dark: colors.black,
         contrastText: colors.white,
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
         default: colors.white,
         paper: colors.white,
      },
      text: {
         primary: colors.black,
         secondary: colors.grey.mid,
      },
      divider: colors.grey.light,
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
               scrollbarColor: `${greyScale[300]} ${greyScale[100]}`,
               "&::-webkit-scrollbar": {
                  width: "8px",
                  height: "8px",
               },
               "&::-webkit-scrollbar-track": {
                  background: greyScale[100],
                  borderRadius: "4px",
               },
               "&::-webkit-scrollbar-thumb": {
                  background: greyScale[300],
                  borderRadius: "4px",
                  "&:hover": {
                     background: greyScale[400],
                  },
               },
               "&::-webkit-scrollbar-corner": {
                  background: greyScale[100],
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
            },
         },
      },
      MuiCard: {
         styleOverrides: {
            root: {
               borderRadius: 4,
               boxShadow: SUBTLE_SHADOW,
               border: "1px solid",
               borderColor: colors.grey.light,
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
                  backgroundColor: "white",
                  paddingLeft: 4,
                  paddingRight: 4,
                  "&.MuiInputLabel-shrink": {
                     transform: "translate(14px, -9px) scale(0.75)",
                  },
               },
            },
         },
      },
      MuiDialog: {
         styleOverrides: {
            paper: {
               borderRadius: 4,
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
               color: greyScale[600],
            },
         },
      },
      MuiInputLabel: {
         styleOverrides: {
            root: {
               "&.MuiInputLabel-outlined": {
                  backgroundColor: "white",
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
               border: `1px solid ${greyScale[200]}`,
               borderRadius: 8,
               transition: "border-color 120ms ease-in",
               "&:hover": {
                  borderColor: greyScale[300],
               },
               "&.Mui-focused": {
                  borderColor: greyScale[400],
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
               border: `1px solid ${colors.grey.light}`,
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
               borderColor: colors.grey.light,
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
                  backgroundColor: colors.offWhite,
               },
               "&.Mui-selected": {
                  backgroundColor: colors.offWhite,
                  color: colors.black,
                  "& .MuiListItemIcon-root": {
                     color: colors.black,
                  },
                  "& .MuiListItemText-primary": {
                     color: colors.black,
                  },
                  "&:hover": {
                     backgroundColor: colors.grey.light,
                  },
               },
            },
         },
      },
      MuiAppBar: {
         styleOverrides: {
            root: {
               backgroundColor: colors.white,
               color: colors.black,
               boxShadow: SUBTLE_SHADOW,
            },
         },
      },
   },
});

export default theme;
