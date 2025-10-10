import { createTheme } from "@mui/material/styles";

// Malloy color palette based on brand colors
const malloyColors = {
   primary: {
      main: "#14b3cb", // Malloy teal/cyan
      light: "#22d3ee",
      dark: "#0e7490",
      50: "#ecfeff",
      100: "#cffafe",
      500: "#14b3cb",
      600: "#0891b2",
      700: "#0e7490",
   },
   secondary: {
      main: "#fbbb04", // Malloy yellow/gold
      light: "#fde047",
      dark: "#eab308",
      50: "#fefce8",
      100: "#fef3c7",
      500: "#fbbb04",
      600: "#eab308",
      700: "#ca8a04",
   },
   accent: {
      main: "#e47404", // Malloy orange
      light: "#fb923c",
      dark: "#ea580c",
      50: "#fff7ed",
      100: "#ffedd5",
      500: "#e47404",
      600: "#ea580c",
      700: "#c2410c",
   },
   grey: {
      50: "#f9fafb",
      100: "#f3f4f6",
      200: "#e5e7eb",
      300: "#d1d5db",
      400: "#9ca3af",
      500: "#6b7280",
      600: "#4b5563",
      700: "#374151",
      800: "#1f2937",
      900: "#111827",
   },
   success: {
      main: "#059669",
      light: "#10b981",
      dark: "#047857",
   },
   warning: {
      main: "#d97706",
      light: "#f59e0b",
      dark: "#b45309",
   },
   error: {
      main: "#dc2626",
      light: "#ef4444",
      dark: "#b91c1c",
   },
   info: {
      main: "#0891b2",
      light: "#06b6d4",
      dark: "#0e7490",
   },
};

const theme = createTheme({
   palette: {
      mode: "light",
      primary: malloyColors.primary,
      secondary: malloyColors.secondary,
      grey: malloyColors.grey,
      success: malloyColors.success,
      warning: malloyColors.warning,
      error: malloyColors.error,
      info: malloyColors.info,
      background: {
         default: "#ffffff",
         paper: "#ffffff",
      },
      text: {
         primary: malloyColors.grey[900],
         secondary: malloyColors.grey[600],
      },
      divider: malloyColors.grey[200],
   },
   typography: {
      fontFamily: '"Inter", "Roboto", "Helvetica", "Arial", sans-serif',
      h1: {
         fontWeight: 700,
         letterSpacing: "-0.025em",
      },
      h2: {
         fontWeight: 700,
         letterSpacing: "-0.025em",
      },
      h3: {
         fontWeight: 600,
         letterSpacing: "-0.025em",
      },
      h4: {
         fontWeight: 600,
         letterSpacing: "-0.025em",
      },
      h5: {
         fontWeight: 600,
         letterSpacing: "-0.025em",
      },
      h6: {
         fontWeight: 600,
         letterSpacing: "-0.025em",
      },
      subtitle1: {
         fontWeight: 500,
         letterSpacing: "-0.025em",
      },
      subtitle2: {
         fontWeight: 500,
         letterSpacing: "-0.025em",
      },
      body1: {
         letterSpacing: "-0.025em",
      },
      body2: {
         letterSpacing: "-0.025em",
      },
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
      MuiCssBaseline: {
         styleOverrides: {
            body: {
               scrollbarWidth: "thin",
               scrollbarColor: `${malloyColors.grey[300]} ${malloyColors.grey[100]}`,
               "&::-webkit-scrollbar": {
                  width: "8px",
                  height: "8px",
               },
               "&::-webkit-scrollbar-track": {
                  background: malloyColors.grey[100],
                  borderRadius: "4px",
               },
               "&::-webkit-scrollbar-thumb": {
                  background: malloyColors.grey[300],
                  borderRadius: "4px",
                  "&:hover": {
                     background: malloyColors.grey[400],
                  },
               },
               "&::-webkit-scrollbar-corner": {
                  background: malloyColors.grey[100],
               },
            },
            "*": {
               scrollbarWidth: "thin",
               scrollbarColor: `${malloyColors.grey[300]} ${malloyColors.grey[100]}`,
               "&::-webkit-scrollbar": {
                  width: "8px",
                  height: "8px",
               },
               "&::-webkit-scrollbar-track": {
                  background: malloyColors.grey[100],
                  borderRadius: "4px",
               },
               "&::-webkit-scrollbar-thumb": {
                  background: malloyColors.grey[300],
                  borderRadius: "4px",
                  "&:hover": {
                     background: malloyColors.grey[400],
                  },
               },
               "&::-webkit-scrollbar-corner": {
                  background: malloyColors.grey[100],
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
               borderRadius: 4,
               textTransform: "none",
               fontWeight: 500,
               boxShadow: "none",
               "&:hover": {
                  boxShadow: "0 1px 3px rgba(0, 0, 0, 0.1)",
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
               boxShadow: "0 1px 3px rgba(0, 0, 0, 0.1)",
               border: "1px solid",
               borderColor: "rgba(0, 0, 0, 0.05)",
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
      MuiMenu: {
         styleOverrides: {
            paper: {
               borderRadius: 4,
               boxShadow:
                  "0px 10px 15px -3px rgba(0, 0, 0, 0.1), 0px 4px 6px -2px rgba(0, 0, 0, 0.05)",
               border: "1px solid rgba(0, 0, 0, 0.05)",
            },
         },
      },
      MuiBreadcrumbs: {
         styleOverrides: {
            root: {
               "& .MuiBreadcrumbs-separator": {
                  margin: "0 6px",
                  color: malloyColors.grey[400],
               },
            },
         },
      },
      MuiDivider: {
         styleOverrides: {
            root: {
               margin: "4px 0",
               borderColor: malloyColors.grey[200],
            },
         },
         defaultProps: {
            sx: {
               my: 1,
            },
         },
      },
   },
});

export default theme;
