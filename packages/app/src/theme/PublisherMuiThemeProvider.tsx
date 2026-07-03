import { usePublisherTheme } from "@malloy-publisher/sdk";
import CssBaseline from "@mui/material/CssBaseline";
import { ThemeProvider as MuiThemeProvider } from "@mui/material/styles";
import { useMemo, type ReactNode } from "react";
import { createPublisherTheme } from ".";

interface PublisherMuiThemeProviderProps {
   children: ReactNode;
}

/**
 * App-shell MUI ThemeProvider. Reads the active mode from the SDK's
 * `usePublisherTheme()` hook (the single source of truth, populated by
 * `ServerProvider` from localStorage + `prefers-color-scheme` + the
 * instance `defaultMode`) and rebuilds the MUI theme through
 * `createPublisherTheme(mode)` whenever the mode changes.
 *
 * Must be mounted INSIDE `ServerProvider` so the SDK ThemeProvider is
 * already in scope when this component reads the hook.
 */
export function PublisherMuiThemeProvider({
   children,
}: PublisherMuiThemeProviderProps) {
   const { mode } = usePublisherTheme();
   const muiTheme = useMemo(() => createPublisherTheme(mode), [mode]);
   return (
      <MuiThemeProvider theme={muiTheme}>
         <CssBaseline />
         {children}
      </MuiThemeProvider>
   );
}
