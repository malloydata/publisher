import { ThemeProvider } from "@mui/material/styles";
import {
  PaletteMode,
  createTheme,
  CssBaseline,
  Box,
  alpha,
} from "@mui/material";
import { useState, useMemo } from "react";
import AppNavbar from "./components/AppNavbar";
import SideMenu from "./components/SideMenu";
import MalloySamplesDashboard from "./components/MalloySamplesDashboard";
import SingleEmbedDashboard from "./components/SingleEmbedDashboard";
import DynamicDashboard from "./components/DynamicDashboard";
import InteractiveDashboard from "./components/InteractiveDashboard";

export default function AppShell() {
  const [mode, setMode] = useState<PaletteMode>("light");
  const defaultTheme = useMemo(
    () => createTheme({ palette: { mode } }),
    [mode]
  );
  const [selectedView, setSelectedView] = useState<
    "malloySamples" | "singleEmbed" | "dynamicDashboard" | "interactive"
  >("malloySamples");

  return (
    <ThemeProvider theme={defaultTheme}>
      <CssBaseline enableColorScheme />
      <Box sx={{ display: "flex" }}>
        <SideMenu
          selectedView={selectedView}
          setSelectedView={setSelectedView}
        />
        <AppNavbar
          selectedView={selectedView}
          setSelectedView={setSelectedView}
        />
        <Box
          component="main"
          sx={{
            flexGrow: 1,
            backgroundColor: (theme) =>
              alpha(theme.palette.background.default, 1),
            overflow: "auto",
            p: 3,
            position: "relative",
          }}
        >
          {
            <>
              {selectedView === "malloySamples" && (
                <MalloySamplesDashboard selectedView={selectedView} />
              )}
              {selectedView === "singleEmbed" && (
                <SingleEmbedDashboard selectedView={selectedView} />
              )}
              {selectedView === "dynamicDashboard" && (
                <DynamicDashboard
                  selectedView={selectedView}
                  resourceUri={`publisher://projects/malloy-samples`}
                />
              )}
              {selectedView === "interactive" && (
                <InteractiveDashboard selectedView={selectedView} />
              )}
            </>
          }
        </Box>
      </Box>
    </ThemeProvider>
  );
}
