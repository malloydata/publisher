import { ServerProvider } from "@malloy-publisher/sdk/client";
import { CssBaseline } from "@mui/material";
import { ThemeProvider } from "@mui/material/styles";
import * as React from "react";
import * as ReactDOM from "react-dom/client";
import AppShell from "./AppShell";
import theme from "./theme";
// Import required CSS
import "@malloy-publisher/sdk/styles.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <ServerProvider>
        <AppShell />
      </ServerProvider>
    </ThemeProvider>
  </React.StrictMode>
);
