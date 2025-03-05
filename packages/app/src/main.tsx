import * as React from "react";
import * as ReactDOM from "react-dom/client";
import { ThemeProvider } from "@mui/material/styles";
import { CssBaseline } from "@mui/material";
import theme from "./theme";
import MainPage from "./components/MainPage";
import { RouteError } from "./components/RouteError";
import { PackagePage } from "./components/PackagePage";
import { ProjectPage } from "./components/ProjectPage";
import { ModelPage } from "./components/ModelPage";
import { createBrowserRouter, RouterProvider, Navigate } from "react-router-dom";

const server = `${window.location.protocol}//${window.location.host}/api/v0`;
const router = createBrowserRouter([
   {
      path: "/",
      element: (
         <ThemeProvider theme={theme}>
            <CssBaseline />
            <MainPage />
         </ThemeProvider>
      ),
      errorElement: <RouteError />,

      children: [
         {
            path: "",
            element: <Navigate to="/home/" />,
         },
         {
            path: ":projectName",
            element: <ProjectPage server={server} />,
         },
         {
            path: ":projectName/:packageName",
            element: <PackagePage server={server} />,
         },
         {
            path: ":projectName/:packageName/*",
            element: <ModelPage server={server} />,
         },
      ],
   },
]);

ReactDOM.createRoot(document.getElementById("root")!).render(
   <React.StrictMode>
      <RouterProvider router={router} />
   </React.StrictMode>,
);
