import {
   WorkbookStorage,
   WorkbookStorageProvider,
} from "@malloy-publisher/sdk";
import { ServerProvider } from "@malloy-publisher/sdk/client";
import "@malloy-publisher/sdk/styles.css";
import "@malloydata/malloy-explorer/styles.css";
import { CssBaseline } from "@mui/material";
import { ThemeProvider } from "@mui/material/styles";
import * as React from "react";
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import { HeaderProps } from "./components/Header";
import theme from "./theme";

/**
 * Vite automatically handles code splitting and chunking when using
 * React.lazy and dynamic import() statements for lazy loading React
 * components.
 */
const HomePage = React.lazy(() => import("./components/HomePage"));
const MainPage = React.lazy(() => import("./components/MainPage"));
const ModelPage = React.lazy(() => import("./components/ModelPage"));
const PackagePage = React.lazy(() => import("./components/PackagePage"));
const ProjectPage = React.lazy(() => import("./components/ProjectPage"));
const RouteError = React.lazy(() => import("./components/RouteError"));
const WorkbookPage = React.lazy(() => import("./components/WorkbookPage"));

// Create router configuration function
export const createMalloyRouter = (
   basePath: string = "/",
   workbookStorage: WorkbookStorage,
   headerProps?: HeaderProps,
) => {
   return createBrowserRouter([
      {
         path: basePath,
         element: (
            <ServerProvider>
               <WorkbookStorageProvider workbookStorage={workbookStorage}>
                  <ThemeProvider theme={theme}>
                     <CssBaseline />
                     <MainPage headerProps={headerProps} />
                  </ThemeProvider>
               </WorkbookStorageProvider>
            </ServerProvider>
         ),
         errorElement: <RouteError />,
         children: [
            {
               path: "",
               element: <HomePage />,
            },
            {
               path: ":projectName",
               element: <ProjectPage />,
            },
            {
               path: ":projectName/:packageName",
               element: <PackagePage />,
            },
            {
               path: ":projectName/:packageName/*",
               element: <ModelPage />,
            },
            {
               path: ":projectName/:packageName/workbook/:workspace/:workbookPath",
               element: <WorkbookPage />,
            },
         ],
      },
   ]);
};

export interface MalloyPublisherAppProps {
   basePath?: string;
   headerProps: HeaderProps;
   workbookStorage: WorkbookStorage;
}

export const MalloyPublisherApp: React.FC<MalloyPublisherAppProps> = ({
   workbookStorage,
   basePath = "/",
   headerProps,
}) => {
   const router = createMalloyRouter(basePath, workbookStorage, headerProps);
   return <RouterProvider router={router} />;
};
