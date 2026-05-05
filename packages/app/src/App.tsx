import {
   Loading,
   WorkbookStorage,
   WorkbookStorageProvider,
} from "@malloy-publisher/sdk";
import { ServerProvider } from "@malloy-publisher/sdk/client";
import "@malloy-publisher/sdk/styles.css";
import "@malloydata/malloy-explorer/styles.css";
import CssBaseline from "@mui/material/CssBaseline";
import { ThemeProvider } from "@mui/material/styles";
import * as React from "react";
import { Suspense, useMemo } from "react";
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import { HeaderProps } from "./components/layout/Header/Header";
import theme from "./theme";

/**
 * Vite automatically handles code splitting and chunking when using
 * React.lazy and dynamic import() statements for lazy loading React
 * components.
 */
const HomePage = React.lazy(
   () => import("./components/pages/HomePage/HomePage"),
);
const MainPage = React.lazy(
   () => import("./components/layout/MainPage/MainPage"),
);
const ModelPage = React.lazy(
   () => import("./components/pages/ModelPage/ModelPage"),
);
const PackagePage = React.lazy(
   () => import("./components/pages/PackagePage/PackagePage"),
);
const EnvironmentPage = React.lazy(
   () => import("./components/pages/EnvironmentPage/EnvironmentPage"),
);
const RouteError = React.lazy(
   () => import("./components/common/RouteError/RouteError"),
);
const WorkbookPage = React.lazy(
   () => import("./components/pages/WorkbookPage/WorkbookPage"),
);

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
                     <Suspense fallback={<Loading />}>
                        <MainPage headerProps={headerProps} />
                     </Suspense>
                  </ThemeProvider>
               </WorkbookStorageProvider>
            </ServerProvider>
         ),
         errorElement: <RouteError />,
         children: [
            {
               index: true,
               element: <HomePage />,
            },
            {
               path: ":environmentName",
               element: <EnvironmentPage />,
            },
            {
               path: ":environmentName/:packageName",
               element: <PackagePage />,
            },
            {
               path: ":environmentName/:packageName/*",
               element: <ModelPage />,
            },
            {
               path: ":environmentName/:packageName/workbook/:workspace/:workbookPath",
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

export const MalloyPublisherApp = ({
   basePath = "/",
   workbookStorage,
   headerProps,
}: MalloyPublisherAppProps) => {
   const router = useMemo(
      () => createMalloyRouter(basePath, workbookStorage, headerProps),
      [basePath, workbookStorage, headerProps],
   );

   return <RouterProvider router={router} />;
};
