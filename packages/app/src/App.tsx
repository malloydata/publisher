/* eslint-disable react/prop-types */
import {
   WorkbookStorage,
   WorkbookStorageProvider,
} from "@malloy-publisher/sdk";
import { ServerProvider } from "@malloy-publisher/sdk/client";
import "@malloy-publisher/sdk/styles.css";
import "@malloydata/malloy-explorer/styles.css";

import CssBaseline from "@mui/material/CssBaseline";
import { ThemeProvider } from "@mui/material/styles";

import { lazy, Suspense, useMemo } from "react";
import { createBrowserRouter, RouterProvider } from "react-router-dom";

import { Loading } from "./components/common/Loading/Loading";
import { HeaderProps } from "./components/layout/Header";
import theme from "./theme";
import { RouteError } from "./components/common/RouteError/RouteError";

const HomePage = lazy(() => import("./components/pages/HomePage/HomePage"));
const MainPage = lazy(() => import("./components/layout/MainPage/MainPage"));
const ModelPage = lazy(() => import("./components/pages/ModelPage/ModelPage"));
const PackagePage = lazy(
   () => import("./components/pages/PackagePage/PackagePage"),
);
const ProjectPage = lazy(
   () => import("./components/pages/ProjectPage/ProjectPage"),
);
const WorkbookPage = lazy(
   () => import("./components/pages/WorkbookPage/WorkbookPage"),
);

interface RootLayoutProps {
   workbookStorage: WorkbookStorage;
   headerProps?: HeaderProps;
}

const RootLayout: React.FC<RootLayoutProps> = ({
   workbookStorage,
   headerProps,
}) => {
   return (
      <ServerProvider>
         <WorkbookStorageProvider workbookStorage={workbookStorage}>
            <ThemeProvider theme={theme}>
               <CssBaseline />
               <Suspense fallback={<Loading text="Loading..." />}>
                  <MainPage headerProps={headerProps} />
               </Suspense>
            </ThemeProvider>
         </WorkbookStorageProvider>
      </ServerProvider>
   );
};

export const createMalloyRouter = (
   basePath: string = "/",
   workbookStorage: WorkbookStorage,
   headerProps?: HeaderProps,
) => {
   return createBrowserRouter([
      {
         path: basePath,
         element: (
            <RootLayout
               workbookStorage={workbookStorage}
               headerProps={headerProps}
            />
         ),
         errorElement: (
            <Suspense fallback={<Loading />}>
               <RouteError />
            </Suspense>
         ),
         children: [
            {
               index: true,
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
               errorElement: <RouteError />,
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
   basePath = "/",
   workbookStorage,
   headerProps,
}) => {
   const router = useMemo(
      () => createMalloyRouter(basePath, workbookStorage, headerProps),
      [basePath, workbookStorage, headerProps],
   );

   return <RouterProvider router={router} />;
};
