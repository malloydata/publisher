import {
   WorkbookStorage,
   WorkbookStorageProvider,
} from "@malloy-publisher/sdk";
import { ServerProvider } from "@malloy-publisher/sdk/client";
import "@malloy-publisher/sdk/styles.css";
import "@malloydata/malloy-explorer/styles.css";
import { CssBaseline } from "@mui/material";
import { ThemeProvider } from "@mui/material/styles";
import { lazy, Suspense, useMemo } from "react";
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import { Loading } from "./components/common/Loading/Loading";
import { HeaderProps } from "./components/layout/Header/Header";
import theme from "./theme";

/**
 * Vite automatically handles code splitting and chunking when using
 * React.lazy and dynamic import() statements for lazy loading React
 * components.
 */
const HomePage = lazy(() => import("./components/pages/HomePage/HomePage"));
const MainPage = lazy(() => import("./components/layout/MainPage/MainPage"));
const ModelPage = lazy(() => import("./components/pages/ModelPage/ModelPage"));
const PackagePage = lazy(
   () => import("./components/pages/PackagePage/PackagePage"),
);
const ProjectPage = lazy(
   () => import("./components/pages/ProjectPage/ProjectPage"),
);
const RouteError = lazy(() => import("./components/common/RouteError/RouteError"));
const WorkbookPage = lazy(
   () => import("./components/pages/WorkbookPage/WorkbookPage"),
);

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
               element: (
                  <Suspense fallback={<Loading />}>
                     <HomePage />
                  </Suspense>
               ),
            },
            {
               path: ":projectName",
               element: (
                  <Suspense fallback={<Loading />}>
                     <ProjectPage />
                  </Suspense>
               ),
            },
            {
               path: ":projectName/:packageName",
               element: (
                  <Suspense fallback={<Loading />}>
                     <PackagePage />
                  </Suspense>
               ),
            },
            {
               path: ":projectName/:packageName/*",
               element: (
                  <Suspense fallback={<Loading />}>
                     <ModelPage />
                  </Suspense>
               ),
            },
            {
               path: ":projectName/:packageName/workbook/:workspace/:workbookPath",
               element: (
                  <Suspense fallback={<Loading />}>
                     <WorkbookPage />
                  </Suspense>
               ),
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
