// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   BrowserDocumentStorage,
   DocumentStorage,
   DocumentStorageProvider,
   Loading,
   setConsoleEventHandler,
} from "@malloy-publisher/sdk";
import { ServerProvider } from "@malloy-publisher/sdk/client";
import "@malloy-publisher/sdk/styles.css";
import "@malloydata/malloy-explorer/styles.css";
import * as React from "react";
import { Suspense, useMemo } from "react";
import {
   createBrowserRouter,
   Navigate,
   RouterProvider,
} from "react-router-dom";
import { HeaderProps } from "./components/layout/Header/Header";
import { logConsoleEvent } from "./utils/consoleTelemetry";
import { PublisherMuiThemeProvider } from "./theme/PublisherMuiThemeProvider";

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
const ThemeEditorPage = React.lazy(
   () => import("./components/pages/ThemeEditorPage/ThemeEditorPage"),
);

/**
 * @param documentStorage Where documents authored in the Console are kept. A
 *    host with a store of its own passes an implementation; left out, the
 *    Console keeps them in this browser's localStorage.
 */
export const createMalloyRouter = (
   basePath: string = "/",
   documentStorage: DocumentStorage = new BrowserDocumentStorage(),
   headerProps?: HeaderProps,
) => {
   // Here rather than in `main.tsx`, which is only the local dev entry: this
   // is the one function every host calls, embedders included, so the writes
   // are reported wherever the Console is mounted.
   setConsoleEventHandler(logConsoleEvent);
   return createBrowserRouter([
      {
         path: basePath,
         element: (
            <ServerProvider>
               <DocumentStorageProvider documentStorage={documentStorage}>
                  <PublisherMuiThemeProvider>
                     <Suspense fallback={<Loading />}>
                        <MainPage headerProps={headerProps} />
                     </Suspense>
                  </PublisherMuiThemeProvider>
               </DocumentStorageProvider>
            </ServerProvider>
         ),
         errorElement: <RouteError />,
         children: [
            {
               index: true,
               element: <HomePage />,
            },
            {
               // Literal-prefix route, must come before the catch-all
               // :environmentName segment so "settings" isn't read as an
               // environment name.
               path: "settings/theme",
               element: <ThemeEditorPage />,
            },
            {
               // Bare /settings has no page of its own; redirect to the
               // theme editor instead of letting the URL fall through to
               // the :environmentName loader and 404.
               path: "settings",
               element: <Navigate to="/settings/theme" replace />,
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
         ],
      },
   ]);
};

export interface MalloyPublisherAppProps {
   basePath?: string;
   headerProps: HeaderProps;
   /** See {@link createMalloyRouter}. Defaults to this browser's localStorage. */
   documentStorage?: DocumentStorage;
}

export const MalloyPublisherApp = ({
   basePath = "/",
   documentStorage,
   headerProps,
}: MalloyPublisherAppProps) => {
   const router = useMemo(
      () => createMalloyRouter(basePath, documentStorage, headerProps),
      [basePath, documentStorage, headerProps],
   );

   return <RouterProvider router={router} />;
};
