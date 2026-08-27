// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as React from "react";
import * as ReactDOM from "react-dom/client";
import { RouterProvider } from "react-router-dom";
import { createMalloyRouter } from "./App";
import { BrowserWorkbookStorage } from "@malloy-publisher/sdk";

// Main.tsx is used to run the app locally. This is not used when the app is
// embedded in another project.
const router = createMalloyRouter("/", new BrowserWorkbookStorage(), undefined);

ReactDOM.createRoot(document.getElementById("root")!).render(
   <React.StrictMode>
      <RouterProvider router={router} />
   </React.StrictMode>,
);
