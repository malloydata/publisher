// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as React from "react";
import * as ReactDOM from "react-dom/client";
import { RouterProvider } from "react-router-dom";
import { createMalloyRouter } from "../../../src/App";
import {
   FakeAuthoritativeStorage,
   FakeScratchStorage,
   type FakeStorage,
} from "./fakeStorage";

// Test-only host: the Console mounted the way an embedder mounts it, over a
// store of its own. `?host=scratch` swaps the record for a non-authoritative
// store; the page exposes it as `window.__host` for specs to seed and inspect.
const host: FakeStorage =
   new URLSearchParams(window.location.search).get("host") === "scratch"
      ? new FakeScratchStorage()
      : new FakeAuthoritativeStorage();
// Documents a spec wants the host to already hold, set before the page loads
// so the first read finds them.
const seed = (window as unknown as { __seed?: Record<string, string> }).__seed;
for (const [documentPath, text] of Object.entries(seed ?? {}))
   host.documents.set(documentPath, text);
(window as unknown as { __host: FakeStorage }).__host = host;

ReactDOM.createRoot(document.getElementById("root")!).render(
   <RouterProvider router={createMalloyRouter("/", host)} />,
);
