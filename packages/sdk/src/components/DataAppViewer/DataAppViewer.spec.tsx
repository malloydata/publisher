// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createTheme, ThemeProvider } from "@mui/material";
import { act, fireEvent, render } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import type { ReactNode } from "react";
import {
   clearCache,
   mockServerProvider,
   serverWrapper,
} from "../../../test/serverProvider";
import {
   PUBLISHER_COLOR_SCHEME_APPLIED_MESSAGE_TYPE,
   PUBLISHER_COLOR_SCHEME_MESSAGE_TYPE,
   PUBLISHER_COLOR_SCHEME_REQUEST_MESSAGE_TYPE,
} from "../../utils/dataAppEmbed";

mockServerProvider({
   dataApps: { listDataApps: async () => ({ data: [] }) },
});
const { default: DataAppViewer } = await import("./DataAppViewer");

const RESOURCE = "publisher://environments/env/packages/pkg/models/index.html";
const FRAME_ORIGIN = "http://localhost";

function mount(mode: "light" | "dark") {
   const wrapper = ({ children }: { children: ReactNode }) =>
      serverWrapper({
         children: (
            <ThemeProvider theme={createTheme({ palette: { mode } })}>
               {children}
            </ThemeProvider>
         ),
      });
   const view = render(<DataAppViewer resourceUri={RESOURCE} />, { wrapper });
   const iframe = view.container.querySelector("iframe") as HTMLIFrameElement;
   // happy-dom gives the frame no window of its own: a stand-in that records
   // what the viewer posts, and is the `source` of what the page posts back.
   const posted: unknown[] = [];
   const frameWindow = {
      postMessage: mock((data: unknown, origin: string) =>
         posted.push({ data, origin }),
      ),
   };
   Object.defineProperty(iframe, "contentWindow", {
      configurable: true,
      get: () => frameWindow,
   });
   const fromPage = (data: unknown, origin = FRAME_ORIGIN) =>
      act(() => {
         window.dispatchEvent(
            new MessageEvent("message", {
               data,
               origin,
               source: frameWindow as unknown as Window,
            }),
         );
      });
   return { iframe, posted, fromPage };
}

describe("DataAppViewer color scheme", () => {
   beforeEach(clearCache);

   it("sends the console's mode to the page's own origin when the frame loads", () => {
      const { iframe, posted } = mount("dark");
      fireEvent.load(iframe);
      expect(posted).toContainEqual({
         data: { type: PUBLISHER_COLOR_SCHEME_MESSAGE_TYPE, mode: "dark" },
         origin: FRAME_ORIGIN,
      });
   });

   it("answers the page's request with the mode", () => {
      const { posted, fromPage } = mount("light");
      fromPage({ type: PUBLISHER_COLOR_SCHEME_REQUEST_MESSAGE_TYPE });
      expect(posted).toContainEqual({
         data: { type: PUBLISHER_COLOR_SCHEME_MESSAGE_TYPE, mode: "light" },
         origin: FRAME_ORIGIN,
      });
   });

   it("gives the frame the page's colour scheme once the page says it drew it", () => {
      const { iframe, fromPage } = mount("dark");
      expect(iframe.style.colorScheme).toBe("");
      fromPage({
         type: PUBLISHER_COLOR_SCHEME_APPLIED_MESSAGE_TYPE,
         mode: "dark",
      });
      expect(iframe.style.colorScheme).toBe("dark");
   });

   it("ignores an answer from another origin, and forgets the answer when a new page loads", () => {
      const { iframe, fromPage } = mount("dark");
      fromPage(
         { type: PUBLISHER_COLOR_SCHEME_APPLIED_MESSAGE_TYPE, mode: "dark" },
         "https://elsewhere.example",
      );
      expect(iframe.style.colorScheme).toBe("");
      fromPage({
         type: PUBLISHER_COLOR_SCHEME_APPLIED_MESSAGE_TYPE,
         mode: "dark",
      });
      expect(iframe.style.colorScheme).toBe("dark");
      act(() => {
         fireEvent.load(iframe);
      });
      expect(iframe.style.colorScheme).toBe("");
   });
});
