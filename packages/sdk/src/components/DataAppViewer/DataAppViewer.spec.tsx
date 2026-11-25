// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a data app's frame may do in the Console: downloads, new tabs, the
 * clipboard and full screen, and never anything over the Console itself.
 */
import { expect, it } from "bun:test";
import { render } from "@testing-library/react";
import {
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";

mockServerProvider({ dataApps: { listDataApps: pending } });

const { default: DataAppViewer } = await import("./DataAppViewer");

function frame() {
   const { container } = render(
      <DataAppViewer resourceUri="publisher://environments/env/packages/pkg/models/index.html" />,
      { wrapper: serverWrapper },
   );
   const iframe = container.querySelector("iframe");
   expect(iframe).not.toBeNull();
   return {
      sandbox: (iframe!.getAttribute("sandbox") ?? "").split(/\s+/),
      allow: (iframe!.getAttribute("allow") ?? "")
         .split(";")
         .map((feature) => feature.trim()),
   };
}

it("lets the page save a file, copy text, show an element full screen and open a link in a new tab", () => {
   const { sandbox, allow } = frame();
   expect(sandbox).toEqual(
      expect.arrayContaining(["allow-downloads", "allow-popups"]),
   );
   expect(allow).toEqual(
      expect.arrayContaining(["clipboard-write", "fullscreen"]),
   );
});

it("keeps what the page needs to run, query as the reader and report its size", () => {
   expect(frame().sandbox).toEqual(
      expect.arrayContaining([
         "allow-scripts",
         "allow-same-origin",
         "allow-forms",
      ]),
   );
});

it("grants nothing else: no top navigation, dialogs, clipboard reads, tabs that leave the sandbox or other features", () => {
   const { sandbox, allow } = frame();
   expect([...sandbox].sort()).toEqual([
      "allow-downloads",
      "allow-forms",
      "allow-popups",
      "allow-same-origin",
      "allow-scripts",
   ]);
   expect([...allow].sort()).toEqual(["clipboard-write", "fullscreen"]);
});
