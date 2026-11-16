// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What `Publisher.embed` lets the frame it creates do, read from the frame the
 * real runtime builds on a host page. The same rules as the Console's viewer
 * (DataAppViewer): downloads, new tabs, the clipboard and full screen, and never anything
 * over the host.
 */
import { describe, expect, it } from "bun:test";
import fs from "fs";
import path from "path";
import { Window } from "happy-dom";

const RUNTIME = fs.readFileSync(path.join(__dirname, "publisher.js"), "utf8");

function embedFrame(options: Record<string, unknown> = {}) {
   const window = new Window({ url: "https://host.example.com/page" });
   window.document.body.innerHTML = '<div id="slot"></div>';
   // A host page, not a framed one: the runtime sees it is the top window and
   // installs only window.Publisher.
   new Function("window", "document", "location", "MutationObserver", RUNTIME)(
      window,
      window.document,
      window.location,
      window.MutationObserver,
   );
   const publisher = (
      window as unknown as {
         Publisher: {
            embed: (
               selector: string,
               options: Record<string, unknown>,
            ) => { iframe: HTMLIFrameElement };
         };
      }
   ).Publisher;
   const { iframe } = publisher.embed("#slot", {
      src: "https://data.example.com/environments/e/packages/p/index.html",
      ...options,
   });
   const sandbox = (iframe.getAttribute("sandbox") ?? "").split(/\s+/);
   const allow = (iframe.getAttribute("allow") ?? "")
      .split(";")
      .map((feature: string) => feature.trim())
      .filter(Boolean);
   void window.happyDOM.close();
   return { sandbox, allow };
}

describe("Publisher.embed frame permissions", () => {
   it("lets the page save a file, open a link in a new tab, copy and go full screen", () => {
      const { sandbox, allow } = embedFrame();
      expect(sandbox).toEqual(
         expect.arrayContaining(["allow-downloads", "allow-popups"]),
      );
      expect(allow).toEqual(["clipboard-write", "fullscreen"]);
   });

   it("keeps what the page needs to run, query as the reader and report its size", () => {
      expect(embedFrame().sandbox).toEqual(
         expect.arrayContaining([
            "allow-scripts",
            "allow-same-origin",
            "allow-forms",
         ]),
      );
   });

   it("grants nothing else: no top navigation, dialogs, clipboard reads, tabs that leave the sandbox or other features", () => {
      const { sandbox, allow } = embedFrame();
      expect([...sandbox].sort()).toEqual([
         "allow-downloads",
         "allow-forms",
         "allow-popups",
         "allow-same-origin",
         "allow-scripts",
      ]);
      expect([...allow].sort()).toEqual(["clipboard-write", "fullscreen"]);
   });

   it("takes the host's own allow, and an empty one turns the default off", () => {
      expect(embedFrame({ allow: "fullscreen" }).allow).toEqual(["fullscreen"]);
      expect(embedFrame({ allow: "" }).allow).toEqual([]);
   });
});
