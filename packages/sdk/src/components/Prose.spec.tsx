// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import { Prose } from "./Prose";

// Package content comes from untrusted git and S3 sources, so the one thing
// every surface that renders it has to agree on is what a link may do. These
// are that agreement, tested once against the one component that now renders
// prose everywhere; the notebook cell used to be the only surface that held
// to it.

describe("Prose: links", () => {
   it("refuses a script-bearing href, keeping the text", () => {
      render(
         <Prose>
            {"[run](javascript:alert(1)) and [d](data:text/html,x)"}
         </Prose>,
      );
      expect(screen.getByText("run").getAttribute("href")).toBeNull();
      expect(screen.getByText("d").getAttribute("href")).toBeNull();
   });

   it("opens an external link in a new tab, disowning the opener", () => {
      render(<Prose>{"[docs](https://example.com/docs)"}</Prose>);
      const a = screen.getByText("docs");
      expect(a.getAttribute("href")).toBe("https://example.com/docs");
      expect(a.getAttribute("target")).toBe("_blank");
      expect(a.getAttribute("rel")).toBe("noopener noreferrer");
   });

   it("resolves a relative link against the source file, within the package", () => {
      render(
         <Prose
            links={{
               environmentName: "env",
               packageName: "pkg",
               sourcePath: "guides/intro.malloynb",
            }}
         >
            {"[next](../sales.malloynb) [up](../../../etc/passwd)"}
         </Prose>,
      );
      expect(screen.getByText("next").getAttribute("href")).toBe(
         "/env/pkg/sales.malloynb",
      );
      // An escape past the package root is clamped at it, never above.
      expect(screen.getByText("up").getAttribute("href")).toBe(
         "/env/pkg/etc/passwd",
      );
   });

   it("routes a resolved link through the host when it can", () => {
      const onNavigate = mock((_to: string) => undefined);
      render(
         <Prose
            links={{ environmentName: "env", packageName: "pkg", onNavigate }}
         >
            {"[readme](README.md)"}
         </Prose>,
      );
      fireEvent.click(screen.getByText("readme"));
      expect(onNavigate).toHaveBeenCalledTimes(1);
      expect(onNavigate.mock.calls[0][0]).toBe("/env/pkg/README.md");
   });

   it("leaves a relative link alone when it has no package to resolve against", () => {
      render(<Prose>{"[here](./notes.md)"}</Prose>);
      expect(screen.getByText("here").getAttribute("href")).toBe("./notes.md");
   });
});

describe("Prose: variants", () => {
   it("renders headings as headings in both, so structure survives", () => {
      render(<Prose variant="document">{"# Title\n\nBody"}</Prose>);
      expect(screen.getByRole("heading", { level: 1 }).textContent).toBe(
         "Title",
      );
      render(<Prose variant="caption">{"## Section\n\nBody"}</Prose>);
      expect(screen.getByRole("heading", { level: 2 }).textContent).toBe(
         "Section",
      );
   });
});
