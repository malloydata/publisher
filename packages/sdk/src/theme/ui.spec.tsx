// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { render } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import { ThemeProvider, usePublisherTheme } from "./ThemeContext";
import { DEFAULT_UI, mergeUi } from "./ui";

describe("mergeUi", () => {
   it("is the defaults when the host passes nothing", () => {
      expect(mergeUi(undefined)).toBe(DEFAULT_UI);
   });

   it("lays one field over the defaults, and leaves the rest", () => {
      const ui = mergeUi({
         radius: { control: "2px" },
         type: { control: { fontWeight: 600 } },
      });
      expect(ui.radius).toEqual({ ...DEFAULT_UI.radius, control: "2px" });
      expect(ui.type.control).toEqual({
         ...DEFAULT_UI.type.control,
         fontWeight: 600,
      });
      expect(ui.type.prose).toEqual(DEFAULT_UI.type.prose);
      expect(ui.size).toEqual(DEFAULT_UI.size);
   });

   it("sets Malloy names in the host's mono face", () => {
      const ui = mergeUi({ font: { mono: "Fira Code, monospace" } });
      expect(ui.type.code.fontFamily).toBe("Fira Code, monospace");
   });
});

describe("ThemeProvider's ui", () => {
   it("reaches every component through usePublisherTheme", () => {
      let seen: string | undefined;
      const Probe = () => {
         seen = usePublisherTheme().theme.ui.radius.tile;
         return null;
      };
      render(
         <ThemeProvider
            layers={[]}
            mode="light"
            userChoice={undefined}
            setMode={() => {}}
            ui={{ radius: { tile: "0px" } }}
         >
            <Probe />
         </ThemeProvider>,
      );
      expect(seen).toBe("0px");
   });

   it("is the defaults outside any provider", () => {
      let seen: string | undefined;
      const Probe = () => {
         seen = usePublisherTheme().theme.ui.radius.tile;
         return null;
      };
      render(<Probe />);
      expect(seen).toBe(DEFAULT_UI.radius.tile);
   });
});
