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

   it("keeps one theme across renders for the same tokens, however they are passed", () => {
      const seen: { ui: unknown }[] = [];
      const Probe = () => {
         seen.push(usePublisherTheme().theme);
         return null;
      };
      // Stable, as ServerProvider's are: the layers are not what is tested.
      const layers: [] = [];
      const tree = (n: number) => (
         <ThemeProvider
            layers={layers}
            mode="light"
            userChoice={undefined}
            setMode={() => {}}
            // A new object each render, as an inline prop is.
            ui={{ radius: { tile: "0px" } }}
         >
            <Probe key={n} />
         </ThemeProvider>
      );
      const view = render(tree(1));
      view.rerender(tree(1));
      expect(seen.length).toBeGreaterThan(1);
      expect(seen.at(-1)?.ui).toBe(seen[0]?.ui);
      expect(seen.at(-1)).toBe(seen[0]);
   });
});
