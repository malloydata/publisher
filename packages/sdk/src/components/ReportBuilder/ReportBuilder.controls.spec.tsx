// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   act,
   fireEvent,
   render,
   screen,
   waitFor,
   within,
} from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import { BuilderBar } from "./BuilderBar";
import type { BuilderControls } from "./builderControls";
import { ReportBuilder } from "./ReportBuilder";
import { openDocument } from "./testing/fixtures";
import { closeMenu } from "./testing/inline";

/**
 * A host that takes the builder's page-level controls: no toolbar on the page,
 * the controls handed over as state and stable actions, and drawn by the host
 * (here with the SDK's own BuilderBar).
 */
const GRID = `## artifact { layout=grid title="Storefront" tiles=["a -> by_cat"] } dashboard { columns=12 }
import "../data_app.malloy"

source: a is scoped_orders extend {
  # colspan=6
  # label="By category"
  view: by_cat is by_category
}`;

const mount = async (
   onControlsChange: (controls: BuilderControls | undefined) => void,
   onSave: (source: string) => Promise<void> | void = () => {},
) =>
   render(
      <ReportBuilder
         source={GRID}
         document={await openDocument(GRID)}
         onSave={onSave}
         onControlsChange={onControlsChange}
      />,
   );

describe("ReportBuilder: a host that takes the controls", () => {
   it("draws no toolbar of its own, and hands the controls over", async () => {
      const handed = mock((_controls: BuilderControls | undefined) => {});
      await mount(handed);
      expect(
         screen.queryByRole("group", { name: "Builder actions", hidden: true }),
      ).toBeNull();
      expect(screen.queryByRole("button", { name: "Undo" })).toBeNull();
      const controls = handed.mock.calls.at(-1)?.[0];
      expect(controls?.status).toBe("saved");
      expect(controls?.canUndo).toBe(false);
      expect(controls?.layout?.value).toBe("grid");
      expect(controls?.save).toBeDefined();
      expect(controls?.add.text).toBeDefined();
      expect(controls?.add.filter).toBeDefined();
      // No catalog was passed, so there is nothing to pick a tile from.
      expect(controls?.add.tile).toBeUndefined();
   });

   it("keeps its actions' identity, so a host can hold them, and they act on the latest edit", async () => {
      const handed = mock((_controls: BuilderControls | undefined) => {});
      const saved = mock((_source: string) => {});
      await mount(handed, saved);
      const first = handed.mock.calls.at(-1)?.[0];
      act(() => first?.layout?.set("linear"));
      await waitFor(() =>
         expect(handed.mock.calls.at(-1)?.[0]?.status).toBe("unsaved"),
      );
      const now = handed.mock.calls.at(-1)?.[0];
      expect(now?.layout?.value).toBe("linear");
      expect(now?.canUndo).toBe(true);
      expect(now?.save).toBe(first?.save);
      await act(async () => {
         first?.save?.();
      });
      await waitFor(() => expect(saved).toHaveBeenCalledTimes(1));
      expect(saved.mock.calls[0]?.[0]).toContain("layout=linear");
   });

   it("hands over undefined when the builder goes, so the host stops drawing them", async () => {
      const handed = mock((_controls: BuilderControls | undefined) => {});
      const view = await mount(handed);
      view.unmount();
      expect(handed.mock.calls.at(-1)?.[0]).toBeUndefined();
   });
});

describe("BuilderBar", () => {
   const base: BuilderControls = {
      status: "saved",
      saveTarget: "Saves to the package file",
      save: () => {},
      canUndo: false,
      undo: () => {},
      canRedo: false,
      redo: () => {},
      add: {},
   };

   it("says Saved and offers Done alone while every edit is saved", () => {
      render(<BuilderBar controls={base} onDone={() => {}} />);
      expect(screen.getByRole("status").textContent).toBe("Saved");
      expect(screen.getByRole("button", { name: "Done" })).toBeDefined();
      expect(screen.queryByRole("button", { name: "Save" })).toBeNull();
   });

   it("offers Save beside Done while there are unsaved edits", () => {
      const save = mock(() => {});
      render(
         <BuilderBar
            controls={{ ...base, status: "unsaved", save }}
            onDone={() => {}}
         />,
      );
      expect(screen.getByRole("status").textContent).toBe("Unsaved changes");
      screen.getByRole("button", { name: "Save" }).click();
      expect(save).toHaveBeenCalledTimes(1);
   });

   it("draws Done alone while the builder opens", () => {
      render(<BuilderBar controls={undefined} onDone={() => {}} />);
      expect(screen.getByRole("button", { name: "Done" })).toBeDefined();
      expect(screen.queryByRole("button", { name: "Undo" })).toBeNull();
   });
});

describe("the tile menu's width", () => {
   it("sets a tile's width on a grid from the menu, and writes it", async () => {
      const saved = mock((_source: string) => {});
      render(
         <ReportBuilder
            source={GRID}
            document={await openDocument(GRID)}
            onSave={saved}
         />,
      );
      fireEvent.click(screen.getByLabelText("Settings for By category"));
      fireEvent.click(
         screen.getByRole("menuitem", { name: /^Width/, hidden: true }),
      );
      const menu = screen.getByRole("menu", { name: "Width", hidden: true });
      const items = within(menu).getAllByRole("menuitemradio", {
         hidden: true,
      });
      expect(items.map((item) => item.textContent)).toEqual([
         "3 of 12 · A quarter",
         "4 of 12 · A third",
         "6 of 12 · Half",
         "8 of 12 · Two-thirds",
         "9 of 12 · Three-quarters",
         "12 of 12 · Full width",
      ]);
      expect(
         items.find((item) => item.getAttribute("aria-checked") === "true")
            ?.textContent,
      ).toBe("6 of 12 · Half");
      fireEvent.click(
         items.find((item) => item.textContent === "8 of 12 · Two-thirds")!,
      );
      closeMenu();
      fireEvent.click(
         screen.getByRole("button", { name: "Save", hidden: true }),
      );
      await waitFor(() => expect(saved).toHaveBeenCalledTimes(1));
      expect(saved.mock.calls[0]?.[0]).toContain("# colspan=8");
   });
});
