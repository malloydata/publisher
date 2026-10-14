// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import { BuilderBar } from "./BuilderBar";
import type { BuilderControls } from "./builderControls";
import { ReportBuilder } from "./ReportBuilder";
import { openDocument } from "./testing/fixtures";

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

describe("a host that takes the controls later", () => {
   it("is handed them at once, not on the next change", async () => {
      const handed = mock((_controls: BuilderControls | undefined) => {});
      const document = await openDocument(GRID);
      const view = render(
         <ReportBuilder source={GRID} document={document} onSave={() => {}} />,
      );
      view.rerender(
         <ReportBuilder
            source={GRID}
            document={document}
            onSave={() => {}}
            onControlsChange={handed}
         />,
      );
      await waitFor(() => expect(handed).toHaveBeenCalled());
      expect(handed.mock.calls.at(-1)?.[0]?.status).toBe("saved");
   });
});

describe("BuilderBar's Done without a host Done", () => {
   it("leaves through the builder's own exit", () => {
      const exit = mock(() => {});
      render(
         <BuilderBar
            controls={{
               status: "saved",
               saveTarget: "",
               save: () => {},
               canUndo: false,
               undo: () => {},
               canRedo: false,
               redo: () => {},
               add: {},
               exit,
            }}
         />,
      );
      screen.getByRole("button", { name: "Done" }).click();
      expect(exit).toHaveBeenCalledTimes(1);
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
