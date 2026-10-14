// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   act,
   fireEvent,
   render,
   screen,
   waitFor,
} from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import { BuilderBar } from "./BuilderBar";
import type { BuilderControls } from "./builderControls";
import { ReportBuilder } from "./ReportBuilder";
import { draftStore } from "./useReportDraft";
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

   it("keeps its spoken status to a 1px box, so it never widens the host", () => {
      render(<BuilderBar controls={base} onDone={() => {}} />);
      const status = getComputedStyle(screen.getByRole("status"));
      expect(status.position).toBe("absolute");
      expect(status.width).toBe("1px");
      expect(status.height).toBe("1px");
      expect(status.margin).toBe("-1px");
   });

   it("draws Done alone while the builder opens", () => {
      render(<BuilderBar controls={undefined} onDone={() => {}} />);
      expect(screen.getByRole("button", { name: "Done" })).toBeDefined();
      expect(screen.queryByRole("button", { name: "Undo" })).toBeNull();
   });
});

describe("Duplicate, from a tile's menu", () => {
   it("writes a copy as a view of its own, right after the original", async () => {
      const saved = mock((_source: string) => {});
      render(
         <ReportBuilder
            source={GRID}
            document={await openDocument(GRID)}
            onSave={saved}
         />,
      );
      screen.getByLabelText("Settings for By category").click();
      await waitFor(() =>
         screen.getByRole("menuitem", { name: "Duplicate", hidden: true }),
      );
      screen.getByRole("menuitem", { name: "Duplicate", hidden: true }).click();
      await waitFor(() =>
         expect(screen.getAllByText("By category")).toHaveLength(2),
      );
      screen.getByRole("button", { name: "Save", hidden: true }).click();
      await waitFor(() => expect(saved).toHaveBeenCalledTimes(1));
      const text = saved.mock.calls[0]?.[0] ?? "";
      expect(text).toContain('tiles=["a -> by_cat", "a -> by_category_tile"]');
      expect(text).toMatch(
         /# colspan=6\n\s*# label="By category"\n\s*view: by_category_tile is by_category/,
      );
   });

   it("selects the copy", async () => {
      render(
         <ReportBuilder
            source={GRID}
            document={await openDocument(GRID)}
            onSave={() => {}}
         />,
      );
      screen.getByLabelText("Settings for By category").click();
      await waitFor(() =>
         screen.getByRole("menuitem", { name: "Duplicate", hidden: true }),
      );
      screen.getByRole("menuitem", { name: "Duplicate", hidden: true }).click();
      await waitFor(() =>
         expect(
            document
               .querySelector('[aria-label="Tile by_category_tile"]')
               ?.getAttribute("aria-current"),
         ).toBe("true"),
      );
   });
});

describe("a draft kept in this browser, and Done", () => {
   const KEY = "examples/storefront/spec-draft";
   const stored = () =>
      window.localStorage.getItem(`malloy-publisher.report-draft:${KEY}`);
   const mountWithDraft = async (
      onExit: () => void = () => {},
      onSave: (source: string) => Promise<void> | void = () => {},
   ) => {
      const handed = mock((_controls: BuilderControls | undefined) => {});
      const view = render(
         <ReportBuilder
            source={GRID}
            document={await openDocument(GRID)}
            onSave={onSave}
            onExit={onExit}
            draftKey={KEY}
            onControlsChange={handed}
         />,
      );
      return { view, last: () => handed.mock.calls.at(-1)?.[0] };
   };

   it("keeps an edit, puts it back on the next open, and says so", async () => {
      window.localStorage.removeItem(`malloy-publisher.report-draft:${KEY}`);
      const first = await mountWithDraft();
      act(() => first.last()?.layout?.set("linear"));
      await waitFor(() => expect(stored()).toContain('"layout":"linear"'));
      expect(first.last()?.draftKeptAt).toBeNumber();
      first.view.unmount();

      const second = await mountWithDraft();
      await waitFor(() => expect(second.last()?.layout?.value).toBe("linear"));
      expect(second.last()?.status).toBe("unsaved");
      expect(
         screen.getByText(/Restored your unsaved edits from/),
      ).toBeDefined();
      // Undo takes the restore back.
      act(() => second.last()?.undo());
      await waitFor(() => expect(second.last()?.layout?.value).toBe("grid"));
      await waitFor(() => expect(stored()).toBeNull());
   });

   it("Discard puts the saved report back and drops the draft", async () => {
      window.localStorage.removeItem(`malloy-publisher.report-draft:${KEY}`);
      const { last } = await mountWithDraft();
      act(() => last()?.layout?.set("linear"));
      await waitFor(() => expect(last()?.discard).toBeDefined());
      act(() => last()?.discard?.());
      await waitFor(() => expect(last()?.layout?.value).toBe("grid"));
      await waitFor(() => expect(stored()).toBeNull());
   });

   it("Done writes the edits, then leaves, and the draft goes", async () => {
      window.localStorage.removeItem(`malloy-publisher.report-draft:${KEY}`);
      const exited = mock(() => {});
      const saved = mock((_source: string) => {});
      const { last } = await mountWithDraft(exited, saved);
      act(() => last()?.layout?.set("linear"));
      await waitFor(() => expect(last()?.status).toBe("unsaved"));
      await act(async () => {
         last()?.done?.();
      });
      await waitFor(() => expect(exited).toHaveBeenCalledTimes(1));
      expect(saved.mock.calls[0]?.[0]).toContain("layout=linear");
      await waitFor(() => expect(stored()).toBeNull());
   });

   it("Done stays when the write is refused", async () => {
      window.localStorage.removeItem(`malloy-publisher.report-draft:${KEY}`);
      const exited = mock(() => {});
      const { last } = await mountWithDraft(exited, () =>
         Promise.reject(new Error("The file changed underneath")),
      );
      act(() => last()?.layout?.set("linear"));
      await waitFor(() => expect(last()?.status).toBe("unsaved"));
      await act(async () => {
         last()?.done?.();
      });
      await waitFor(() => expect(last()?.status).toBe("unsaved"));
      expect(exited).not.toHaveBeenCalled();
      expect(stored()).toContain('"layout":"linear"');
   });
});

describe("a draft made before the report changed", () => {
   const KEY = "examples/storefront/spec-stale";
   const slot = `malloy-publisher.report-draft:${KEY}`;
   const keepStale = async () => {
      const opened = await openDocument(GRID);
      window.localStorage.setItem(
         slot,
         JSON.stringify({
            v: 1,
            // Made over another file: the fingerprint does not match.
            base: "0:other",
            savedAt: Date.now(),
            document: { ...opened, layout: "linear" },
         }),
      );
   };
   const mountIt = async () => {
      const handed = mock((_controls: BuilderControls | undefined) => {});
      render(
         <ReportBuilder
            source={GRID}
            document={await openDocument(GRID)}
            onSave={() => {}}
            onExit={() => {}}
            draftKey={KEY}
            onControlsChange={handed}
         />,
      );
      return () => handed.mock.calls.at(-1)?.[0];
   };

   it("is offered, not applied, and stays until the author chooses", async () => {
      await keepStale();
      const last = await mountIt();
      expect(
         await screen.findByText(/made before this report changed/),
      ).toBeDefined();
      expect(last()?.layout?.value).toBe("grid");
      expect(last()?.status).toBe("saved");
      expect(window.localStorage.getItem(slot)).not.toBeNull();
      fireEvent.click(screen.getByRole("button", { name: "Restore them" }));
      await waitFor(() => expect(last()?.layout?.value).toBe("linear"));
   });

   it("Discard drops it without touching the report", async () => {
      await keepStale();
      const last = await mountIt();
      await screen.findByText(/made before this report changed/);
      fireEvent.click(screen.getByRole("button", { name: "Discard" }));
      expect(window.localStorage.getItem(slot)).toBeNull();
      expect(last()?.layout?.value).toBe("grid");
   });
});

describe("a draft the browser will not keep", () => {
   it("does not say Draft saved", async () => {
      const KEY = "examples/storefront/spec-full";
      const real = draftStore.storage;
      const full = window.localStorage;
      draftStore.storage = () =>
         ({
            getItem: (k: string) => full.getItem(k),
            removeItem: (k: string) => full.removeItem(k),
            setItem: () => {
               throw new Error("QuotaExceededError");
            },
         }) as unknown as Storage;
      try {
         const handed = mock((_controls: BuilderControls | undefined) => {});
         render(
            <ReportBuilder
               source={GRID}
               document={await openDocument(GRID)}
               onSave={() => {}}
               onExit={() => {}}
               draftKey={KEY}
               onControlsChange={handed}
            />,
         );
         act(() => handed.mock.calls.at(-1)?.[0]?.layout?.set("linear"));
         await waitFor(() =>
            expect(handed.mock.calls.at(-1)?.[0]?.status).toBe("unsaved"),
         );
         expect(handed.mock.calls.at(-1)?.[0]?.draftKeptAt).toBeUndefined();
      } finally {
         draftStore.storage = real;
      }
   });
});

describe("BuilderBar with Done that saves", () => {
   const base: BuilderControls = {
      status: "unsaved",
      saveTarget: "Saves to the package file",
      save: () => {},
      canUndo: true,
      undo: () => {},
      canRedo: false,
      redo: () => {},
      add: {},
   };

   it("offers Done alone, says the draft is kept, and Done calls done", () => {
      const done = mock(() => {});
      render(
         <BuilderBar
            controls={{ ...base, done, draftKeptAt: Date.now() }}
            onDone={() => {}}
         />,
      );
      expect(screen.getByRole("status").textContent).toBe("Draft saved");
      expect(screen.queryByRole("button", { name: "Save" })).toBeNull();
      screen.getByRole("button", { name: "Done" }).click();
      expect(done).toHaveBeenCalledTimes(1);
   });

   it("offers Discard edits from the status", async () => {
      const discard = mock(() => {});
      render(
         <BuilderBar
            controls={{
               ...base,
               done: () => {},
               draftKeptAt: Date.now(),
               discard,
            }}
         />,
      );
      screen.getByRole("button", { name: /Draft saved/ }).click();
      await waitFor(() =>
         screen.getByRole("menuitem", { name: /Discard edits/ }),
      );
      screen.getByRole("menuitem", { name: /Discard edits/ }).click();
      expect(discard).toHaveBeenCalledTimes(1);
   });
});

describe("selection", () => {
   it("clears when the reader presses anywhere off the selected tile", async () => {
      render(
         <ReportBuilder
            source={GRID}
            document={await openDocument(GRID)}
            onSave={() => {}}
         />,
      );
      const tile = document.querySelector<HTMLElement>(
         '[data-tile-key="a.by_cat"]',
      )!;
      fireEvent.pointerDown(tile);
      fireEvent.click(tile);
      await waitFor(() =>
         expect(
            document
               .querySelector('[aria-label="Tile by_cat"]')
               ?.getAttribute("aria-current"),
         ).toBe("true"),
      );
      // A press inside the tile keeps it.
      fireEvent.pointerDown(tile);
      expect(
         document
            .querySelector('[aria-label="Tile by_cat"]')
            ?.getAttribute("aria-current"),
      ).toBe("true");
      fireEvent.pointerDown(document.body);
      await waitFor(() =>
         expect(
            document
               .querySelector('[aria-label="Tile by_cat"]')
               ?.getAttribute("aria-current"),
         ).toBe("false"),
      );
   });
});
