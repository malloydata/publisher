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
import { draftStore, textFingerprint } from "./useReportDraft";
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

/** Whether the text tile the controls' `add.text` appends is on the page. */
const textTileShown = () =>
   screen.queryByRole("group", { name: "Tile text_1", hidden: true }) !== null;

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
      act(() => first?.add.text?.());
      await waitFor(() =>
         expect(handed.mock.calls.at(-1)?.[0]?.status).toBe("unsaved"),
      );
      const now = handed.mock.calls.at(-1)?.[0];
      expect(textTileShown()).toBe(true);
      expect(now?.canUndo).toBe(true);
      expect(now?.save).toBe(first?.save);
      await act(async () => {
         first?.save?.();
      });
      await waitFor(() => expect(saved).toHaveBeenCalledTimes(1));
      expect(saved.mock.calls[0]?.[0]).toContain("##|(markdown) text_1");
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
   it("writes a copy as a query of the report's own, right after the original", async () => {
      const saved = mock((_source: string) => {});
      render(
         <ReportBuilder
            source={GRID}
            document={await openDocument(GRID)}
            onSave={saved}
         />,
      );
      screen.getAllByLabelText("Settings for By category")[0].click();
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
      expect(text).toContain(
         'tiles=["a -> by_cat", "by_category_tile" { colspan=6 }]',
      );
      expect(text).toContain(
         '\n\n# label="By category"\nquery: by_category_tile is a -> by_category\n',
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
      screen.getAllByLabelText("Settings for By category")[0].click();
      await waitFor(() =>
         screen.getByRole("menuitem", { name: "Duplicate", hidden: true }),
      );
      screen.getByRole("menuitem", { name: "Duplicate", hidden: true }).click();
      await waitFor(() =>
         expect(
            document
               .querySelector('[data-tile-key="query.by_category_tile"]')
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
      act(() => first.last()?.add.text?.());
      await waitFor(() => expect(stored()).toContain('"name":"text_1"'));
      expect(first.last()?.draftKeptAt).toBeNumber();
      first.view.unmount();

      const second = await mountWithDraft();
      await waitFor(() => expect(textTileShown()).toBe(true));
      expect(second.last()?.status).toBe("unsaved");
      // Said in the status, not in a banner over the page.
      expect(second.last()?.restored).toMatchObject({
         stale: false,
         pending: false,
      });
      expect(screen.queryByRole("alert")).toBeNull();
      expect(screen.queryByText(/Restored your unsaved edits/)).toBeNull();
      // Undo takes the restore back.
      act(() => second.last()?.undo());
      await waitFor(() => expect(textTileShown()).toBe(false));
      await waitFor(() => expect(stored()).toBeNull());
   });

   it("Discard puts the saved report back and drops the draft", async () => {
      window.localStorage.removeItem(`malloy-publisher.report-draft:${KEY}`);
      const { last } = await mountWithDraft();
      act(() => last()?.add.text?.());
      await waitFor(() => expect(last()?.discard).toBeDefined());
      act(() => last()?.discard?.());
      await waitFor(() => expect(textTileShown()).toBe(false));
      await waitFor(() => expect(stored()).toBeNull());
   });

   it("Done writes the edits, then leaves, and the draft goes", async () => {
      window.localStorage.removeItem(`malloy-publisher.report-draft:${KEY}`);
      const exited = mock(() => {});
      const saved = mock((_source: string) => {});
      const { last } = await mountWithDraft(exited, saved);
      act(() => last()?.add.text?.());
      await waitFor(() => expect(last()?.status).toBe("unsaved"));
      await act(async () => {
         last()?.done?.();
      });
      await waitFor(() => expect(exited).toHaveBeenCalledTimes(1));
      expect(saved.mock.calls[0]?.[0]).toContain("##|(markdown) text_1");
      await waitFor(() => expect(stored()).toBeNull());
   });

   it("Done stays when the write is refused", async () => {
      window.localStorage.removeItem(`malloy-publisher.report-draft:${KEY}`);
      const exited = mock(() => {});
      const { last } = await mountWithDraft(exited, () =>
         Promise.reject(new Error("The file changed underneath")),
      );
      act(() => last()?.add.text?.());
      await waitFor(() => expect(last()?.status).toBe("unsaved"));
      await act(async () => {
         last()?.done?.();
      });
      await waitFor(() => expect(last()?.status).toBe("unsaved"));
      expect(exited).not.toHaveBeenCalled();
      expect(stored()).toContain('"name":"text_1"');
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
            document: {
               ...opened,
               tiles: [
                  ...opened.tiles,
                  { kind: "text", name: "text_1", markdown: "", colspan: 12 },
               ],
            },
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
      expect(textTileShown()).toBe(false);
      expect(last()?.status).toBe("saved");
      expect(window.localStorage.getItem(slot)).not.toBeNull();
      // A choice the author has to make: no close, and both answers handed
      // to a host drawing its own status.
      expect(screen.queryByRole("button", { name: /close/i })).toBeNull();
      expect(last()?.restored?.pending).toBe(true);
      expect(last()?.restore).toBeDefined();
      expect(last()?.discard).toBeDefined();
      expect(last()?.dismissRestored).toBeUndefined();
      fireEvent.click(screen.getByRole("button", { name: "Restore them" }));
      await waitFor(() => expect(textTileShown()).toBe(true));
      expect(screen.queryByText(/made before this report changed/)).toBeNull();
   });

   it("is answered by a host's restore as well as the page's button", async () => {
      await keepStale();
      const last = await mountIt();
      await screen.findByText(/made before this report changed/);
      act(() => last()?.restore?.());
      await waitFor(() => expect(textTileShown()).toBe(true));
      expect(last()?.restored?.pending).toBe(false);
      expect(last()?.restore).toBeUndefined();
   });

   it("goes when the author edits over it instead, since the edits replace it", async () => {
      await keepStale();
      const last = await mountIt();
      await screen.findByText(/made before this report changed/);
      act(() => last()?.add.text?.());
      await waitFor(() =>
         expect(
            screen.queryByText(/made before this report changed/),
         ).toBeNull(),
      );
      expect(last()?.restored).toBeUndefined();
      // What is kept now is the edit, made over the current file.
      expect(window.localStorage.getItem(slot)).not.toContain(
         '"base":"0:other"',
      );
   });

   it("Discard drops it without touching the report", async () => {
      await keepStale();
      await mountIt();
      await screen.findByText(/made before this report changed/);
      fireEvent.click(screen.getByRole("button", { name: "Discard" }));
      expect(window.localStorage.getItem(slot)).toBeNull();
      expect(textTileShown()).toBe(false);
   });
});

describe("edits put back on opening", () => {
   const KEY = "examples/storefront/spec-restored";
   const slot = `malloy-publisher.report-draft:${KEY}`;
   /** A draft kept over this very file, so it is put back, not offered. */
   const keepFresh = async () => {
      const opened = await openDocument(GRID);
      window.localStorage.setItem(
         slot,
         JSON.stringify({
            v: 1,
            base: textFingerprint(GRID),
            savedAt: Date.now(),
            document: { ...opened, title: "Kept title" },
         }),
      );
   };

   it("in a host's bar: no banner, Draft saved, and the menu says they were restored until it is seen", async () => {
      await keepFresh();
      const handed = mock((_controls: BuilderControls | undefined) => {});
      const page = render(
         <ReportBuilder
            source={GRID}
            document={await openDocument(GRID)}
            onSave={() => {}}
            onExit={() => {}}
            draftKey={KEY}
            onControlsChange={handed}
         />,
      );
      const last = () => handed.mock.calls.at(-1)?.[0];
      await waitFor(() => expect(last()?.draftKeptAt).toBeNumber());
      expect(screen.queryByRole("alert")).toBeNull();
      expect(last()?.restored?.pending).toBe(false);
      expect(last()?.dismissRestored).toBeDefined();
      expect(last()?.restore).toBeUndefined();

      // The host draws the SDK's bar with what it was handed.
      const bar = render(<BuilderBar controls={last()} />);
      const chip = bar.getByRole("button", { name: "Draft saved" });
      expect(chip.querySelector('[data-restored="true"]')).not.toBeNull();
      fireEvent.click(chip);
      within(await bar.findByRole("menu")).getByText(
         /Restored your unsaved edits from/,
      );
      bar.getByRole("menuitem", { name: /Discard edits/ });
      fireEvent.keyDown(bar.getByRole("menu"), { key: "Escape" });
      await waitFor(() => expect(last()?.restored).toBeUndefined());
      bar.unmount();
      page.unmount();
   });

   it("in the page's own toolbar: no banner, and the status says it", async () => {
      await keepFresh();
      render(
         <ReportBuilder
            source={GRID}
            document={await openDocument(GRID)}
            onSave={() => {}}
            draftKey={KEY}
         />,
      );
      const chip = await screen.findByRole("button", { name: "Draft saved" });
      expect(screen.queryByRole("alert")).toBeNull();
      fireEvent.click(chip);
      within(await screen.findByRole("menu")).getByText(
         /Restored your unsaved edits from/,
      );
      fireEvent.click(screen.getByRole("menuitem", { name: /Discard edits/ }));
      await waitFor(() => expect(window.localStorage.getItem(slot)).toBeNull());
   });

   it("is old news after the next edit", async () => {
      await keepFresh();
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
      const last = () => handed.mock.calls.at(-1)?.[0];
      await waitFor(() => expect(last()?.restored).toBeDefined());
      act(() => last()?.add.text?.());
      await waitFor(() => expect(last()?.restored).toBeUndefined());
      expect(last()?.status).toBe("unsaved");
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
         act(() => handed.mock.calls.at(-1)?.[0]?.add.text?.());
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

   it("says edits were restored in its menu, and hands back that they were seen", async () => {
      const dismissRestored = mock(() => {});
      render(
         <BuilderBar
            controls={{
               ...base,
               done: () => {},
               draftKeptAt: Date.now(),
               discard: () => {},
               restored: { savedAt: Date.now(), stale: false, pending: false },
               dismissRestored,
            }}
         />,
      );
      expect(screen.queryByRole("alert")).toBeNull();
      screen.getByRole("button", { name: "Draft saved" }).click();
      const menu = await screen.findByRole("menu");
      within(menu).getByText(/Restored your unsaved edits from/);
      expect(within(menu).queryByText(/Kept in this browser/)).toBeNull();
      // Said aloud too, as the banner was.
      // (The open menu hides the rest of the page from the tree.)
      expect(screen.getByRole("status", { hidden: true }).textContent).toMatch(
         /^Draft saved\. Restored your unsaved edits from/,
      );
      fireEvent.keyDown(screen.getByRole("menu"), { key: "Escape" });
      await waitFor(() => expect(dismissRestored).toHaveBeenCalledTimes(1));
   });

   it("says only that the draft is kept once there is no news", async () => {
      render(
         <BuilderBar
            controls={{
               ...base,
               done: () => {},
               draftKeptAt: Date.now(),
               discard: () => {},
            }}
         />,
      );
      screen.getByRole("button", { name: "Draft saved" }).click();
      expect(await screen.findByText(/Kept in this browser at/)).toBeDefined();
      expect(screen.queryByText(/Restored your unsaved edits/)).toBeNull();
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
               .querySelector('[data-tile-key="a.by_cat"]')
               ?.getAttribute("aria-current"),
         ).toBe("true"),
      );
      // A press inside the tile keeps it.
      fireEvent.pointerDown(tile);
      expect(
         document
            .querySelector('[data-tile-key="a.by_cat"]')
            ?.getAttribute("aria-current"),
      ).toBe("true");
      fireEvent.pointerDown(document.body);
      await waitFor(() =>
         expect(
            document
               .querySelector('[data-tile-key="a.by_cat"]')
               ?.getAttribute("aria-current"),
         ).toBe("false"),
      );
   });
});
