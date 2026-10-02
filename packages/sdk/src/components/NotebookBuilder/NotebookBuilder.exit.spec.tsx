// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   act,
   cleanup,
   fireEvent,
   render,
   screen,
   waitFor,
   within,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import {
   clearCache,
   mockServerProvider,
   serverWrapper,
} from "../../../test/serverProvider";
import {
   notebookSourceRefused,
   readNotebookSource,
} from "./readNotebookSource";

mockServerProvider({
   models: { executeQueryModel: mock(async () => ({ data: {} })) },
});

const { NotebookBuilder } = await import("./NotebookBuilder");

const SOURCE = `## artifact { kind=notebook }
##(markdown) Intro.

source: a is duckdb.table('t')
`;

const mount = async (
   options: {
      onExit?: () => void;
      onSave?: (source: string) => Promise<void> | void;
      onDirtyChange?: (dirty: boolean) => void;
   } = {},
) => {
   const read = await readNotebookSource(SOURCE);
   if (notebookSourceRefused(read)) throw new Error(read.refused);
   return render(
      <NotebookBuilder
         source={SOURCE}
         notebook={read.source}
         environmentName="env"
         packageName="pkg"
         modelPath="notebooks/tour.malloy"
         {...(options.onExit ? { onExit: options.onExit } : {})}
         {...(options.onSave ? { onSave: options.onSave } : {})}
         {...(options.onDirtyChange
            ? { onDirtyChange: options.onDirtyChange }
            : {})}
      />,
      { wrapper: serverWrapper },
   );
};

const button = (name: string) =>
   screen.getByRole("button", { name, hidden: true });

const edit = () => {
   fireEvent.click(button("Edit text"));
   fireEvent.change(screen.getByLabelText("Markdown"), {
      target: { value: "Changed." },
   });
   fireEvent.click(button("Done"));
};

const shortcut = (key: string) =>
   fireEvent.keyDown(window, { key, ctrlKey: true, metaKey: true });

const dialogs = () => screen.getAllByRole("dialog", { hidden: true });

const settleTick = () => act(async () => {});

beforeEach(clearCache);
afterEach(cleanup);

describe("NotebookBuilder: leaving", () => {
   it("renders no Done editing without an onExit", async () => {
      await mount();
      expect(screen.queryByRole("button", { name: "Done editing" })).toBeNull();
   });

   it("exits at once when nothing is unsaved", async () => {
      const onExit = mock(() => {});
      await mount({ onExit });
      fireEvent.click(button("Done editing"));
      expect(onExit).toHaveBeenCalledTimes(1);
   });

   it("asks first when there are edits, and Keep editing stays", async () => {
      const onExit = mock(() => {});
      await mount({ onExit, onSave: async () => {} });
      edit();
      fireEvent.click(button("Done editing"));
      expect(screen.getByRole("dialog")).toBeDefined();
      expect(onExit).not.toHaveBeenCalled();
      fireEvent.click(button("Keep editing"));
      expect(onExit).not.toHaveBeenCalled();
   });

   it("Discard changes exits without saving", async () => {
      const onExit = mock(() => {});
      const onSave = mock(async () => {});
      await mount({ onExit, onSave });
      edit();
      fireEvent.click(button("Done editing"));
      fireEvent.click(button("Discard changes"));
      expect(onExit).toHaveBeenCalledTimes(1);
      expect(onSave).not.toHaveBeenCalled();
   });

   it("Save and exit saves, then exits", async () => {
      const onExit = mock(() => {});
      const onSave = mock(async () => {});
      await mount({ onExit, onSave });
      edit();
      fireEvent.click(button("Done editing"));
      fireEvent.click(button("Save and exit"));
      await waitFor(() => expect(onExit).toHaveBeenCalledTimes(1));
      expect(onSave).toHaveBeenCalledTimes(1);
   });

   it("stays open when the save fails", async () => {
      const onExit = mock(() => {});
      const onSave = mock(async () => {
         throw new Error("nope");
      });
      await mount({ onExit, onSave });
      edit();
      fireEvent.click(button("Done editing"));
      fireEvent.click(button("Save and exit"));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      await screen.findByText(/nope/);
      expect(onExit).not.toHaveBeenCalled();
   });

   it("offers no Save and exit when the builder cannot save", async () => {
      await mount({ onExit: () => {} });
      edit();
      fireEvent.click(button("Done editing"));
      expect(
         screen.queryByRole("button", { name: "Save and exit" }),
      ).toBeNull();
      expect(button("Discard changes")).toBeDefined();
   });

   it("reports clean when it unmounts dirty", async () => {
      const onDirtyChange = mock((_dirty: boolean) => {});
      const view = await mount({ onDirtyChange, onSave: async () => {} });
      edit();
      expect(onDirtyChange.mock.calls.at(-1)?.[0]).toBe(true);
      view.unmount();
      expect(onDirtyChange.mock.calls.at(-1)?.[0]).toBe(false);
   });
});

const removeText = () =>
   fireEvent.click(
      within(
         screen.getByRole("group", { name: "Cell 1, text", hidden: true }),
      ).getByRole("button", { name: "Remove text", hidden: true }),
   );

describe("NotebookBuilder: leaving with a structural edit", () => {
   it("ignores save, undo and redo shortcuts while the exit dialog is open", async () => {
      const onSave = mock(async () => {});
      await mount({ onExit: () => {}, onSave });
      removeText();
      fireEvent.click(button("Done editing"));
      shortcut("s");
      shortcut("z");
      await settleTick();
      expect(dialogs()).toHaveLength(1);
      expect(onSave).not.toHaveBeenCalled();
   });

   it("Save and exit writes a structural edit at once, saving once and exiting once", async () => {
      const onExit = mock(() => {});
      const onSave = mock(async () => {});
      await mount({ onExit, onSave });
      removeText();
      fireEvent.click(button("Done editing"));
      fireEvent.click(button("Save and exit"));
      await waitFor(() => expect(onExit).toHaveBeenCalledTimes(1));
      expect(onSave).toHaveBeenCalledTimes(1);
   });

   it("waits out a save already in flight, then exits without saving twice", async () => {
      const onExit = mock(() => {});
      let finish = () => {};
      const onSave = mock(
         () => new Promise<void>((resolve) => (finish = resolve)),
      );
      await mount({ onExit, onSave });
      edit();
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      fireEvent.click(button("Done editing"));
      fireEvent.click(button("Save and exit"));
      await settleTick();
      expect(onExit).not.toHaveBeenCalled();
      await act(async () => finish());
      await waitFor(() => expect(onExit).toHaveBeenCalledTimes(1));
      expect(onSave).toHaveBeenCalledTimes(1);
   });
});

describe("NotebookBuilder: leaving with a text draft still open", () => {
   const typeThenTabToDone = () => {
      fireEvent.click(button("Edit text"));
      const field = screen.getByLabelText("Markdown");
      fireEvent.change(field, { target: { value: "Changed." } });
      fireEvent.blur(field, { relatedTarget: button("Done") });
   };

   it("asks before Done editing exits, since the draft is unsaved", async () => {
      const onExit = mock(() => {});
      await mount({ onExit, onSave: async () => {} });
      typeThenTabToDone();
      fireEvent.click(button("Done editing"));
      expect(screen.getByRole("dialog")).toBeDefined();
      expect(onExit).not.toHaveBeenCalled();
   });

   it("Save and exit commits the draft, saves it, then exits", async () => {
      const onExit = mock(() => {});
      const onSave = mock(async (_source: string) => {});
      await mount({ onExit, onSave });
      typeThenTabToDone();
      fireEvent.click(button("Done editing"));
      fireEvent.click(button("Save and exit"));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0]).toContain("Changed.");
      await waitFor(() => expect(onExit).toHaveBeenCalledTimes(1));
   });
});
