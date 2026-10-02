// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   cleanup,
   fireEvent,
   render,
   screen,
   waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import {
   clearCache,
   mockServerProvider,
   serverWrapper,
} from "../../../test/serverProvider";
import { isMac } from "../DashboardBuilder/useBuilderShortcuts";
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

const typeDraft = (text: string) => {
   fireEvent.click(button("Edit text"));
   fireEvent.change(screen.getByLabelText("Markdown"), {
      target: { value: text },
   });
};

beforeEach(clearCache);
afterEach(cleanup);

describe("NotebookBuilder: an open text draft", () => {
   it("counts as unsaved while open, and not once cancelled", async () => {
      const onDirtyChange = mock((_dirty: boolean) => {});
      await mount({ onDirtyChange });
      expect(onDirtyChange.mock.calls.at(-1)?.[0]).toBe(false);
      typeDraft("Changed.");
      await waitFor(() =>
         expect(onDirtyChange.mock.calls.at(-1)?.[0]).toBe(true),
      );
      fireEvent.click(button("Cancel"));
      await waitFor(() =>
         expect(onDirtyChange.mock.calls.at(-1)?.[0]).toBe(false),
      );
   });

   it("reports clean when the builder unmounts with a draft open", async () => {
      const onDirtyChange = mock((_dirty: boolean) => {});
      const { unmount } = await mount({ onDirtyChange });
      typeDraft("Changed.");
      await waitFor(() =>
         expect(onDirtyChange.mock.calls.at(-1)?.[0]).toBe(true),
      );
      unmount();
      expect(onDirtyChange.mock.calls.at(-1)?.[0]).toBe(false);
   });
});

describe("NotebookBuilder: save from inside the text field", () => {
   it("commits the draft and saves it", async () => {
      const onSave = mock(async (_source: string) => {});
      await mount({ onSave });
      typeDraft("Changed.");
      fireEvent.keyDown(screen.getByLabelText("Markdown"), {
         key: "s",
         ...(isMac ? { metaKey: true } : { ctrlKey: true }),
      });
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0]).toContain("Changed.");
   });
});
