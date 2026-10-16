// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
   act,
   fireEvent,
   render,
   renderHook,
   screen,
   waitFor,
} from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import type { ReactNode } from "react";
import type { OptionPage, OptionSearch } from "../../hooks/useSuggestOptions";
import { encodeFilterList } from "./filterValue";
import { GivenInput } from "./GivenInput";
import { useOptionSearch } from "./useOptionSearch";

const withClient = () => {
   const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
   });
   function Wrapper({ children }: { children: ReactNode }) {
      return (
         <QueryClientProvider client={client}>{children}</QueryClientProvider>
      );
   }
   return Wrapper;
};

/** A search whose answers the test releases, one text at a time and in any order. */
const controlledSearch = () => {
   const waiting = new Map<string, (page: OptionPage) => void>();
   const run = mock(
      (text: string) =>
         new Promise<OptionPage>((resolve) => waiting.set(text, resolve)),
   );
   const search: OptionSearch = { key: ["givenSuggestSearch", "MAKER"], run };
   const answer = (text: string, values: string[]) =>
      act(() => waiting.get(text)?.({ values, truncated: false }));
   return { search, run, answer };
};

describe("useOptionSearch", () => {
   it("asks nothing without a search: a list that fits filters in the browser", async () => {
      const { result } = renderHook(() => useOptionSearch(undefined, "gen"), {
         wrapper: withClient(),
      });
      await new Promise((resolve) => setTimeout(resolve, 300));
      expect(result.current).toEqual({
         active: false,
         searching: false,
         page: undefined,
         failed: false,
      });
   });

   it("waits for typing to pause, then asks once for what is typed", async () => {
      const { search, run } = controlledSearch();
      const { rerender } = renderHook(
         ({ typed }) => useOptionSearch(search, typed),
         { initialProps: { typed: "g" }, wrapper: withClient() },
      );
      rerender({ typed: "ge" });
      rerender({ typed: "gen" });
      await waitFor(() => expect(run).toHaveBeenCalledTimes(1));
      expect(run.mock.calls[0][0]).toBe("gen");
   });

   it("never shows an older text's answer over a newer one's, whichever lands first", async () => {
      const { search, run, answer } = controlledSearch();
      const { result, rerender } = renderHook(
         ({ typed }) => useOptionSearch(search, typed),
         { initialProps: { typed: "gen" }, wrapper: withClient() },
      );
      await waitFor(() => expect(run).toHaveBeenCalledTimes(1));
      rerender({ typed: "general" });
      await waitFor(() => expect(run).toHaveBeenCalledTimes(2));
      expect(result.current.searching).toBe(true);

      // The newer answer lands first, then the older one, late.
      await answer("general", ["General Motors, LLC"]);
      await waitFor(() =>
         expect(result.current.page?.values).toEqual(["General Motors, LLC"]),
      );
      await answer("gen", ["Genesis", "General Motors, LLC"]);
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(result.current.page?.values).toEqual(["General Motors, LLC"]);
      expect(result.current.searching).toBe(false);
   });

   it("reuses a text's answer when the reader types back to it", async () => {
      const { search, run, answer } = controlledSearch();
      const { result, rerender } = renderHook(
         ({ typed }) => useOptionSearch(search, typed),
         { initialProps: { typed: "gen" }, wrapper: withClient() },
      );
      await waitFor(() => expect(run).toHaveBeenCalledTimes(1));
      await answer("gen", ["Genesis"]);
      rerender({ typed: "gene" });
      await waitFor(() => expect(run).toHaveBeenCalledTimes(2));
      rerender({ typed: "gen" });
      await waitFor(() =>
         expect(result.current.page?.values).toEqual(["Genesis"]),
      );
      expect(run).toHaveBeenCalledTimes(2);
   });
});

describe("GivenInput over a cut-off list", () => {
   const MAKER = {
      name: "MAKER",
      type: "filter<string>",
      control: "select" as const,
      label: "Manufacturer",
   };
   // The first thousand, alphabetically: "General Motors, LLC" is not among them.
   const firstThousand = Array.from(
      { length: 1000 },
      (_, i) => `A${String(i).padStart(4, "0")}`,
   );

   it("finds a value past the cutoff by searching the server, and picking it filters by it", async () => {
      const run = mock(async (text: string) => ({
         values: text.toLowerCase().includes("general")
            ? ["General Motors, LLC"]
            : [],
         truncated: false,
      }));
      const onChange = mock((_next: unknown) => {});
      const Wrapper = withClient();
      render(
         <Wrapper>
            <GivenInput
               given={MAKER}
               value={undefined}
               onChange={onChange}
               options={firstThousand}
               optionsTruncated
               optionSearch={{ key: ["givenSuggestSearch", "MAKER"], run }}
            />
         </Wrapper>,
      );
      const box = screen.getByRole("combobox", { name: "Manufacturer" });
      fireEvent.focus(box);
      fireEvent.change(box, { target: { value: "General Motors" } });

      const option = await screen.findByRole("option", {
         name: "General Motors, LLC",
      });
      expect(run.mock.calls.at(-1)?.[0]).toBe("General Motors");
      fireEvent.click(option);
      // The filter that matches exactly that value, as the control row sends it.
      expect(onChange).toHaveBeenCalledWith(
         encodeFilterList(["General Motors, LLC"]),
      );
   });

   it("says the list is partial before anything is typed", async () => {
      const Wrapper = withClient();
      render(
         <Wrapper>
            <GivenInput
               given={MAKER}
               value={undefined}
               onChange={() => {}}
               options={firstThousand}
               optionsTruncated
               optionSearch={{
                  key: ["givenSuggestSearch", "MAKER"],
                  run: async () => ({ values: [], truncated: false }),
               }}
            />
         </Wrapper>,
      );
      fireEvent.mouseDown(
         screen.getByRole("combobox", { name: "Manufacturer" }),
      );
      expect((await screen.findByRole("note")).textContent).toBe(
         "Showing the first 1,000 values. Type to search them all.",
      );
   });

   it("for a named query, filters what it has and says a value can still be typed", async () => {
      render(
         <GivenInput
            given={MAKER}
            value={undefined}
            onChange={() => {}}
            options={firstThousand}
            optionsTruncated
         />,
      );
      fireEvent.mouseDown(
         screen.getByRole("combobox", { name: "Manufacturer" }),
      );
      expect((await screen.findByRole("note")).textContent).toBe(
         "Showing the first 1,000 values. A value not listed can still be typed.",
      );
   });

   it("adds no footnote to a list that is whole", async () => {
      render(
         <GivenInput
            given={MAKER}
            value={undefined}
            onChange={() => {}}
            options={["Ford", "General Motors, LLC"]}
         />,
      );
      fireEvent.mouseDown(
         screen.getByRole("combobox", { name: "Manufacturer" }),
      );
      await screen.findByRole("option", { name: "Ford" });
      expect(screen.queryByRole("note")).toBeNull();
   });
});
