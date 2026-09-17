// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import type { QueryResultState } from "../../hooks/useQueryResult";
import { ResultPanel } from "./ResultPanel";

// The two states short of a result, which every query surface used to draw
// for itself — and one of them (the model cell) not at all. The result state
// goes through `ResultContainer`, which has its own coverage and lazy-loads
// the renderer, so it is not driven from here.

const stateOf = (partial: Partial<QueryResultState>) =>
   ({ isSuccess: false, isError: false, ...partial }) as QueryResultState;

describe("ResultPanel", () => {
   it("says it is running until the query answers", () => {
      render(<ResultPanel state={stateOf({})} context="orders -> by_month" />);
      expect(screen.getByText("Running…")).toBeDefined();
   });

   it("takes the caller's words for the wait", () => {
      render(
         <ResultPanel
            state={stateOf({})}
            context="orders -> by_month"
            loadingText="Fetching…"
         />,
      );
      expect(screen.getByText("Fetching…")).toBeDefined();
   });

   it("shows a refused query's reason, and what was asked", () => {
      const error = Object.assign(new Error("refused"), {
         status: 400,
         data: { code: 400, message: "filter 'state' is required" },
      });
      render(
         <ResultPanel
            state={stateOf({ isError: true, error })}
            context="orders -> by_month"
         />,
      );
      expect(screen.getByText("filter 'state' is required")).toBeDefined();
      expect(screen.getByText("orders -> by_month")).toBeDefined();
      expect(screen.queryByText("Running…")).toBeNull();
   });
});
