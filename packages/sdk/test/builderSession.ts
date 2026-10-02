// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The session the most recently rendered builder returned, so a host spec can
 * drive what the builder has no control for yet (Undo save).
 *
 * `mock.module` is process-global and cannot be undone, so the stand-in is the
 * real hook with a recorder around it: every spec that inherits it gets the
 * same behavior. Import this BEFORE the component under test.
 */
import { mock } from "bun:test";
import * as session from "../src/components/DashboardBuilder/useBuilderSession";

type Session = ReturnType<typeof session.useBuilderSession>;

export const lastSession: { current: Session | undefined } = {
   current: undefined,
};

// Captured before the mock, which replaces the module's bindings in place.
const real = { ...session };
const useBuilderSession: typeof session.useBuilderSession = (options) => {
   const result = real.useBuilderSession(options);
   lastSession.current = result as Session;
   return result;
};
mock.module("../src/components/DashboardBuilder/useBuilderSession", () => ({
   ...real,
   useBuilderSession,
}));
