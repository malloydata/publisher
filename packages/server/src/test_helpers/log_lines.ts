// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Test helper: the lines a logger spy saw that belong to the code under test.
 *
 * `bun test` runs many spec files in one process, and their async work logs
 * through the same process-wide logger while a spy is active. A spec that
 * asserts on every call (or on every call with a common message, such as
 * "Environment env does not exist") fails whenever another file's work lands
 * in that window. Give the code under test a marker of its own (an
 * environment, package or file name no other spec uses) and assert only on
 * the lines that mention it.
 */

type Spy = { mock: { calls: unknown[][] } };

/** Whether a logger call's arguments name `marker`, reading an Error by its name and message (JSON drops both). */
export function mentions(args: unknown[], marker: string): boolean {
   return JSON.stringify(args, (_key, value) =>
      value instanceof Error ? `${value.name}: ${value.message}` : value,
   ).includes(marker);
}

/** The calls `spy` saw whose arguments mention `marker`, in order. */
export function linesMentioning(spy: Spy, marker: string): unknown[][] {
   return spy.mock.calls.filter((args) => mentions(args, marker));
}
