// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Opt a test suite into the colocated-persist row-level relaxation.
 *
 * `PERSIST_COLOCATED_RELAXATION_ENABLED` defaults to `false` — enabling it
 * starts materializing gated sources in packages nobody republished, so the
 * default is the half of the flag that carries the safety property. That makes
 * every test OF the relaxation an opt-in: left implicit, such a test asserts
 * the pre-relaxation refusal while still reading as relaxation coverage.
 *
 * Call inside the `describe` that exercises the relaxation. Restores the
 * previous value afterwards, so a sibling suite asserting the refusing default
 * is unaffected by ordering.
 */
export function enableColocatedRelaxationForTests(hooks: {
   beforeEach: (fn: () => void) => void;
   afterEach: (fn: () => void) => void;
}): void {
   const prev = process.env.PERSIST_COLOCATED_RELAXATION_ENABLED;
   hooks.beforeEach(() => {
      process.env.PERSIST_COLOCATED_RELAXATION_ENABLED = "true";
   });
   hooks.afterEach(() => {
      if (prev === undefined) {
         delete process.env.PERSIST_COLOCATED_RELAXATION_ENABLED;
      } else {
         process.env.PERSIST_COLOCATED_RELAXATION_ENABLED = prev;
      }
   });
}
