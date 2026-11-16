// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `retrieval` block's presets and defaults, in a module that imports
 * nothing, so the settings registry can name them without importing the
 * retrieval code (see import_cycles.spec.ts). ./config.ts re-exports them.
 */

/** What may leave the machine for an LLM. See RetrievalConfig.egress. */
export type EgressPreset = "default" | "full";
export const EGRESS_PRESETS: readonly EgressPreset[] = ["default", "full"];

export const DEFAULT_LLM_TIMEOUT_MS = 30_000;
export const DEFAULT_LLM_CONCURRENCY = 4;
export const DEFAULT_LLM_MAX_CALLS_PER_SYNC = 300;
export const DEFAULT_LLM_MAX_CALLS_PER_REQUEST = 20;

/** What each `retrieval.dimensionValues` key means when the operator leaves it out. */
export const DIMENSION_VALUE_DEFAULTS = { enabled: true } as const;
