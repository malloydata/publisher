// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package's search index build state (engine_state.ts). Its own module so
 * the RetrievalRegistry, which holds one per Package, names the type without
 * importing the module that reads the registry.
 */

import type { FcjIndex } from "./index_build";
import type { VectorProgress } from "./vectors";

export type FcjState =
   | {
        status: "building";
        startedAt: number;
        /** Set once the vectors step starts. */
        progress?: VectorProgress;
        /** When a vector this build keeps was last written. */
        lastSyncedAt?: number;
     }
   | {
        status: "ready";
        index: FcjIndex;
        /** When this index's vectors were last written; unset when it has none. */
        lastSyncedAt?: number;
        /** Set when value reads failed: when to build again for them. */
        rebuildAt?: number;
        /**
         * Builds in a row that left values unread. Each doubles the wait
         * before the next ({@link valueRetryDelayMs}).
         */
        valueFailures?: number;
        /** A rebuild for failed value reads is queued; this index serves until it lands. */
        rebuilding?: boolean;
     }
   | { status: "error"; message: string; failedAt: number };
