// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createContext } from "react";
import type { GivenInfo } from "./modelGivens";

/**
 * The model's givens a reader can set (router-filled ones already left out), for
 * the rows that read one: each marks itself "Given" and says which, from its own
 * code (givensReadBy). Provided once by the explorer rather than threaded
 * through every section and group.
 */
export const ModelGivensContext = createContext<ReadonlyArray<GivenInfo>>([]);
