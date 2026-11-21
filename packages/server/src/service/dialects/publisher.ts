// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `publisher` connection type's registry entry: a connection to another
 * Publisher's connection, whose dialect and attributes are resolved at
 * runtime, so it has no static attributes.
 */

import type { ConnectionDialect } from "./types";

export const PUBLISHER: ConnectionDialect = Object.freeze({
   type: "publisher",
   attributes: undefined,
});
