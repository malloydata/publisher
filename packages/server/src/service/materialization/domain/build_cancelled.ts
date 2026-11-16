// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A materialization build stopping at a checkpoint because its run was
 * cancelled ({@link BuildCancelledError}). It never answers a request: a
 * cancelled run settles as CANCELLED.
 */
export class BuildCancelledError extends Error {
   constructor() {
      super("Build cancelled");
      this.name = "BuildCancelledError";
   }
}
