// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The deployment switches materialization reads, as one value: read once by
 * a shell (`materializationSwitches()` in `config.ts`) and passed down, so a
 * pure rule takes them as an input rather than reading the environment.
 */

/**
 * How `storage=` is honored (`PERSIST_STORAGE_MODE`): `off`, it is inert and
 * a source declaring it is served live; `write-only`, builds write the
 * destination but serving stays live; `on`, builds write it and serve from it.
 */
export type StorageMode = "off" | "write-only" | "on";

export interface Switches {
   /** `materialize.enabled` (`PUBLISHER_MATERIALIZE`). */
   readonly materialize: boolean;
   /** `materialize.gatedSources` (`PUBLISHER_MATERIALIZE_GATED_SOURCES`). */
   readonly gatedSources: boolean;
   /** `persist.storageMode` (`PERSIST_STORAGE_MODE`). */
   readonly storageMode: StorageMode;
   /**
    * `persist.addressIncludesDestination`
    * (`PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION`).
    */
   readonly addressIncludesDestination: boolean;
}
