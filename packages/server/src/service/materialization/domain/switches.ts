// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The deployment switches materialization reads, as one type, so a pure rule
 * takes the ones it needs as an input rather than reading the environment.
 * The rules take a part of it today (`GateSwitches`, read at a shell by
 * `gateSwitches()` in `config.ts`; a `StorageMode`); nothing yet reads the
 * whole value at once.
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
