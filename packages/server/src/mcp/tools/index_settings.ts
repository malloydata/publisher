// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The settings that decide what a package's semantic index contains: the
 * package's own `retrieval` block (publisher.json) combined with what the
 * operator allows (publisher.config.json). Resolved once per call from the
 * loaded package, never derived in a loop.
 */

import {
   DEFAULT_PACKAGE_RETRIEVAL,
   type PackageRepresentation,
} from "../../service/package_retrieval";
import type { Package } from "../../service/package";

export interface IndexSettings {
   representation: PackageRepresentation;
   /**
    * A string that changes whenever a setting that alters rows changes. It is
    * folded into the readiness fingerprint, so editing `publisher.json`
    * (and reloading) makes the package `indexing` until the sync has applied
    * the change, and a reload that changed nothing keeps its warm index.
    */
   key: string;
}

/**
 * The index settings for a package instance. A stand-in without the accessor
 * (a test double, a package from before the accessor existed) gets the
 * defaults.
 */
export function indexSettingsOf(pkg: Package | undefined): IndexSettings {
   const retrieval =
      (pkg as Partial<Package> | undefined)?.getRetrievalSettings?.() ??
      DEFAULT_PACKAGE_RETRIEVAL;
   return {
      representation: retrieval.representation,
      key: retrieval.representation,
   };
}
