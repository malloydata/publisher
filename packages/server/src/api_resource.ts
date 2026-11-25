// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The REST API's resource paths, in one place. An API object a listing or a
 * status read returns carries its own `resource` path; this module is the
 * only place one is spelled, so the domain names what it describes and the
 * REST layout lives with the API prefix.
 */

import { API_PREFIX } from "./constants";

/** `GET {API_PREFIX}/environments/{environmentName}`. */
export function environmentResource(environmentName: string): string {
   return `${API_PREFIX}/environments/${environmentName}`;
}

/** `GET {API_PREFIX}/environments/{environmentName}/packages/{packageName}`. */
export function packageResource(
   environmentName: string,
   packageName: string,
): string {
   return `${environmentResource(environmentName)}/packages/${packageName}`;
}
