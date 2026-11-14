// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { AuthClient, GoogleAuth, Impersonated } from "google-auth-library";

/**
 * Google Cloud service-account impersonation for BigQuery connections.
 *
 * A connection that sets `impersonateServiceAccount` runs every BigQuery call
 * as that service account, with short-lived tokens from the IAM
 * `generateAccessToken` API, so no key material is exported. The publisher's
 * ambient ADC credential only mints tokens: it needs
 * `roles/iam.serviceAccountTokenCreator` on each target, which holds the
 * data-access roles.
 *
 * Query execution (the `gcpImpersonation` overlay) and schema discovery
 * (`db_utils.ts` `createBigQueryClient`) deliberately share these clients: a
 * discovery client built from ADC would silently read as the publisher's
 * broad identity, which is what impersonation exists to prevent.
 */

const CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform";

/**
 * The publisher's ambient ADC credential, shared by every impersonated
 * connection. The promise is cached so concurrent first callers share one
 * resolution; a failed resolution is not cached, so a transient
 * metadata-server error at boot does not wedge the process.
 */
let sourceClientPromise: Promise<AuthClient> | undefined;

function getSourceClient(): Promise<AuthClient> {
   if (!sourceClientPromise) {
      sourceClientPromise = new GoogleAuth({
         scopes: [CLOUD_PLATFORM_SCOPE],
      })
         .getClient()
         .catch((error) => {
            sourceClientPromise = undefined;
            throw error;
         });
   }
   return sourceClientPromise;
}

/**
 * One `Impersonated` client per target service account. Each refreshes its own
 * tokens (3600s lifetime), so token minting is once an hour per target rather
 * than per query.
 */
const impersonatedClients = new Map<string, Promise<Impersonated>>();

export async function getImpersonatedAuthClient(
   targetPrincipal: string,
): Promise<Impersonated> {
   let clientPromise = impersonatedClients.get(targetPrincipal);
   if (!clientPromise) {
      clientPromise = getSourceClient().then(
         (sourceClient) =>
            new Impersonated({
               sourceClient,
               targetPrincipal,
               targetScopes: [CLOUD_PLATFORM_SCOPE],
               delegates: [],
            }),
      );
      // Don't cache a failed source-credential resolution against this target.
      clientPromise.catch(() => impersonatedClients.delete(targetPrincipal));
      impersonatedClients.set(targetPrincipal, clientPromise);
   }
   return clientPromise;
}

/**
 * The `gcpImpersonation` config overlay: resolves
 * `authClient: {gcpImpersonation: "<sa-email>"}` to a live `Impersonated`
 * client. The bigquery `authClient` property is `opaque`, overlay-sourced and
 * `mustHaveValue`, so only this registration can fill it (never a config
 * literal) and a failed resolution errors instead of falling back to ambient
 * ADC. An empty path is a malformed reference and is refused.
 */
export function gcpImpersonationOverlay(): (
   path: string[],
) => Promise<Impersonated> {
   return (path: string[]) => {
      const targetPrincipal = path[0];
      if (!targetPrincipal) {
         throw new Error(
            "gcpImpersonation overlay reference is missing the target " +
               "service account email.",
         );
      }
      return getImpersonatedAuthClient(targetPrincipal);
   };
}
