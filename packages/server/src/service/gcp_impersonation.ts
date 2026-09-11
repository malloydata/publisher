// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { AuthClient, GoogleAuth, Impersonated } from "google-auth-library";

/**
 * Google Cloud service-account impersonation for BigQuery connections.
 *
 * A connection that sets `impersonateServiceAccount` runs every BigQuery call
 * as that service account, using short-lived tokens minted through the IAM
 * Service Account Credentials API (`generateAccessToken`) — no exportable key
 * material anywhere. The publisher's own credential (ambient ADC) is only the
 * token *minter*: it needs `roles/iam.serviceAccountTokenCreator` on each
 * target service account, and the data-access roles live on the target instead.
 *
 * Two consumers share the clients built here, deliberately:
 *  - Malloy query execution, via the `gcpImpersonation` config overlay that
 *    `buildEnvironmentMalloyConfig` registers (the connection pojo carries
 *    `authClient: {gcpImpersonation: "<sa-email>"}`), and
 *  - schema discovery (`db_utils.ts` `createBigQueryClient`), which passes the
 *    same client as `ServiceOptions.authClient` to `new BigQuery(...)`.
 * If discovery built its own client from ADC it would silently read as the
 * publisher's broad identity — exactly what impersonation exists to prevent.
 */

const CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform";

/**
 * The publisher's ambient credential, fetched once per process and shared by
 * every impersonated connection. `GoogleAuth.getClient()` resolves ADC (a
 * mounted credentials file, workload identity, or the metadata server); the
 * promise is cached rather than the client so concurrent first callers share
 * one resolution. A failed resolution is NOT cached — the next call retries —
 * so a transient metadata-server hiccup at boot doesn't wedge the process.
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
 * One `Impersonated` client per target service account. The client refreshes
 * its own short-lived tokens (default lifetime 3600s) for the process
 * lifetime, so callers never see an expired credential, and one entry per
 * distinct target keeps the `generateAccessToken` call volume at
 * one-mint-per-hour per tenant rather than per query.
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
 * The `gcpImpersonation` config overlay. A connection pojo referencing
 * `authClient: {gcpImpersonation: "sa@project.iam.gserviceaccount.com"}`
 * resolves through this to a live `Impersonated` client. The property is
 * declared `opaque` + `source: 'overlay'` + `mustHaveValue` on the bigquery
 * connection type, so the reference can only be filled by this registration —
 * never by a config literal — and a failure to resolve errors instead of
 * silently falling back to ambient ADC.
 *
 * The overlay resolver awaits async overlays, so returning the promise is
 * fine. An empty path means a malformed reference; refuse it rather than
 * letting `Impersonated` target `undefined`.
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
