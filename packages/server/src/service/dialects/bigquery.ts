// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `bigquery` connection type's registry entry: Malloy's dialect for it is `standardsql`. */

import { boolean, Mismatch } from "../../settings/types";
import type {
   ApiConnection,
   ConnectionDialect,
   CoreConnectionEntry,
   NamedConnection,
} from "./types";

export const BIGQUERY: ConnectionDialect = Object.freeze({
   type: "bigquery",
   attributes: Object.freeze({
      dialectName: "standardsql",
      isPool: false,
      canPersist: true,
      canStream: true,
   }),
   validateShape: validateBigQueryShape,
   coreEntry: buildBigQueryEntry,
});

// Field names follow Google's service-account key file format, so they are
// fixed by that format and do not track Publisher's own vocabulary.
type ServiceAccountKey = {
   type?: string;
   project_id?: string;
   private_key?: string;
   client_email?: string;
   [key: string]: unknown;
};

export function parseServiceAccountKey(
   json?: string,
): ServiceAccountKey | undefined {
   if (!json) return undefined;
   const keyData = JSON.parse(json) as ServiceAccountKey;
   const requiredFields = ["type", "project_id", "private_key", "client_email"];
   for (const field of requiredFields) {
      if (!keyData[field]) {
         throw new Error(
            `Invalid service account key: missing "${field}" field`,
         );
      }
   }
   if (keyData.type !== "service_account") {
      throw new Error('Invalid service account key: incorrect "type" field');
   }
   return keyData;
}

function validateBigQueryShape(connection: ApiConnection): void {
   const bigquery = connection.bigqueryConnection;
   if (bigquery?.impersonateServiceAccount) {
      // An authClient replaces the credential entirely, so a key alongside it
      // would look live while the impersonated identity runs every query.
      // Core refuses this too; refusing here makes it a load-time error.
      if (bigquery.serviceAccountKeyJson) {
         throw new Error(
            `Connection '${connection.name}' sets impersonateServiceAccount ` +
               `and serviceAccountKeyJson. Impersonation replaces the ` +
               `credential entirely — the key would be ignored — so supply ` +
               `one or the other.`,
         );
      }
      // With an authClient the SDK resolves the project through the
      // impersonated credential, not ADC, so auto-detection is no fallback.
      if (!bigquery.billingProjectId) {
         throw new Error(
            `Connection '${connection.name}' sets impersonateServiceAccount ` +
               `but no billingProjectId. Impersonated connections resolve ` +
               `the project through the impersonated credential, so the ` +
               `project that runs (and is billed for) jobs must be named ` +
               `explicitly.`,
         );
      }
   }
}

function buildBigQueryEntry(connection: NamedConnection): CoreConnectionEntry {
   const serviceAccountKey = parseServiceAccountKey(
      connection.bigqueryConnection?.serviceAccountKeyJson as
         | string
         | undefined,
   );
   // Impersonation rides the config-overlay mechanism: the pojo carries a
   // plain-JSON reference, which the `gcpImpersonation` overlay registered by
   // buildEnvironmentMalloyConfig resolves to a live auth client. The property
   // is opaque, overlay-sourced and mustHaveValue, so a literal can never
   // satisfy it and an unresolved reference errors instead of falling back to
   // ambient ADC. The reference embeds the SA email, so core's connection
   // digest (and so the BuildID) is per identity.
   const impersonateServiceAccount =
      connection.bigqueryConnection?.impersonateServiceAccount;
   return {
      is: "bigquery",
      projectId:
         connection.bigqueryConnection?.defaultProjectId ||
         serviceAccountKey?.project_id,
      serviceAccountKey,
      // Spread rather than `authClient: x ?? undefined`: the property is
      // mustHaveValue and core checks whether it is SET, so an undefined
      // entry would fail every plain connection with "no value arrived".
      ...(impersonateServiceAccount
         ? {
              authClient: {
                 gcpImpersonation: impersonateServiceAccount,
              },
           }
         : {}),
      location: connection.bigqueryConnection?.location,
      maximumBytesBilled: connection.bigqueryConnection?.maximumBytesBilled,
      timeoutMs: connection.bigqueryConnection?.queryTimeoutMilliseconds,
      billingProjectId: connection.bigqueryConnection?.billingProjectId,
      dbmeta: configBoolean(
         connection.bigqueryConnection?.dbmeta,
         `dbmeta on BigQuery connection "${connection.name}"`,
      ),
   };
}

const BOOLEAN = boolean();

/**
 * A boolean connection setting as a boolean. A string, as `${VAR}`
 * substitution and API clients deliver it, is read the way a boolean setting
 * is, blank as unset; anything else is refused rather than dropped.
 */
export function configBoolean(
   value: unknown,
   name: string,
): boolean | undefined {
   if (value === undefined || value === null) return undefined;
   if (typeof value === "boolean") return value;
   if (typeof value === "string") {
      if (value.trim() === "") return undefined;
      try {
         return BOOLEAN.fromString(value);
      } catch (error) {
         if (!(error instanceof Mismatch)) throw error;
      }
   }
   throw new Error(
      `Invalid value for ${name}: expected ${BOOLEAN.expected}, got ${JSON.stringify(value)}`,
   );
}
