import { components } from "../api";

type ApiConnection = components["schemas"]["Connection"];

/**
 * A connection config holds warehouse credentials, so a response built by
 * spreading one publishes every field the config happens to carry, including
 * fields that did not exist when the response was written.
 *
 * This is an ALLOWLIST, and the direction it fails in is the point. A denylist
 * of secret-looking names puts the risk on the wrong case: the next connection
 * type adds a credential field, nobody adds it to the list, and it ships
 * readable. Here an unlisted field is simply absent, so a new credential is
 * invisible until someone deliberately publishes it, and the cost of forgetting
 * a non-secret field is only that the UI does not show it yet.
 *
 * The OpenAPI contract cannot supply this list. `api-doc.yaml` declares
 * `password` as a property of `PostgresConnection` because one `Connection`
 * schema serves both the write path, where a caller must supply a credential,
 * and the read path, where it must never come back.
 *
 * Secrets are OMITTED, never masked. A placeholder would be written into the
 * stored config as a real credential by any caller that round-trips what it
 * reads; `mergeConnectionUpdate` covers the omission instead.
 */
interface PublicShape {
   readonly [field: string]: PublicField;
}

type PublicField =
   | "scalar"
   | "any"
   | { readonly kind: "array"; readonly shape: PublicShape }
   | PublicShape;

const listOf = (shape: PublicShape): PublicField => ({ kind: "array", shape });

const CONNECTION_ATTRIBUTES: PublicShape = {
   dialectName: "scalar",
   isPool: "scalar",
   canPersist: "scalar",
   canStream: "scalar",
};

// Per-type shapes. Credential fields are absent by omission, not by exclusion:
// postgres password/connectionString, bigquery serviceAccountKeyJson, snowflake
// password/privateKey/privateKeyPass, trino password/peakaKey, databricks
// token/oauthClientSecret, mysql password, motherduck and publisher accessToken.
const POSTGRES: PublicShape = {
   host: "scalar",
   port: "scalar",
   databaseName: "scalar",
   userName: "scalar",
   sslmode: "scalar",
};

const BIGQUERY: PublicShape = {
   defaultProjectId: "scalar",
   billingProjectId: "scalar",
   location: "scalar",
   // An email naming the account to impersonate, not a credential: the token
   // it stands for is minted at call time from the publisher's own ADC.
   impersonateServiceAccount: "scalar",
   maximumBytesBilled: "scalar",
   queryTimeoutMilliseconds: "scalar",
};

const SNOWFLAKE: PublicShape = {
   account: "scalar",
   username: "scalar",
   warehouse: "scalar",
   database: "scalar",
   schema: "scalar",
   role: "scalar",
   responseTimeoutMilliseconds: "scalar",
};

const TRINO: PublicShape = {
   server: "scalar",
   port: "scalar",
   catalog: "scalar",
   schema: "scalar",
   user: "scalar",
};

const DATABRICKS: PublicShape = {
   host: "scalar",
   path: "scalar",
   oauthClientId: "scalar",
   defaultCatalog: "scalar",
   defaultSchema: "scalar",
   setupSQL: "scalar",
};

const MYSQL: PublicShape = {
   host: "scalar",
   port: "scalar",
   database: "scalar",
   user: "scalar",
};

const MOTHERDUCK: PublicShape = { database: "scalar" };

const PUBLISHER: PublicShape = { connectionUri: "scalar" };

// Object-store credentials. The key IDs are identifiers rather than secrets and
// match what `SENSITIVE_KEY_NAMES` in logger.ts already treats as safe to log;
// the paired secrets (secretAccessKey, sessionToken, secret) are omitted.
const S3: PublicShape = {
   region: "scalar",
   endpoint: "scalar",
   accessKeyId: "scalar",
};

const GCS: PublicShape = { keyId: "scalar" };

// sasUrl and clientSecret omitted; a SAS URL is a bearer credential in a URL.
const AZURE: PublicShape = {
   authType: "scalar",
   tenantId: "scalar",
   clientId: "scalar",
   accountName: "scalar",
   fileUrl: "scalar",
};

// hostKey is the bastion's PUBLIC key, used to pin it; the client privateKey and
// its passphrase are omitted.
const SSH_PROXY: PublicShape = {
   host: "scalar",
   port: "scalar",
   username: "scalar",
   hostKey: "scalar",
};

const PROXY: PublicShape = { type: "scalar", ssh: SSH_PROXY };

const ATTACHED_DATABASE: PublicShape = {
   name: "scalar",
   type: "scalar",
   attributes: CONNECTION_ATTRIBUTES,
   bigqueryConnection: BIGQUERY,
   snowflakeConnection: SNOWFLAKE,
   postgresConnection: POSTGRES,
   gcsConnection: GCS,
   s3Connection: S3,
   azureConnection: AZURE,
};

const DUCKDB: PublicShape = {
   attachedDatabases: listOf(ATTACHED_DATABASE),
};

const DUCKLAKE: PublicShape = {
   storage: {
      bucketUrl: "scalar",
      s3Connection: S3,
      gcsConnection: GCS,
   },
   catalog: {
      postgresConnection: POSTGRES,
      metadataSchema: "scalar",
   },
};

const PUBLIC_CONNECTION: PublicShape = {
   resource: "scalar",
   name: "scalar",
   type: "scalar",
   fingerprint: "scalar",
   // Free-form property bags the contract documents as non-secret and
   // round-tripped verbatim, so they cannot be enumerated field by field.
   queryMetadata: "any",
   queryMetadataEnforced: "any",
   attributes: CONNECTION_ATTRIBUTES,
   proxy: PROXY,
   postgresConnection: POSTGRES,
   bigqueryConnection: BIGQUERY,
   snowflakeConnection: SNOWFLAKE,
   trinoConnection: TRINO,
   databricksConnection: DATABRICKS,
   mysqlConnection: MYSQL,
   duckdbConnection: DUCKDB,
   motherduckConnection: MOTHERDUCK,
   ducklakeConnection: DUCKLAKE,
   publisherConnection: PUBLISHER,
};

/**
 * The allowlist keyed by the OpenAPI schema each shape mirrors. Exported so the
 * spec can hold it against api-doc.yaml: a field added to the contract has to
 * be classified, here or in the spec's secret list, before the suite passes.
 * That is what stops a new credential field from shipping readable.
 */
export const PUBLIC_FIELDS_BY_SCHEMA: Readonly<
   Record<string, readonly string[]>
> = {
   Connection: Object.keys(PUBLIC_CONNECTION),
   PostgresConnection: Object.keys(POSTGRES),
   BigqueryConnection: Object.keys(BIGQUERY),
   SnowflakeConnection: Object.keys(SNOWFLAKE),
   TrinoConnection: Object.keys(TRINO),
   DatabricksConnection: Object.keys(DATABRICKS),
   MysqlConnection: Object.keys(MYSQL),
   DuckdbConnection: Object.keys(DUCKDB),
   MotherDuckConnection: Object.keys(MOTHERDUCK),
   DucklakeConnection: Object.keys(DUCKLAKE),
   PublisherConnection: Object.keys(PUBLISHER),
   S3Connection: Object.keys(S3),
   GCSConnection: Object.keys(GCS),
   AzureConnection: Object.keys(AZURE),
   SshProxyConfig: Object.keys(SSH_PROXY),
   AttachedDatabase: Object.keys(ATTACHED_DATABASE),
   ConnectionProxy: Object.keys(PROXY),
   ConnectionAttributes: Object.keys(CONNECTION_ATTRIBUTES),
};

/**
 * The nested objects DucklakeConnection declares inline rather than as named
 * schemas, so the spec can hold them against the contract too. Keyed by the
 * dotted path under that schema.
 */
export const PUBLIC_FIELDS_BY_INLINE_PATH: Readonly<
   Record<string, readonly string[]>
> = {
   "DucklakeConnection.storage": Object.keys(DUCKLAKE.storage as PublicShape),
   "DucklakeConnection.catalog": Object.keys(DUCKLAKE.catalog as PublicShape),
};

/** Keys that address an object's prototype rather than its own fields. */
const UNSAFE_KEYS: ReadonlySet<string> = new Set([
   "__proto__",
   "constructor",
   "prototype",
]);

function isPlainObject(value: unknown): value is Record<string, unknown> {
   return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asArraySpec(
   spec: PublicField,
): { kind: "array"; shape: PublicShape } | undefined {
   if (typeof spec === "string") return undefined;
   const candidate = spec as { kind?: unknown; shape?: unknown };
   return candidate.kind === "array" && isPlainObject(candidate.shape)
      ? (spec as { kind: "array"; shape: PublicShape })
      : undefined;
}

function projectField(value: unknown, spec: PublicField): unknown {
   if (spec === "any") return value;
   if (spec === "scalar") {
      // A field declared scalar that arrives as an object or array is dropped
      // rather than emitted: its actual shape is unreviewed, so it may carry
      // credentials in fields nobody has looked at.
      return typeof value === "object" && value !== null ? undefined : value;
   }
   const array = asArraySpec(spec);
   if (array) {
      if (!Array.isArray(value)) return undefined;
      return value.map((item) => projectShape(item, array.shape) ?? {});
   }
   return projectShape(value, spec as PublicShape);
}

function projectShape(
   value: unknown,
   shape: PublicShape,
): Record<string, unknown> | undefined {
   if (!isPlainObject(value)) return undefined;
   const out: Record<string, unknown> = {};
   for (const [field, spec] of Object.entries(shape)) {
      if (!Object.hasOwn(value, field)) continue;
      const projected = projectField(value[field], spec);
      if (projected !== undefined) out[field] = projected;
   }
   return out;
}

/**
 * A copy without `withheldFields`, tolerating a null or non-object input. The
 * spread it replaced accepted null, so this has to as well: a stored config can
 * be absent.
 */
function withoutWithheldFields(value: unknown): Record<string, unknown> {
   if (!isPlainObject(value)) return {};
   const copy = { ...value };
   delete copy["withheldFields"];
   return copy;
}

/**
 * Dotted paths of the withheld leaves, flattened out of `hiddenFields`. Names
 * only: this is what lets a client tell a credential that is set from one that
 * is absent, without being able to read either.
 */
function flattenWithheld(
   hidden: Record<string, unknown>,
   prefix: string,
   out: string[],
): void {
   for (const [key, value] of Object.entries(hidden)) {
      const path = prefix ? `${prefix}.${key}` : key;
      if (Array.isArray(value)) {
         for (const item of value) {
            if (!isPlainObject(item)) continue;
            // `name` is the identity marker hiddenFields carries for matching,
            // not a withheld field, so it labels the path instead of appearing
            // in it.
            const { name, ...rest } = item;
            const label = typeof name === "string" ? `${path}.${name}` : path;
            flattenWithheld(rest, label, out);
         }
      } else if (isPlainObject(value)) {
         flattenWithheld(value, path, out);
      } else if (value !== undefined && value !== null && value !== "") {
         out.push(path);
      }
   }
}

/**
 * The connection as the API is allowed to return it. Credentials are absent,
 * not masked, and `withheldFields` names the ones that are set.
 */
export function toPublicConnection(connection: ApiConnection): ApiConnection {
   // Server-computed, so a stored copy of it (from a client that sent one) is
   // dropped rather than trusted or reported as withheld.
   const stored = withoutWithheldFields(connection);
   const view = (projectShape(stored, PUBLIC_CONNECTION) ?? {}) as Record<
      string,
      unknown
   >;
   const withheld: string[] = [];
   flattenWithheld(hiddenFields(stored, PUBLIC_CONNECTION) ?? {}, "", withheld);
   if (withheld.length > 0) view.withheldFields = withheld.sort();
   return view as ApiConnection;
}

export function toPublicConnections(
   connections: readonly ApiConnection[] | undefined,
): ApiConnection[] {
   return Array.isArray(connections) ? connections.map(toPublicConnection) : [];
}

/**
 * The fields of `value` that `toPublicConnection` would NOT return, as a sparse
 * object. Derived from the same shape as the projection so the two cannot
 * drift: what the API hides is exactly what an update preserves.
 */
function hiddenFields(
   value: unknown,
   shape: PublicShape,
): Record<string, unknown> | undefined {
   if (!isPlainObject(value)) return undefined;
   const hidden: Record<string, unknown> = {};
   for (const [field, fieldValue] of Object.entries(value)) {
      if (UNSAFE_KEYS.has(field)) continue;
      // Own-property lookup, not `shape[field]`: a stored config carrying a
      // `__proto__` or `constructor` key would otherwise resolve to
      // Object.prototype rather than to "unlisted", and one level deeper to
      // null, which threw. Reachable from a POSTed config, and this runs on the
      // read path, so the throw would have 500'd every connection read.
      const spec: PublicField | undefined = Object.hasOwn(shape, field)
         ? shape[field]
         : undefined;
      if (spec === undefined || spec === null) {
         // Unlisted: the projection drops it whole, so it is hidden whole.
         hidden[field] = fieldValue;
         continue;
      }
      if (spec === "any") continue;
      if (spec === "scalar") {
         // Mirrors projectField: a declared-scalar field holding an object is
         // dropped from the response, so it counts as hidden.
         if (typeof fieldValue === "object" && fieldValue !== null) {
            hidden[field] = fieldValue;
         }
         continue;
      }
      const array = asArraySpec(spec);
      if (array) {
         if (!Array.isArray(fieldValue)) {
            hidden[field] = fieldValue;
            continue;
         }
         const items = fieldValue.map((item) => {
            const itemHidden = hiddenFields(item, array.shape) ?? {};
            if (Object.keys(itemHidden).length === 0) return {};
            // Carry the entry's name so reinstate matches by identity rather
            // than by position. The name is public and already in the patch;
            // it is here only to identify which entry these secrets belong to.
            return isNamed(item)
               ? { name: item.name, ...itemHidden }
               : itemHidden;
         });
         if (items.some((item) => Object.keys(item).length > 0)) {
            hidden[field] = items;
         }
         continue;
      }
      const nested = hiddenFields(fieldValue, spec as PublicShape);
      if (nested && Object.keys(nested).length > 0) hidden[field] = nested;
   }
   return hidden;
}

function isNamed(value: unknown): value is { name: string } {
   return isPlainObject(value) && typeof value["name"] === "string";
}

/**
 * Credential slots that are alternatives to one another, and how a caller
 * selects one.
 *
 * This exists because the connect path picks by PRESENCE, not by what the
 * caller most recently meant: `buildPostgresConnectionString` returns
 * `connectionString` if set, `hasSnowflakePrivateKey` routes to key-pair auth
 * if a private key is set, `hasS3` wins over GCS, and Trino's `peakaKey` short
 * circuits before `password` is even read. Carrying a remnant of the old method
 * forward would silently beat the method just configured, and the caller cannot
 * clear what it cannot see.
 *
 * `selects` is deliberately narrow: only a CREDENTIAL the caller actually
 * supplied, non-empty, selects a slot. Two reasons, both learned by getting it
 * wrong. An empty string is an untouched form box, not a choice, and treating
 * it as one destroyed the stored credential of any connection described solely
 * by a field the form cannot display. And a non-secret field is echoed back on
 * every write by any client that resubmits what it read, so it says nothing
 * about intent: `oauthClientId` came back on every Databricks patch and kept
 * the OAuth slot permanently selected.
 *
 * `fields` is what gets dropped when a sibling slot is selected. Only hidden
 * fields can be dropped, so listing a public field here would be a no-op.
 */
interface CredentialSlot {
   readonly selects: readonly string[];
   readonly fields: readonly string[];
}

const EXCLUSIVE_SLOTS: readonly (readonly CredentialSlot[])[] = [
   // Postgres: a connection string describes the whole connection.
   [
      { selects: ["connectionString"], fields: ["connectionString"] },
      { selects: ["password"], fields: ["password"] },
   ],
   // Snowflake: key-pair auth versus password.
   [
      { selects: ["privateKey"], fields: ["privateKey", "privateKeyPass"] },
      { selects: ["password"], fields: ["password"] },
   ],
   // Trino: peakaKey short circuits and password is never passed.
   [
      { selects: ["peakaKey"], fields: ["peakaKey"] },
      { selects: ["password"], fields: ["password"] },
   ],
   // Object store for DuckLake storage and DuckDB attached databases.
   [
      { selects: ["s3Connection"], fields: ["s3Connection"] },
      { selects: ["gcsConnection"], fields: ["gcsConnection"] },
      { selects: ["azureConnection"], fields: ["azureConnection"] },
   ],
   // Databricks: the driver prefers OAuth over a PAT, so a stale OAuth secret
   // would keep authenticating as the old app. Selected by the secret only,
   // never by oauthClientId, which is public and comes back on every write.
   [
      { selects: ["token"], fields: ["token"] },
      {
         selects: ["oauthClientSecret"],
         fields: ["oauthClientId", "oauthClientSecret"],
      },
   ],
];

/** A value the caller actually supplied, as opposed to an untouched blank. */
function isSupplied(value: unknown): boolean {
   if (value === undefined || value === null || value === "") return false;
   if (isPlainObject(value)) return Object.keys(value).length > 0;
   return true;
}

/**
 * Hidden field names to leave behind, because the patch selected a sibling
 * slot. Only when exactly one slot is selected: selecting several is explicit
 * enough to take at face value, and selecting none is a plain omission.
 */
function exclusionsFor(patch: Record<string, unknown>): ReadonlySet<string> {
   const skip = new Set<string>();
   for (const slots of EXCLUSIVE_SLOTS) {
      const selected = slots.filter((slot) =>
         slot.selects.some(
            (field) => Object.hasOwn(patch, field) && isSupplied(patch[field]),
         ),
      );
      if (selected.length !== 1) continue;
      for (const slot of slots) {
         if (slot === selected[0]) continue;
         for (const field of slot.fields) skip.add(field);
      }
   }
   return skip;
}

/**
 * Graft `hidden` back onto `patch` wherever the patch did not speak for itself.
 * A value the patch supplies always wins, including an explicit empty string,
 * which is how a caller clears a credential.
 */
function reinstate(patch: unknown, hidden: unknown): unknown {
   if (!isPlainObject(hidden)) return patch;
   // An explicit null is a replacement, not an omission, so it clears the
   // sub-object and everything hidden inside it. `{}` keeps them: it is what a
   // caller sends when it has nothing to say, and erring toward keeping a
   // credential beats erring toward destroying one.
   if (patch === null) return null;
   if (!isPlainObject(patch)) return hidden;
   const skip = exclusionsFor(patch);
   const out: Record<string, unknown> = { ...patch };
   for (const [field, hiddenValue] of Object.entries(hidden)) {
      // Never write a key that would re-parent the object being built rather
      // than adding a field to it.
      if (UNSAFE_KEYS.has(field)) continue;
      if (skip.has(field)) continue;
      if (!Object.hasOwn(patch, field)) {
         out[field] = hiddenValue;
         continue;
      }
      const patchValue = patch[field];
      if (Array.isArray(hiddenValue) && Array.isArray(patchValue)) {
         // Match array entries by name rather than by index: a caller may
         // reorder, add, or drop attached databases. An entry we cannot
         // identify keeps only what the patch sent, because grafting a
         // credential onto the wrong database is worse than making the caller
         // resend it.
         out[field] = patchValue.map((item) => {
            if (!isNamed(item)) return item;
            const match = hiddenValue.find(
               (candidate) =>
                  isNamed(candidate) && candidate.name === item.name,
            );
            return match ? reinstate(item, match) : item;
         });
         continue;
      }
      out[field] = reinstate(patchValue, hiddenValue);
   }
   return out;
}

/**
 * Merge a connection update over its stored config.
 *
 * The response path omits credentials, so a caller cannot echo back what it
 * never received. Without this, an update that legitimately sends only the
 * fields it can see would replace the sub-object holding the credential and
 * destroy it. Every field the API hides is carried forward unless the update
 * names it explicitly, which keeps "absent means unchanged" true for exactly
 * the fields a caller cannot read.
 */
export function mergeConnectionUpdate(
   stored: ApiConnection,
   patch: Partial<ApiConnection>,
): ApiConnection {
   // Read-only and server-computed, so it is not part of what gets stored.
   const incoming = withoutWithheldFields(patch);
   const current = withoutWithheldFields(stored);
   // The top-level shallow spread is the pre-existing update semantics, kept as
   // it was: a stored field the patch does not mention survives.
   const shallow = { ...current, ...incoming } as Record<string, unknown>;
   const hidden = hiddenFields(current, PUBLIC_CONNECTION);
   if (!hidden || Object.keys(hidden).length === 0) {
      return shallow as ApiConnection;
   }
   // What the shallow spread cannot do is reach a credential nested inside a
   // sub-object the patch replaced wholesale.
   return reinstate(shallow, hidden) as ApiConnection;
}
