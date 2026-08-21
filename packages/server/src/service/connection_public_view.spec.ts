import { describe, expect, it } from "bun:test";
import { readFileSync } from "fs";
import { resolve } from "path";
import { components } from "../api";
import {
   PUBLIC_FIELDS_BY_INLINE_PATH,
   PUBLIC_FIELDS_BY_SCHEMA,
   mergeConnectionUpdate,
   toPublicConnection,
   toPublicConnections,
} from "./connection_public_view";

type ApiConnection = components["schemas"]["Connection"];

const SENTINEL = "SENTINEL-SECRET-DO-NOT-RETURN";

/**
 * Every property api-doc.yaml declares on a connection schema is either public
 * (allowlisted in connection_public_view.ts) or a credential listed here. A
 * field added to the contract belongs to neither until someone says which, and
 * the parity test below fails until they do. That is the whole safety property:
 * the next connection type cannot ship a readable credential by omission.
 */
/**
 * Returned, but not projected from the stored config: the server computes it
 * from what the projection withheld. A third category, so the parity test below
 * still fails on a field that is genuinely unclassified.
 */
const COMPUTED_FIELDS: Readonly<Record<string, readonly string[]>> = {
   Connection: ["withheldFields"],
};

const KNOWN_SECRET_FIELDS: Readonly<Record<string, readonly string[]>> = {
   Connection: [],
   PostgresConnection: ["password", "connectionString"],
   BigqueryConnection: ["serviceAccountKeyJson"],
   SnowflakeConnection: ["password", "privateKey", "privateKeyPass"],
   TrinoConnection: ["password", "peakaKey"],
   DatabricksConnection: ["token", "oauthClientSecret"],
   MysqlConnection: ["password"],
   DuckdbConnection: [],
   MotherDuckConnection: ["accessToken"],
   DucklakeConnection: [],
   PublisherConnection: ["accessToken"],
   S3Connection: ["secretAccessKey", "sessionToken"],
   GCSConnection: ["secret"],
   AzureConnection: ["sasUrl", "clientSecret"],
   SshProxyConfig: ["privateKey", "privateKeyPass"],
   AttachedDatabase: [],
   ConnectionProxy: [],
   ConnectionAttributes: [],
};

/** Same split, for the objects DucklakeConnection declares inline. */
const KNOWN_SECRET_INLINE_FIELDS: Readonly<Record<string, readonly string[]>> =
   {
      "DucklakeConnection.storage": [],
      "DucklakeConnection.catalog": [],
   };

/**
 * Property names a schema declares, read out of api-doc.yaml as text. Same
 * approach as theme_key_parity.spec.ts: the contract is the shared artifact and
 * there is no YAML parser in this package's dependencies.
 */
function contractProperties(apiDoc: string, schemaName: string): string[] {
   const start = apiDoc.indexOf(`\n    ${schemaName}:\n`);
   if (start === -1) throw new Error(`schema ${schemaName} not found`);
   const rest = apiDoc.slice(start + 1);
   // Schema names carry digits (S3Connection, GCSConnection); a name-only
   // pattern here silently ran one schema block into the next.
   const nextSchema = rest.search(/\n {4}[A-Za-z][A-Za-z0-9]*:\n/);
   const block = nextSchema === -1 ? rest : rest.slice(0, nextSchema);
   const propsAt = block.indexOf("\n      properties:\n");
   if (propsAt === -1) return [];
   const propsBlock = block.slice(propsAt + 1);
   return [...propsBlock.matchAll(/\n {8}([A-Za-z][A-Za-z0-9_]*):/g)].map(
      (match) => match[1],
   );
}

/**
 * Property names of an object a schema declares INLINE rather than by $ref.
 * Their keys sit at 12 spaces, not 8, which is why the schema-level extractor
 * above cannot see them and a credential added there once passed unnoticed.
 */
function inlineProperties(
   apiDoc: string,
   schemaName: string,
   fieldName: string,
): string[] {
   const start = apiDoc.indexOf(`\n    ${schemaName}:\n`);
   if (start === -1) throw new Error(`schema ${schemaName} not found`);
   const rest = apiDoc.slice(start + 1);
   const nextSchema = rest.search(/\n {4}[A-Za-z][A-Za-z0-9]*:\n/);
   const block = nextSchema === -1 ? rest : rest.slice(0, nextSchema);
   const fieldAt = block.indexOf(`\n        ${fieldName}:\n`);
   if (fieldAt === -1) throw new Error(`${schemaName}.${fieldName} not found`);
   const afterField = block.slice(fieldAt + 1);
   const nextField = afterField.search(/\n {8}[A-Za-z][A-Za-z0-9]*:/);
   const fieldBlock =
      nextField === -1 ? afterField : afterField.slice(0, nextField);
   const propsAt = fieldBlock.indexOf("\n          properties:\n");
   if (propsAt === -1) return [];
   return [
      ...fieldBlock
         .slice(propsAt + 1)
         .matchAll(/\n {12}([A-Za-z][A-Za-z0-9_]*):/g),
   ].map((match) => match[1]);
}

function readApiDoc(): string {
   return readFileSync(resolve(import.meta.dir, "../../../../api-doc.yaml"), {
      encoding: "utf8",
   });
}

/** Every string anywhere in a JSON-serializable value. */
function allStrings(value: unknown, found: string[] = []): string[] {
   if (typeof value === "string") found.push(value);
   else if (Array.isArray(value)) value.forEach((v) => allStrings(v, found));
   else if (value && typeof value === "object") {
      Object.values(value).forEach((v) => allStrings(v, found));
   }
   return found;
}

describe("connection public view: contract parity", () => {
   const apiDoc = readApiDoc();

   for (const schemaName of Object.keys(PUBLIC_FIELDS_BY_SCHEMA)) {
      it(`classifies every ${schemaName} property as public or secret`, () => {
         const declared = contractProperties(apiDoc, schemaName);
         expect(declared.length).toBeGreaterThan(0);
         const classified = new Set([
            ...PUBLIC_FIELDS_BY_SCHEMA[schemaName],
            ...KNOWN_SECRET_FIELDS[schemaName],
            ...(COMPUTED_FIELDS[schemaName] ?? []),
         ]);
         const unclassified = declared.filter((p) => !classified.has(p));
         expect(unclassified).toEqual([]);
      });

      it(`does not allowlist a known ${schemaName} credential`, () => {
         const publicFields = new Set(PUBLIC_FIELDS_BY_SCHEMA[schemaName]);
         const leaked = KNOWN_SECRET_FIELDS[schemaName].filter((f) =>
            publicFields.has(f),
         );
         expect(leaked).toEqual([]);
      });
   }

   for (const path of Object.keys(PUBLIC_FIELDS_BY_INLINE_PATH)) {
      it(`classifies every ${path} property as public or secret`, () => {
         const [schemaName, fieldName] = path.split(".");
         const declared = inlineProperties(apiDoc, schemaName, fieldName);
         expect(declared.length).toBeGreaterThan(0);
         const classified = new Set([
            ...PUBLIC_FIELDS_BY_INLINE_PATH[path],
            ...KNOWN_SECRET_INLINE_FIELDS[path],
         ]);
         expect(declared.filter((p) => !classified.has(p))).toEqual([]);
      });
   }

   it("registers every schema reachable from Connection", () => {
      // The parity checks above iterate a hand-maintained map, so on their own
      // they prove nothing about a schema nobody added to it: the next
      // connection type could ship an allowlist that publishes its password and
      // the suite would stay green. This walks the contract instead, so the
      // guarantee is "everything reachable is classified" rather than
      // "everything listed is classified".
      const blockOf = (name: string): string => {
         const start = apiDoc.indexOf(`\n    ${name}:\n`);
         if (start === -1) throw new Error(`schema ${name} not found`);
         const rest = apiDoc.slice(start + 1);
         const next = rest.search(/\n {4}[A-Za-z][A-Za-z0-9]*:\n/);
         return next === -1 ? rest : rest.slice(0, next);
      };

      const seen = new Set<string>();
      const queue = ["Connection"];
      while (queue.length > 0) {
         const name = queue.shift() as string;
         if (seen.has(name)) continue;
         seen.add(name);
         for (const match of blockOf(name).matchAll(
            /\$ref: "#\/components\/schemas\/([A-Za-z][A-Za-z0-9]*)"/g,
         )) {
            queue.push(match[1]);
         }
      }

      expect(seen.has("PostgresConnection")).toBe(true);
      expect(seen.size).toBeGreaterThan(10);

      // QueryMetadata is a free-form string bag the contract documents as
      // non-secret and round-trips verbatim, so it is mapped to "any" rather
      // than field-by-field and has no allowlist to hold against the contract.
      const notAConnectionConfig = new Set(["QueryMetadata"]);
      const unregistered = [...seen].filter(
         (name) =>
            !Object.hasOwn(PUBLIC_FIELDS_BY_SCHEMA, name) &&
            !notAConnectionConfig.has(name),
      );
      expect(unregistered).toEqual([]);
   });

   it("has a shape for every connection type in the contract enum", () => {
      // A new connection type has to be given a shape, or its whole config
      // (credentials included) would be dropped silently rather than served.
      const connectionBlock = apiDoc.slice(apiDoc.indexOf("\n    Connection:"));
      const enumStart = connectionBlock.indexOf("enum:");
      const enumBlock = connectionBlock.slice(
         enumStart,
         connectionBlock.indexOf("]", enumStart),
      );
      const types = [...enumBlock.matchAll(/\n\s+([a-z]+),?/g)].map(
         (m) => m[1],
      );
      expect(types).toContain("postgres");
      expect(types.length).toBeGreaterThanOrEqual(10);

      const publicTop = new Set(PUBLIC_FIELDS_BY_SCHEMA.Connection);
      for (const type of types) {
         const field =
            type === "motherduck"
               ? "motherduckConnection"
               : `${type}Connection`;
         expect(publicTop.has(field)).toBe(true);
      }
   });
});

describe("toPublicConnection", () => {
   it("keeps the public fields of a postgres connection", () => {
      const view = toPublicConnection({
         name: "pg",
         type: "postgres",
         resource: "/api/v0/connections/pg",
         postgresConnection: {
            host: "db.internal",
            port: 5432,
            databaseName: "analytics",
            userName: "readonly",
            password: SENTINEL,
         },
      } as ApiConnection);

      expect(view).toEqual({
         name: "pg",
         type: "postgres",
         resource: "/api/v0/connections/pg",
         postgresConnection: {
            host: "db.internal",
            port: 5432,
            databaseName: "analytics",
            userName: "readonly",
         },
         withheldFields: ["postgresConnection.password"],
      } as ApiConnection);
   });

   it("omits the credential rather than masking it", () => {
      // A masked placeholder would be echoed back by any caller that
      // round-trips what it reads, and stored as the real credential.
      const view = toPublicConnection({
         name: "pg",
         type: "postgres",
         postgresConnection: { host: "h", password: SENTINEL },
      } as ApiConnection);
      const pg = (view as { postgresConnection: Record<string, unknown> })
         .postgresConnection;
      expect("password" in pg).toBe(false);
   });

   it("drops a field nobody has allowlisted", () => {
      // The fail-safe property: a credential field added to a future connection
      // type is invisible until someone deliberately publishes it.
      const view = toPublicConnection({
         name: "pg",
         type: "postgres",
         postgresConnection: { host: "h", futureSecret: SENTINEL },
         futureTopLevelSecret: SENTINEL,
      } as unknown as ApiConnection);
      expect(allStrings(view)).not.toContain(SENTINEL);
   });

   it("drops a declared-scalar field that arrives as an object", () => {
      const view = toPublicConnection({
         name: "pg",
         type: "postgres",
         postgresConnection: { host: { nested: SENTINEL } },
      } as unknown as ApiConnection);
      expect(allStrings(view)).not.toContain(SENTINEL);
   });

   it("passes through the query metadata bags the contract calls non-secret", () => {
      const view = toPublicConnection({
         name: "pg",
         type: "postgres",
         queryMetadata: { team: "growth" },
         queryMetadataEnforced: { tenant: "acme" },
      } as unknown as ApiConnection);
      expect(view.queryMetadata).toEqual({ team: "growth" });
      expect(view.queryMetadataEnforced).toEqual({ tenant: "acme" });
   });

   it("keeps a bastion's public host key and drops its private key", () => {
      const view = toPublicConnection({
         name: "pg",
         type: "postgres",
         proxy: {
            type: "ssh",
            ssh: {
               host: "bastion",
               port: 22,
               username: "tunnel",
               hostKey: "ssh-ed25519 AAAA-public",
               privateKey: SENTINEL,
               privateKeyPass: SENTINEL,
            },
         },
      } as unknown as ApiConnection);
      const ssh = (
         view as unknown as { proxy: { ssh: Record<string, unknown> } }
      ).proxy.ssh;
      expect(ssh.hostKey).toBe("ssh-ed25519 AAAA-public");
      expect(allStrings(view)).not.toContain(SENTINEL);
   });

   it("strips credentials nested in duckdb attached databases", () => {
      const view = toPublicConnection({
         name: "dd",
         type: "duckdb",
         duckdbConnection: {
            attachedDatabases: [
               {
                  name: "warehouse",
                  type: "snowflake",
                  snowflakeConnection: {
                     account: "acct",
                     privateKey: SENTINEL,
                     password: SENTINEL,
                  },
               },
               {
                  name: "lake",
                  type: "s3",
                  s3Connection: {
                     region: "us-east-1",
                     accessKeyId: "AKIAPUBLIC",
                     secretAccessKey: SENTINEL,
                     sessionToken: SENTINEL,
                  },
               },
            ],
         },
      } as unknown as ApiConnection);

      expect(allStrings(view)).not.toContain(SENTINEL);
      const dbs = (
         view as unknown as {
            duckdbConnection: { attachedDatabases: Record<string, unknown>[] };
         }
      ).duckdbConnection.attachedDatabases;
      expect(dbs.length).toBe(2);
      expect(dbs[0].name).toBe("warehouse");
      expect((dbs[1].s3Connection as Record<string, unknown>).accessKeyId).toBe(
         "AKIAPUBLIC",
      );
   });

   it("strips the ducklake catalog password and keeps the bucket url", () => {
      const view = toPublicConnection({
         name: "lake",
         type: "ducklake",
         ducklakeConnection: {
            storage: {
               bucketUrl: "s3://bucket/prefix",
               s3Connection: { region: "eu-west-1", secretAccessKey: SENTINEL },
            },
            catalog: {
               metadataSchema: "ducklake_meta",
               postgresConnection: { host: "catalog.db", password: SENTINEL },
            },
         },
      } as unknown as ApiConnection);

      expect(allStrings(view)).not.toContain(SENTINEL);
      const dl = (
         view as unknown as {
            ducklakeConnection: {
               storage: Record<string, unknown>;
               catalog: Record<string, unknown>;
            };
         }
      ).ducklakeConnection;
      expect(dl.storage.bucketUrl).toBe("s3://bucket/prefix");
      expect(dl.catalog.metadataSchema).toBe("ducklake_meta");
   });

   it("returns no credential for a connection of every contract type", () => {
      // One fixture per type, every credential field that type declares set to
      // the sentinel, so a shape that forgets one fails here.
      const fixtures: ApiConnection[] = [
         {
            name: "a",
            type: "postgres",
            postgresConnection: {
               password: SENTINEL,
               connectionString: SENTINEL,
            },
         },
         {
            name: "b",
            type: "bigquery",
            bigqueryConnection: { serviceAccountKeyJson: SENTINEL },
         },
         {
            name: "c",
            type: "snowflake",
            snowflakeConnection: {
               password: SENTINEL,
               privateKey: SENTINEL,
               privateKeyPass: SENTINEL,
            },
         },
         {
            name: "d",
            type: "trino",
            trinoConnection: { password: SENTINEL, peakaKey: SENTINEL },
         },
         {
            name: "e",
            type: "databricks",
            databricksConnection: {
               token: SENTINEL,
               oauthClientSecret: SENTINEL,
            },
         },
         {
            name: "f",
            type: "mysql",
            mysqlConnection: { password: SENTINEL },
         },
         {
            name: "g",
            type: "motherduck",
            motherduckConnection: { accessToken: SENTINEL },
         },
         {
            name: "h",
            type: "publisher",
            publisherConnection: { accessToken: SENTINEL },
         },
      ] as unknown as ApiConnection[];

      for (const fixture of fixtures) {
         expect(allStrings(toPublicConnection(fixture))).not.toContain(
            SENTINEL,
         );
      }
   });

   it("maps a list and tolerates a missing one", () => {
      expect(toPublicConnections(undefined)).toEqual([]);
      expect(
         toPublicConnections([
            {
               name: "pg",
               type: "postgres",
               postgresConnection: { password: SENTINEL },
            },
         ] as unknown as ApiConnection[]),
      ).toEqual([
         {
            name: "pg",
            type: "postgres",
            postgresConnection: {},
            withheldFields: ["postgresConnection.password"],
         },
      ]);
   });
});

describe("withheldFields", () => {
   it("names the withheld credential without returning it", () => {
      const view = toPublicConnection({
         name: "pg",
         type: "postgres",
         postgresConnection: {
            host: "db.internal",
            password: SENTINEL,
            connectionString: SENTINEL,
         },
      } as ApiConnection);

      expect(view.withheldFields).toEqual([
         "postgresConnection.connectionString",
         "postgresConnection.password",
      ]);
      // Names only. This is the whole point: it must not become a second way
      // to read the value it describes.
      expect(allStrings(view)).not.toContain(SENTINEL);
   });

   it("is absent when the connection holds no credential", () => {
      const view = toPublicConnection({
         name: "pg",
         type: "postgres",
         postgresConnection: { host: "db.internal", port: 5432 },
      } as ApiConnection);
      expect(view.withheldFields).toBeUndefined();
   });

   it("distinguishes a set credential from an absent one", () => {
      // The distinction the connection editor needs: an empty box means "keep
      // what is stored" only when something is stored.
      const withCred = toPublicConnection({
         name: "a",
         type: "postgres",
         postgresConnection: { connectionString: SENTINEL },
      } as ApiConnection);
      const withoutCred = toPublicConnection({
         name: "b",
         type: "postgres",
         postgresConnection: { host: "h" },
      } as ApiConnection);
      expect(withCred.withheldFields).toContain(
         "postgresConnection.connectionString",
      );
      expect(withoutCred.withheldFields ?? []).not.toContain(
         "postgresConnection.connectionString",
      );
   });

   it("labels an attached database's credential by name, not index", () => {
      const view = toPublicConnection({
         name: "dd",
         type: "duckdb",
         duckdbConnection: {
            attachedDatabases: [
               {
                  name: "lake",
                  type: "s3",
                  s3Connection: { secretAccessKey: SENTINEL },
               },
            ],
         },
      } as unknown as ApiConnection);
      expect(view.withheldFields).toEqual([
         "duckdbConnection.attachedDatabases.lake.s3Connection.secretAccessKey",
      ]);
   });

   it("does not report an empty stored credential as withheld", () => {
      const view = toPublicConnection({
         name: "pg",
         type: "postgres",
         postgresConnection: { host: "h", password: "" },
      } as ApiConnection);
      expect(view.withheldFields ?? []).not.toContain(
         "postgresConnection.password",
      );
   });

   it("ignores a withheldFields a client sent, and never stores one", () => {
      const view = toPublicConnection({
         name: "pg",
         type: "postgres",
         withheldFields: ["postgresConnection.somethingInvented"],
         postgresConnection: { host: "h" },
      } as unknown as ApiConnection);
      expect(view.withheldFields).toBeUndefined();

      const merged = mergeConnectionUpdate(
         {
            name: "pg",
            type: "postgres",
            postgresConnection: { host: "h", password: "stored" },
         } as ApiConnection,
         {
            withheldFields: ["anything"],
            postgresConnection: { host: "h2" },
         } as unknown as Partial<ApiConnection>,
      );
      expect("withheldFields" in (merged as object)).toBe(false);
      expect(
         (merged.postgresConnection as Record<string, unknown>).password,
      ).toBe("stored");
   });
});

describe("mergeConnectionUpdate", () => {
   const stored = {
      name: "pg",
      type: "postgres",
      postgresConnection: {
         host: "db.internal",
         port: 5432,
         userName: "readonly",
         password: "stored-secret",
      },
   } as ApiConnection;

   it("tolerates an absent stored config", () => {
      // The spread this replaced accepted null, and a stored config can be
      // absent, so losing that tolerance would crash the update path.
      expect(
         mergeConnectionUpdate(
            null as unknown as ApiConnection,
            {
               postgresConnection: { host: "h" },
            } as Partial<ApiConnection>,
         ),
      ).toEqual({ postgresConnection: { host: "h" } } as ApiConnection);
      expect(
         mergeConnectionUpdate(
            stored,
            null as unknown as Partial<ApiConnection>,
         ),
      ).toEqual(stored);
   });

   it("keeps the stored credential when the update omits it", () => {
      // The response never showed the password, so its absence here cannot mean
      // "clear it". Without this the edit would destroy the credential.
      const merged = mergeConnectionUpdate(stored, {
         postgresConnection: { host: "db.new", port: 5432 },
      } as Partial<ApiConnection>);
      expect(merged.postgresConnection).toEqual({
         host: "db.new",
         port: 5432,
         password: "stored-secret",
      });
   });

   it("takes a credential the update supplies", () => {
      const merged = mergeConnectionUpdate(stored, {
         postgresConnection: { host: "db.internal", password: "rotated" },
      } as Partial<ApiConnection>);
      expect(
         (merged.postgresConnection as Record<string, unknown>).password,
      ).toBe("rotated");
   });

   it("lets an explicit empty value clear a credential", () => {
      const merged = mergeConnectionUpdate(stored, {
         postgresConnection: { host: "db.internal", password: "" },
      } as Partial<ApiConnection>);
      expect(
         (merged.postgresConnection as Record<string, unknown>).password,
      ).toBe("");
   });

   it("keeps stored top-level fields the update does not mention", () => {
      const merged = mergeConnectionUpdate(
         stored,
         {} as Partial<ApiConnection>,
      );
      expect(merged.postgresConnection).toEqual({
         host: "db.internal",
         port: 5432,
         userName: "readonly",
         password: "stored-secret",
      });
   });

   it("does not resurrect a connection string when the patch re-points the host", () => {
      // The worst shape this can take: buildPostgresConnectionString returns
      // connectionString when set, so a resurrected one would keep the
      // connection talking to the OLD database while reporting success, and the
      // caller cannot clear a field it cannot read.
      const storedWithUri = {
         name: "pg",
         type: "postgres",
         postgresConnection: {
            connectionString: "postgresql://old:pw@oldhost/olddb",
         },
      } as ApiConnection;

      const merged = mergeConnectionUpdate(storedWithUri, {
         postgresConnection: {
            host: "newhost",
            port: 5432,
            databaseName: "newdb",
            userName: "u",
            password: "newpw",
         },
      } as Partial<ApiConnection>);

      expect(merged.postgresConnection).toEqual({
         host: "newhost",
         port: 5432,
         databaseName: "newdb",
         userName: "u",
         password: "newpw",
      });
   });

   it("does not resurrect a private key when the patch switches to a password", () => {
      // hasSnowflakePrivateKey routes to key-pair auth on presence, so a stale
      // key would beat the new password and make rotating off a revoked key
      // impossible through the API.
      const storedKeyPair = {
         name: "sf",
         type: "snowflake",
         snowflakeConnection: {
            account: "a",
            username: "u",
            warehouse: "w",
            privateKey: "OLD-KEY",
            privateKeyPass: "OLD-PASS",
         },
      } as ApiConnection;

      const merged = mergeConnectionUpdate(storedKeyPair, {
         snowflakeConnection: {
            account: "a",
            username: "u",
            warehouse: "w",
            password: "newpw",
         },
      } as Partial<ApiConnection>);

      const sf = merged.snowflakeConnection as Record<string, unknown>;
      expect(sf.password).toBe("newpw");
      expect(sf.privateKey).toBeUndefined();
      expect(sf.privateKeyPass).toBeUndefined();
   });

   it("does not resurrect an s3 secret when the patch switches storage to gcs", () => {
      // hasS3 wins over GCS by presence, so the remnant would both override the
      // new provider and fail the attach for a missing accessKeyId.
      const storedLakeS3 = {
         name: "lake",
         type: "ducklake",
         ducklakeConnection: {
            storage: {
               bucketUrl: "s3://old",
               s3Connection: { accessKeyId: "AK", secretAccessKey: "SECRET" },
            },
         },
      } as unknown as ApiConnection;

      const merged = mergeConnectionUpdate(storedLakeS3, {
         ducklakeConnection: {
            storage: {
               bucketUrl: "gs://new",
               gcsConnection: { keyId: "KID", secret: "GCSSECRET" },
            },
         },
      } as unknown as Partial<ApiConnection>);

      const storage = (
         merged as unknown as {
            ducklakeConnection: { storage: Record<string, unknown> };
         }
      ).ducklakeConnection.storage;
      expect(storage.bucketUrl).toBe("gs://new");
      expect(storage.s3Connection).toBeUndefined();
   });

   it("keeps a connection string when the patch supplies only untouched blanks", () => {
      // The regression this exclusivity rule itself introduced. A connection
      // described solely by a connection string projects to an empty
      // postgresConnection, so the editor renders every visible box blank and
      // submits empty strings for them. Reading an empty box as "the caller
      // chose the discrete form" destroyed the only credential the connection
      // had, and answered 200.
      const storedUri = {
         name: "pg",
         type: "postgres",
         postgresConnection: { connectionString: "postgresql://u:p@h/db" },
      } as ApiConnection;

      const merged = mergeConnectionUpdate(storedUri, {
         postgresConnection: {
            host: "",
            port: "",
            databaseName: "",
            userName: "",
         },
      } as unknown as Partial<ApiConnection>);

      expect(
         (merged.postgresConnection as Record<string, unknown>)
            .connectionString,
      ).toBe("postgresql://u:p@h/db");
   });

   it("does not resurrect a peaka key when the patch switches to a password", () => {
      // Trino returns early on peakaKey and never reads password, so a stale
      // key silently ignores the new credential.
      const storedPeaka = {
         name: "tr",
         type: "trino",
         trinoConnection: {
            server: "https://s",
            user: "u",
            peakaKey: "PK_OLD",
         },
      } as unknown as ApiConnection;

      const merged = mergeConnectionUpdate(storedPeaka, {
         trinoConnection: { server: "https://s", user: "u", password: "newpw" },
      } as unknown as Partial<ApiConnection>);

      const trino = merged.trinoConnection as Record<string, unknown>;
      expect(trino.password).toBe("newpw");
      expect(trino.peakaKey).toBeUndefined();
   });

   it("drops a stale oauth secret even though the client echoes the public client id", () => {
      // oauthClientId is public, so it comes back on every write. Selecting the
      // OAuth slot on it kept that slot permanently selected and the stale
      // secret alive, and the driver prefers OAuth over a PAT.
      const storedOauth = {
         name: "db",
         type: "databricks",
         databricksConnection: {
            host: "h",
            path: "/p",
            oauthClientId: "cid",
            oauthClientSecret: "SEC_OLD",
         },
      } as unknown as ApiConnection;

      const merged = mergeConnectionUpdate(storedOauth, {
         databricksConnection: {
            host: "h",
            path: "/p",
            oauthClientId: "cid",
            token: "NEW-PAT",
         },
      } as unknown as Partial<ApiConnection>);

      const db = merged.databricksConnection as Record<string, unknown>;
      expect(db.token).toBe("NEW-PAT");
      expect(db.oauthClientSecret).toBeUndefined();
   });

   it("does not resurrect a databricks token when the patch switches to OAuth", () => {
      const storedToken = {
         name: "db",
         type: "databricks",
         databricksConnection: {
            host: "h",
            path: "/p",
            defaultCatalog: "c",
            token: "OLD-TOKEN",
         },
      } as unknown as ApiConnection;

      const merged = mergeConnectionUpdate(storedToken, {
         databricksConnection: {
            host: "h",
            path: "/p",
            defaultCatalog: "c",
            oauthClientId: "cid",
            oauthClientSecret: "csecret",
         },
      } as unknown as Partial<ApiConnection>);

      const db = merged.databricksConnection as Record<string, unknown>;
      expect(db.oauthClientId).toBe("cid");
      expect(db.token).toBeUndefined();
   });

   it("keeps a databricks token when the patch touches neither credential", () => {
      const storedToken = {
         name: "db",
         type: "databricks",
         databricksConnection: { host: "h", path: "/p", token: "KEEP-ME" },
      } as unknown as ApiConnection;
      const merged = mergeConnectionUpdate(storedToken, {
         databricksConnection: { host: "h2", path: "/p" },
      } as unknown as Partial<ApiConnection>);
      expect(
         (merged.databricksConnection as Record<string, unknown>).token,
      ).toBe("KEEP-ME");
   });

   it("keeps an ssh proxy private key when the patch edits the bastion host", () => {
      // The exclusivity rules match by field name at every level, and host/port
      // also name the postgres detail group, so this is the case where a rule
      // could misfire and drop a credential it should keep.
      const storedProxy = {
         name: "pg",
         type: "postgres",
         postgresConnection: { host: "db", password: "PW" },
         proxy: {
            type: "ssh",
            ssh: {
               host: "bastion",
               port: 22,
               username: "u",
               privateKey: "SSH-KEY",
            },
         },
      } as unknown as ApiConnection;

      const merged = mergeConnectionUpdate(storedProxy, {
         proxy: {
            type: "ssh",
            ssh: { host: "bastion2", port: 22, username: "u" },
         },
      } as unknown as Partial<ApiConnection>);

      const ssh = (
         merged as unknown as { proxy: { ssh: Record<string, unknown> } }
      ).proxy.ssh;
      expect(ssh.host).toBe("bastion2");
      expect(ssh.privateKey).toBe("SSH-KEY");
   });

   it("keeps a mysql and a trino password when the patch edits other fields", () => {
      const storedMysql = {
         name: "my",
         type: "mysql",
         mysqlConnection: {
            host: "h",
            port: 3306,
            user: "u",
            password: "MYPW",
         },
      } as unknown as ApiConnection;
      expect(
         (
            mergeConnectionUpdate(storedMysql, {
               mysqlConnection: { host: "h2", port: 3306, user: "u" },
            } as unknown as Partial<ApiConnection>).mysqlConnection as Record<
               string,
               unknown
            >
         ).password,
      ).toBe("MYPW");

      const storedTrino = {
         name: "tr",
         type: "trino",
         trinoConnection: {
            server: "https://s",
            port: 443,
            user: "u",
            password: "TRPW",
            peakaKey: "PK",
         },
      } as unknown as ApiConnection;
      const trino = mergeConnectionUpdate(storedTrino, {
         trinoConnection: { server: "https://s2", port: 443, user: "u" },
      } as unknown as Partial<ApiConnection>).trinoConnection as Record<
         string,
         unknown
      >;
      expect(trino.password).toBe("TRPW");
      expect(trino.peakaKey).toBe("PK");
   });

   it("still keeps the credential when the patch stays on the same method", () => {
      // The exclusivity rule must not break the case the whole change exists
      // for: same method, credential omitted, credential kept.
      const merged = mergeConnectionUpdate(stored, {
         postgresConnection: {
            host: "db.internal",
            port: 5432,
            databaseName: "d",
            userName: "readonly",
         },
      } as Partial<ApiConnection>);
      expect(
         (merged.postgresConnection as Record<string, unknown>).password,
      ).toBe("stored-secret");
   });

   it("treats an explicit null as clearing the sub-object", () => {
      const merged = mergeConnectionUpdate(stored, {
         postgresConnection: null,
      } as unknown as Partial<ApiConnection>);
      expect(merged.postgresConnection).toBeNull();
   });

   it("survives a stored config carrying prototype-addressing keys", () => {
      // Reachable: a POSTed config round-trips through JSON storage, and the
      // read path walks the same table, so a throw here would 500 every
      // connection read for the environment.
      const nasty = JSON.parse(
         '{"name":"pg","type":"postgres","__proto__":{"__proto__":{"a":1}},' +
            '"postgresConnection":{"host":"h","password":"P"}}',
      ) as ApiConnection;
      expect(() => toPublicConnection(nasty)).not.toThrow();
      expect(() => mergeConnectionUpdate(nasty, {})).not.toThrow();
      expect(
         (Object.prototype as unknown as Record<string, unknown>).a,
      ).toBeUndefined();
   });

   it("matches attached databases by name, not by position", () => {
      const storedDuck = {
         name: "dd",
         type: "duckdb",
         duckdbConnection: {
            attachedDatabases: [
               {
                  name: "one",
                  type: "s3",
                  s3Connection: { secretAccessKey: "s1" },
               },
               {
                  name: "two",
                  type: "s3",
                  s3Connection: { secretAccessKey: "s2" },
               },
            ],
         },
      } as unknown as ApiConnection;

      // Reordered, and neither entry can carry the secret it never received.
      const merged = mergeConnectionUpdate(storedDuck, {
         duckdbConnection: {
            attachedDatabases: [
               { name: "two", type: "s3", s3Connection: { region: "eu" } },
               { name: "one", type: "s3", s3Connection: { region: "us" } },
            ],
         },
      } as unknown as Partial<ApiConnection>);

      const dbs = (
         merged as unknown as {
            duckdbConnection: { attachedDatabases: Record<string, unknown>[] };
         }
      ).duckdbConnection.attachedDatabases;
      expect(
         (dbs[0].s3Connection as Record<string, unknown>).secretAccessKey,
      ).toBe("s2");
      expect(
         (dbs[1].s3Connection as Record<string, unknown>).secretAccessKey,
      ).toBe("s1");
   });

   it("does not graft a credential onto an unidentifiable entry", () => {
      const storedDuck = {
         name: "dd",
         type: "duckdb",
         duckdbConnection: {
            attachedDatabases: [
               {
                  name: "one",
                  type: "s3",
                  s3Connection: { secretAccessKey: "s1" },
               },
            ],
         },
      } as unknown as ApiConnection;

      const merged = mergeConnectionUpdate(storedDuck, {
         duckdbConnection: {
            attachedDatabases: [{ type: "s3", s3Connection: { region: "us" } }],
         },
      } as unknown as Partial<ApiConnection>);

      const dbs = (
         merged as unknown as {
            duckdbConnection: { attachedDatabases: Record<string, unknown>[] };
         }
      ).duckdbConnection.attachedDatabases;
      expect(
         (dbs[0].s3Connection as Record<string, unknown>).secretAccessKey,
      ).toBeUndefined();
   });

   it("preserves a nested ducklake catalog password", () => {
      const storedLake = {
         name: "lake",
         type: "ducklake",
         ducklakeConnection: {
            storage: { bucketUrl: "s3://b/p" },
            catalog: {
               postgresConnection: { host: "c.db", password: "catalog-secret" },
            },
         },
      } as unknown as ApiConnection;

      const merged = mergeConnectionUpdate(storedLake, {
         ducklakeConnection: {
            storage: { bucketUrl: "s3://b/p2" },
            catalog: { postgresConnection: { host: "c.db" } },
         },
      } as unknown as Partial<ApiConnection>);

      const dl = (
         merged as unknown as {
            ducklakeConnection: {
               storage: Record<string, unknown>;
               catalog: { postgresConnection: Record<string, unknown> };
            };
         }
      ).ducklakeConnection;
      expect(dl.storage.bucketUrl).toBe("s3://b/p2");
      expect(dl.catalog.postgresConnection.password).toBe("catalog-secret");
   });

   it("round-trips what the API returned without losing the credential", () => {
      // The path that matters: read a connection over the API, send it back
      // with one field changed, and the credential survives.
      const asRead = toPublicConnection(stored);
      const merged = mergeConnectionUpdate(stored, {
         ...asRead,
         postgresConnection: {
            ...(asRead.postgresConnection as Record<string, unknown>),
            userName: "writer",
         },
      } as unknown as Partial<ApiConnection>);
      expect(merged.postgresConnection).toEqual({
         host: "db.internal",
         port: 5432,
         userName: "writer",
         password: "stored-secret",
      });
   });
});
