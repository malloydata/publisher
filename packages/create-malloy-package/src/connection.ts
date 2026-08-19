import { ScaffoldError } from "./errors";
import { preview } from "./names";

/**
 * The warehouses a first run can be pointed at. Publisher's connection `type`
 * enum is wider than this (trino, databricks, mysql, motherduck, ducklake,
 * publisher), and a config naming any of them is perfectly valid; this is the
 * set the scaffolder knows how to author, not the set Publisher supports.
 * Adding one here is a dialect table entry and a template, nothing more.
 */
export const WAREHOUSE_TYPES = ["postgres", "bigquery", "snowflake"] as const;

export type WarehouseType = (typeof WAREHOUSE_TYPES)[number];

export function isWarehouseType(value: string): value is WarehouseType {
   return (WAREHOUSE_TYPES as readonly string[]).includes(value);
}

/**
 * The name every connection this tool writes has to satisfy: the INTERSECTION of
 * what Publisher accepts and what Credible's `cred add connection` accepts, not
 * either one alone.
 *
 * Publisher enforces no character rule at all — it reserves the single name
 * `duckdb` and takes anything else. Credible's CLI validates each name against
 * this pattern and, on a name that fails, SKIPS THAT CONNECTION AND CARRIES ON
 * (cli/src/commands/connection/createconnections.ts). So a perfectly working
 * local connection called `my-warehouse` is silently dropped on the way to
 * Credible, per connection, with the command reporting success for the rest.
 *
 * Emitting only the intersection is what makes the connection block this tool
 * writes portable. Note the `+` rather than `*`: a one-character name fails,
 * which is why `x` is not a legal connection name here even though nothing in
 * Publisher objects to it.
 */
const PORTABLE_CONNECTION_NAME = /^[A-Za-z_][A-Za-z0-9_]+$/;

/**
 * Reserved by Publisher for the per-package DuckDB sandbox every loaded package
 * gets automatically. Checked before the type dispatch on the server side
 * (packages/server/src/service/connection_config.ts), which is why it applies to
 * a Postgres connection called `duckdb` just as much as to a DuckDB one.
 *
 * Measured on 0.0.244: the server still starts, but the whole ENVIRONMENT is
 * skipped — `environments=0 packages=0 load_errors=1` — so with the single
 * environment this tool writes, nothing is served at all. Unlike the unset-${VAR}
 * case above, this one is properly diagnosed: the reason appears in loadErrors
 * and on the packages endpoint verbatim.
 */
const RESERVED_CONNECTION_NAME = "duckdb";

/**
 * Environment-variable names Publisher will actually substitute. The server's
 * pattern is /\$\{([A-Z_][A-Z0-9_]*)\}/g (packages/server/src/config.ts), so it
 * is UPPERCASE-ONLY. Both ways of getting it wrong fail quietly:
 *
 * - A lowercase ${my_password} is not substituted, is not an error, and travels
 *   to the driver as those literal characters.
 * - An UNSET ${VAR} does not stop the server. Measured on 0.0.244: it boots,
 *   prints PUBLISHER_READY, and reports environments=0 packages=0
 *   load_errors=0, with the underlying error reaching the log as an empty
 *   object. A server that serves nothing and reports no error.
 *
 * Every name this module generates is uppercased before it is written, so the
 * first case cannot arise from our side. The second is why the CLI checks
 * whether the variable is set and says so before the user starts anything.
 */
const SUBSTITUTABLE_ENV_VAR = /^[A-Z_][A-Z0-9_]*$/;

/**
 * Characters allowed in a `--table` path. Deliberately narrower than what every
 * warehouse permits: the value is interpolated into a single-quoted Malloy
 * string (`conn.table('...')`), Malloy escapes with a backslash rather than by
 * doubling, and a starter model that will not compile is worse than a table name
 * this tool declines to write. Hyphens are in the set because a BigQuery table
 * path routinely carries one in the project id.
 *
 * This errs toward refusing a legal-but-exotic name rather than emitting a
 * broken model, and says so when it refuses.
 */
const TABLE_PATH = /^[A-Za-z0-9_.-]+$/;

/** Postgres' own default, applied when --pg-port is omitted. */
const DEFAULT_POSTGRES_PORT = 5432;

/** Raw flag values, straight off the command line. */
export interface ConnectionFlags {
   connection: string;
   connectionName?: string;
   table?: string;
   pgHost?: string;
   pgPort?: string;
   pgDatabase?: string;
   pgUser?: string;
   bqProject?: string;
   bqLocation?: string;
   sfAccount?: string;
   sfUser?: string;
   sfWarehouse?: string;
   sfDatabase?: string;
   sfSchema?: string;
   sfRole?: string;
}

/** One environment variable the written config refers to and does not contain. */
export interface EnvVarRef {
   name: string;
   /** What it holds, for .env.example and the printed instructions. */
   describes: string;
}

/** A connection entry, in the shape environments[].connections[] takes. */
export interface ConnectionEntry {
   name: string;
   type: WarehouseType;
   [payload: string]: unknown;
}

export interface BuiltConnection {
   entry: ConnectionEntry;
   /** Empty for a dialect that authenticates ambiently, such as BigQuery ADC. */
   envVars: EnvVarRef[];
   /** The validated --table, absent when the user named none. */
   table?: string;
   /** How this dialect authenticates, for the closing output. */
   credentialNote: string;
}

/**
 * Which identity fields each dialect takes, which of them are required, and what
 * flag names them. Credentials are deliberately absent from this table: no
 * secret is accepted as a flag, so there is nothing here to hold one.
 */
interface FieldSpec {
   /** The key inside the `<type>Connection` payload. */
   key: string;
   /** The flag the user sets it with, for error messages. */
   flag: string;
   /** The property on ConnectionFlags this reads. */
   from: keyof ConnectionFlags;
   required: boolean;
}

/**
 * A flag a dialect owns but that is not in its `fields` table, because it is
 * applied by hand rather than copied straight across.
 *
 * This exists because leaving one out is not a cosmetic gap. Flag ownership
 * drives the "that option belongs to another dialect" check, so a flag missing
 * from both lists is owned by nobody, passes the check, and is then silently
 * dropped. `--pg-port` did exactly that: `--connection bigquery --pg-port 5432`
 * scaffolded a BigQuery connection and discarded the port without a word.
 */
interface ExtraFlag {
   from: keyof ConnectionFlags;
   flag: string;
}

const DIALECTS: Record<
   WarehouseType,
   {
      payloadKey: string;
      fields: FieldSpec[];
      extraFlags: ExtraFlag[];
      credentialNote: string;
   }
> = {
   postgres: {
      payloadKey: "postgresConnection",
      // --pg-port is not in `fields` because it has a default and is applied
      // below; it still has to be declared as postgres' own.
      extraFlags: [{ from: "pgPort", flag: "--pg-port" }],
      fields: [
         {
            key: "host",
            flag: "--pg-host",
            from: "pgHost",
            required: true,
         },
         {
            key: "databaseName",
            flag: "--pg-database",
            from: "pgDatabase",
            required: true,
         },
         {
            key: "userName",
            flag: "--pg-user",
            from: "pgUser",
            required: true,
         },
      ],
      credentialNote:
         "Postgres authenticates with a password, which is read from the " +
         "environment at boot and is not written into publisher.config.json.",
   },
   bigquery: {
      payloadKey: "bigqueryConnection",
      extraFlags: [],
      fields: [
         {
            key: "defaultProjectId",
            flag: "--bq-project",
            from: "bqProject",
            required: true,
         },
         {
            key: "location",
            flag: "--bq-location",
            from: "bqLocation",
            required: false,
         },
      ],
      // Publisher only reads serviceAccountKeyJson when it is set
      // (packages/server/src/service/connection.ts), falling through to
      // Application Default Credentials when it is absent. So omitting it is a
      // working configuration rather than an incomplete one.
      credentialNote:
         "BigQuery uses Application Default Credentials, so no key is written " +
         "anywhere. Run `gcloud auth application-default login` once if you " +
         "have not already.",
   },
   snowflake: {
      payloadKey: "snowflakeConnection",
      extraFlags: [],
      fields: [
         {
            key: "account",
            flag: "--sf-account",
            from: "sfAccount",
            required: true,
         },
         {
            key: "username",
            flag: "--sf-user",
            from: "sfUser",
            required: true,
         },
         {
            key: "warehouse",
            flag: "--sf-warehouse",
            from: "sfWarehouse",
            required: true,
         },
         {
            key: "database",
            flag: "--sf-database",
            from: "sfDatabase",
            required: false,
         },
         {
            key: "schema",
            flag: "--sf-schema",
            from: "sfSchema",
            required: false,
         },
         {
            key: "role",
            flag: "--sf-role",
            from: "sfRole",
            required: false,
         },
      ],
      credentialNote:
         "Snowflake authenticates with a password, which is read from the " +
         "environment at boot and is not written into publisher.config.json.",
   },
};

/**
 * Every flag that only means something with --connection: the dialect-specific
 * ones plus the two that apply to any dialect.
 *
 * Exported so the CLI can reject them when --connection is absent without
 * keeping a second hand-written list of the same names. That duplication is
 * what let `--pg-port` fall between the two checks.
 */
export function connectionFlags(): ExtraFlag[] {
   return [
      { from: "connectionName", flag: "--connection-name" },
      { from: "table", flag: "--table" },
      ...WAREHOUSE_TYPES.flatMap((type) => ownedFlags(type)),
   ];
}

/** Every flag a dialect owns: its payload fields plus its hand-applied extras. */
export function ownedFlags(type: WarehouseType): ExtraFlag[] {
   return [
      ...DIALECTS[type].fields.map((field) => ({
         from: field.from,
         flag: field.flag,
      })),
      ...DIALECTS[type].extraFlags,
   ];
}

/** The flags that belong to a dialect, for the "wrong dialect" error. */
function flagsFor(type: WarehouseType): string[] {
   return ownedFlags(type).map((flag) => flag.flag);
}

/**
 * Reject a flag from one dialect passed alongside another's. Without this,
 * `--connection bigquery --pg-host localhost` scaffolds a BigQuery connection
 * and drops the host on the floor, and the user finds out at first query.
 */
function assertNoForeignFlags(
   flags: ConnectionFlags,
   type: WarehouseType,
): void {
   const own = new Set(ownedFlags(type).map((flag) => flag.from));
   for (const other of WAREHOUSE_TYPES) {
      if (other === type) {
         continue;
      }
      // ownedFlags, not `fields`: a flag applied outside the field table is
      // still that dialect's, and one that appears in neither list is owned by
      // nobody, passes this check, and is dropped in silence.
      for (const candidate of ownedFlags(other)) {
         if (own.has(candidate.from)) {
            continue;
         }
         if (flags[candidate.from] !== undefined) {
            throw new ScaffoldError(
               `${candidate.flag} is a ${other} option, but --connection is ` +
                  `"${type}". Nothing would read it, so no connection was ` +
                  `written.\n\n` +
                  `The ${type} options are: ${flagsFor(type).join(", ")}.`,
            );
         }
      }
   }
}

/**
 * The connection name, validated against the portable intersection rather than
 * sanitized into it. Sanitizing silently is the wrong move here: the name is
 * written into the model as `<name>.table(...)`, printed in the instructions,
 * and typed by the user afterwards, so quietly turning `my-warehouse` into
 * `my_warehouse` leaves three places saying one thing and the user's fingers
 * doing another.
 */
export function resolveConnectionName(
   type: WarehouseType,
   requested?: string,
): string {
   // The type itself: always two or more characters, always letters, never
   // `duckdb`, and it reads correctly in the model (`postgres.table(...)`).
   const name = requested ?? type;
   if (name === RESERVED_CONNECTION_NAME) {
      throw new ScaffoldError(
         `A connection cannot be named "${RESERVED_CONNECTION_NAME}". That ` +
            `name is reserved for the per-package DuckDB sandbox every package ` +
            `gets automatically, and an environment-level connection using it ` +
            `does not just fail itself — Publisher fails the whole environment ` +
            `at startup, so the server serves nothing.\n\n` +
            `Choose another name with --connection-name (for example ` +
            `"warehouse" or "${type}").`,
      );
   }
   if (!PORTABLE_CONNECTION_NAME.test(name)) {
      throw new ScaffoldError(
         `Invalid connection name "${preview(name)}": use letters, digits and ` +
            `underscores, start with a letter or underscore, and use at least ` +
            `two characters.\n\n` +
            `Publisher itself would accept a wider set, but Credible's ` +
            `\`cred add connection\` accepts only this one and SKIPS a ` +
            `connection whose name it rejects rather than failing, so a name ` +
            `outside it works locally and then goes missing without an error ` +
            `when you publish. Hyphens are the usual cause: write ` +
            `"my_warehouse", not "my-warehouse".`,
      );
   }
   return name;
}

/**
 * The environment variable holding this connection's password. Uppercased so the
 * server's substitution pattern matches it, and prefixed so it cannot collide
 * with an unrelated variable already in the user's shell — `POSTGRES_PASSWORD`
 * on its own is the name Docker's own postgres image uses, which is exactly the
 * sort of accidental sharing that makes a wrong credential hard to see.
 */
export function passwordEnvVar(connectionName: string): string {
   const name = `MALLOY_${connectionName.toUpperCase()}_PASSWORD`;
   /* istanbul ignore next -- unreachable: resolveConnectionName has already
      constrained the name to characters that uppercase into this set. Asserted
      rather than assumed, because a name that fails it is not a broken variable,
      it is a variable Publisher silently declines to substitute. */
   if (!SUBSTITUTABLE_ENV_VAR.test(name)) {
      throw new ScaffoldError(
         `Could not derive an environment variable name from connection ` +
            `"${preview(connectionName)}".`,
      );
   }
   return name;
}

/** The `${VAR}` reference written into the config in place of the secret. */
export function envVarReference(name: string): string {
   return `\${${name}}`;
}

function validatePort(raw: string): number {
   const port = Number(raw);
   if (!Number.isInteger(port) || port < 1 || port > 65535) {
      throw new ScaffoldError(
         `--pg-port must be a whole number between 1 and 65535, not ` +
            `"${preview(raw)}".`,
      );
   }
   return port;
}

/**
 * The `--table` value, checked against what can be written into a Malloy string
 * literal without escaping. Returns undefined when none was given, which is a
 * supported outcome: the connection is written and the model is left as a stub
 * for the agent to fill in from the real schema.
 */
export function validateTablePath(table?: string): string | undefined {
   if (table === undefined) {
      return undefined;
   }
   if (table === "") {
      throw new ScaffoldError(
         `--table was given an empty value. Pass the table to model, such as ` +
            `--table public.orders, or leave --table off and the model is ` +
            `scaffolded as a stub you fill in once you know what is there.`,
      );
   }
   if (!TABLE_PATH.test(table)) {
      throw new ScaffoldError(
         `Invalid table path "${preview(table)}": use letters, digits, ` +
            `underscores, dots and hyphens.\n\n` +
            `The value is written into the starter model as ` +
            `\`table('...')\`, so a character that ends the string early ` +
            `produces a model that does not compile. If your table genuinely ` +
            `contains something outside that set, leave --table off and write ` +
            `the source by hand.`,
      );
   }
   return table;
}

/**
 * Build the connection entry from the flags, or throw a message naming the flag
 * the user has to add. Pure: it writes nothing and reads no environment, so the
 * caller can validate a whole invocation before creating anything on disk.
 */
export function buildConnection(flags: ConnectionFlags): BuiltConnection {
   if (!isWarehouseType(flags.connection)) {
      throw new ScaffoldError(
         `--connection must be one of: ${WAREHOUSE_TYPES.join(", ")}. ` +
            `Got "${preview(flags.connection)}".`,
      );
   }
   const type = flags.connection;
   const dialect = DIALECTS[type];
   assertNoForeignFlags(flags, type);

   const name = resolveConnectionName(type, flags.connectionName);
   const table = validateTablePath(flags.table);

   const payload: Record<string, unknown> = {};
   const missing: FieldSpec[] = [];
   for (const field of dialect.fields) {
      const value = flags[field.from];
      if (value === undefined || value === "") {
         if (field.required) {
            missing.push(field);
         }
         continue;
      }
      payload[field.key] = value;
   }
   if (missing.length > 0) {
      throw new ScaffoldError(
         `--connection ${type} needs ${missing.length === 1 ? "one more option" : "these options"}: ` +
            `${missing.map((field) => field.flag).join(", ")}.\n\n` +
            `Nothing was written. The full set for ${type} is: ` +
            `${flagsFor(type).join(", ")}.`,
      );
   }

   // Postgres' port is the one identity field with a sensible default, so it is
   // handled outside the table rather than being made required.
   if (type === "postgres") {
      payload.port =
         flags.pgPort === undefined
            ? DEFAULT_POSTGRES_PORT
            : validatePort(flags.pgPort);
   }

   const envVars: EnvVarRef[] = [];
   if (type === "postgres" || type === "snowflake") {
      const variable = passwordEnvVar(name);
      payload.password = envVarReference(variable);
      // preview(), because this string is written into .env.example as a
      // comment line and the user name behind it is under no character rule at
      // all. A newline in it would end the comment and put whatever follows on
      // its own line of a file that is read line by line. The identity fields
      // are written into JSON everywhere else, where the encoder handles it;
      // this is the one place a raw flag value reaches a line-oriented format.
      envVars.push({
         name: variable,
         describes: `the password for ${type} user "${preview(
            (type === "postgres" ? flags.pgUser : flags.sfUser) ?? "",
         )}"`,
      });
   }

   return {
      entry: { name, type, [dialect.payloadKey]: payload },
      envVars,
      table,
      credentialNote: dialect.credentialNote,
   };
}

/**
 * The .env.example body: every variable the config refers to, with no values.
 * Written next to the config so the names are discoverable without reading the
 * config for `${...}` by eye, and so the file can be committed while the real
 * .env beside it is not.
 */
export function renderEnvExample(built: BuiltConnection): string {
   const lines = [
      "# Credentials for the Malloy Publisher connection.",
      "#",
      "# Copy this file to .env, fill in the values, and export them before",
      "# starting the server. publisher.config.json refers to these by name and",
      "# never contains the values themselves.",
      "",
   ];
   for (const variable of built.envVars) {
      lines.push(`# ${variable.describes}`);
      lines.push(`${variable.name}=`);
      lines.push("");
   }
   return lines.join("\n");
}
