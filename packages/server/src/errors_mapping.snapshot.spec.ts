// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MalloyError, type LogMessage } from "@malloydata/malloy";
import { beforeAll, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import * as errors from "./errors";
import {
   AccessDeniedError,
   AgentNotFoundError,
   BadRequestError,
   BuildPlanUnavailableError,
   ChainedShapeNotCarriedError,
   ChainedUpstreamMissingError,
   CompileRefusedError,
   ConflictError,
   ConnectionAuthError,
   ConnectionError,
   ConnectionFailedError,
   ConnectionNotFoundError,
   ConnectionPoolExhaustedError,
   DashboardNotFoundError,
   DestinationNotFoundError,
   DryRunCapacityError,
   EnvironmentNotFoundError,
   FrozenConfigError,
   internalErrorToHttpError,
   InvalidArgumentError,
   InvalidModelPathError,
   InvalidQueryRequestError,
   InvalidStateTransitionError,
   MaterializationConflictError,
   MaterializationEligibilityError,
   MaterializationNotFoundError,
   ModelCompilationError,
   ModelNotFoundError,
   NotImplementedError,
   NotQueryableError,
   OffSurfaceError,
   PackageAdmissionRefusedError,
   PackageManifestError,
   PackageNotFoundError,
   PackageVersionError,
   type PackageVersionErrorReason,
   PayloadTooLargeError,
   PublisherConfigError,
   QueryCompileError,
   QueryTimeoutError,
   RenderTagRefusedError,
   ResponseUnserializableError,
   ServerConfigurationError,
   ServiceUnavailableError,
   SkillNotFoundError,
   StorageCatalogNotFoundError,
   StorageDestinationNotFoundError,
   TableNotFoundError,
   UnconfiguredConnectionError,
   UnparseableTextError,
   UnsupportedCatalogFormatError,
   VersionFilesMissingError,
   WriteConflictError,
   WriteRollbackFailedError,
   WriteRolledBackError,
   WriteVerifyError,
} from "./errors";
import { logger } from "./logger";
import { classifyToolError, getModelForQuery } from "./mcp/handler_utils";
import { AuthorizeGrammarError } from "./service/authorize_grammar";
import { fakeOf } from "./test_helpers/store_fakes";
import type { PackageLookup } from "./service/environment_ports";

/**
 * Pins how every error the server throws reaches a caller, on both transports:
 *
 *  - REST: internalErrorToHttpError's status and body, and whether it logged.
 *  - MCP: classifyToolError's message and suggestions (and what it logged), and
 *    getModelForQuery's setup path, whose private setupIdentifier decides
 *    whether a setup failure is classified or reported as internal.
 *
 * One entry per exported error class in errors.ts (more where a field or the
 * message changes the answer: every PackageVersionError reason, each message
 * pattern getMalloyErrorDetails recognizes), plus the non-class inputs the
 * mappers branch on. The golden is the contract the error-table refactor must
 * leave byte-identical. Regenerate after an INTENDED change, then review its
 * diff:
 *
 *    UPDATE_GOLDEN=1 bun test src/errors_mapping.snapshot.spec.ts
 */
const GOLDEN_PATH = path.join(
   __dirname,
   "testdata",
   "errors_mapping.golden.json",
);

const ENV = "env";
const PKG = "pkg";
const MODEL = "m.malloy";
const IDENTIFIER = `${ENV}/${PKG}/${MODEL}`;
const OPERATION = "executeQuery";

function errno(code: string, syscall: string, filePath: string): Error {
   return Object.assign(
      new Error(`${code}: refused, ${syscall} '${filePath}'`),
      { code, syscall, path: filePath, errno: -13 },
   );
}

function problem(code: string, message: string): LogMessage {
   return {
      message,
      severity: "error",
      code,
      at: {
         url: "internal://query.malloy",
         range: {
            start: { line: 0, character: 6 },
            end: { line: 0, character: 12 },
         },
      },
   } as LogMessage;
}

// Typed exhaustively, so a new reason fails the typecheck until it has a case.
const VERSION_REASONS: Record<PackageVersionErrorReason, true> = {
   MANIFEST_VERSION_MISSING: true,
   MANIFEST_VERSION_INVALID: true,
   VERSION_ID_INVALID: true,
   VERSION_NOT_FOUND: true,
   VERSION_CONFLICT: true,
   PACKAGE_IS_VERSIONED: true,
   VERSION_IS_LATEST: true,
   VERSION_IS_LAST_ACTIVE: true,
   VERSION_BUILDING: true,
   VERSION_NOT_LATEST: true,
   VERSION_ARCHIVED: true,
};

const CASES: Array<[string, () => unknown]> = [
   ...(Object.keys(VERSION_REASONS) as PackageVersionErrorReason[]).map(
      (reason): [string, () => unknown] => [
         `PackageVersionError ${reason}`,
         () => new PackageVersionError(reason, `version refused: ${reason}`),
      ],
   ),
   [
      "BuildPlanUnavailableError",
      () => new BuildPlanUnavailableError("build plan could not be derived"),
   ],
   [
      "NotImplementedError",
      () => new NotImplementedError("versionId is not supported"),
   ],
   ["BadRequestError", () => new BadRequestError("bad request")],
   [
      "BadRequestError view not found",
      () => new BadRequestError("View 'by_month' not found in source 'orders'"),
   ],
   [
      "BadRequestError source not found",
      () => new BadRequestError("Source 'orderz' not found"),
   ],
   [
      "BadRequestError query not found",
      () => new BadRequestError("Query 'top_ten' not found"),
   ],
   [
      "BadRequestError field not found",
      () => new BadRequestError("Field 'revenu' not found in source 'orders'"),
   ],
   [
      "BadRequestError undefined reference",
      () => new BadRequestError("Reference to undefined object 'revenu'"),
   ],
   [
      "BadRequestError syntax",
      () => new BadRequestError("no viable alternative at input 'run: ->'"),
   ],
   [
      "BadRequestError cannot connect",
      () => new BadRequestError("Cannot connect to database duckdb"),
   ],
   [
      "BadRequestError invalid query request",
      () =>
         new InvalidQueryRequestError(
            "Invalid query request. Query OR queryName must be defined",
         ),
   ],
   [
      "BadRequestError restricted by message",
      () =>
         new BadRequestError(
            "import statements cannot be used in a restricted query",
         ),
   ],
   [
      "InvalidArgumentError",
      () => new InvalidArgumentError("givens must be an object"),
   ],
   [
      "CompileRefusedError",
      () => new CompileRefusedError("append text may not declare a given"),
   ],
   [
      "InvalidModelPathError",
      () => new InvalidModelPathError("model path escapes the package"),
   ],
   [
      "RenderTagRefusedError",
      () => new RenderTagRefusedError("# link is refused in a document"),
   ],
   [
      "UnparseableTextError",
      () =>
         new UnparseableTextError("the text could not be parsed", "line 1:4"),
   ],
   [
      "EnvironmentNotFoundError",
      () => new EnvironmentNotFoundError("Environment env not found"),
   ],
   [
      "EnvironmentNotFoundError with lookup",
      () =>
         new EnvironmentNotFoundError("Environment env not found", {
            environmentName: ENV,
            availableEnvironments: ["examples", "other"],
         }),
   ],
   [
      "EnvironmentNotFoundError with empty lookup",
      () =>
         new EnvironmentNotFoundError("Environment env not found", {
            environmentName: ENV,
            availableEnvironments: [],
         }),
   ],
   [
      "PackageNotFoundError",
      () => new PackageNotFoundError("Package pkg not found"),
   ],
   [
      "PackageNotFoundError caused by EACCES",
      () =>
         new PackageNotFoundError("Package pkg not found", {
            cause: errno("EACCES", "open", "/srv/publisher_data/env/pkg"),
         }),
   ],
   ["ModelNotFoundError", () => new ModelNotFoundError("Model not found")],
   [
      "DashboardNotFoundError",
      () => new DashboardNotFoundError("Report not found"),
   ],
   ["SkillNotFoundError", () => new SkillNotFoundError("Skill not found")],
   ["AgentNotFoundError", () => new AgentNotFoundError("Agent not found")],
   ["ConflictError", () => new ConflictError("already exists")],
   [
      "ConnectionNotFoundError",
      () => new ConnectionNotFoundError("Connection pg not found"),
   ],
   [
      "TableNotFoundError",
      () => new TableNotFoundError('relation "orders" does not exist'),
   ],
   [
      "ConnectionError",
      () => new ConnectionError("connect ECONNREFUSED 10.0.0.5:5432"),
   ],
   [
      "ConnectionError callerSafe",
      () =>
         new ConnectionError("The connection is read-only.", {
            callerSafe: true,
         }),
   ],
   [
      "ConnectionFailedError",
      () => new ConnectionFailedError("getaddrinfo ENOTFOUND db.internal"),
   ],
   ["UnconfiguredConnectionError", () => new UnconfiguredConnectionError("pg")],
   [
      "ConnectionPoolExhaustedError",
      () => new ConnectionPoolExhaustedError("The connection pool is full."),
   ],
   [
      "DestinationNotFoundError",
      () => new DestinationNotFoundError("No destination named warehouse"),
   ],
   [
      "StorageDestinationNotFoundError",
      () => new StorageDestinationNotFoundError("No storage destination"),
   ],
   [
      "StorageCatalogNotFoundError",
      () => new StorageCatalogNotFoundError("No storage catalog"),
   ],
   [
      "ConnectionAuthError",
      () =>
         new ConnectionAuthError(
            'password authentication failed for user "analytics"',
         ),
   ],
   [
      "UnsupportedCatalogFormatError",
      () => new UnsupportedCatalogFormatError("iceberg is not supported"),
   ],
   [
      "ModelCompilationError",
      () => new ModelCompilationError({ message: "model does not compile" }),
   ],
   [
      "VersionFilesMissingError",
      () => new VersionFilesMissingError("version files are missing"),
   ],
   [
      "PackageManifestError",
      () => new PackageManifestError("publisher.json: version is not semver"),
   ],
   [
      "MaterializationEligibilityError",
      () =>
         new MaterializationEligibilityError({
            message: "source reads a given",
            reason: "given",
         }),
   ],
   [
      "ChainedUpstreamMissingError",
      () =>
         new ChainedUpstreamMissingError(["orders"], "upstream orders missing"),
   ],
   [
      "ChainedShapeNotCarriedError",
      () => new ChainedShapeNotCarriedError("shape not carried"),
   ],
   [
      "PublisherConfigError",
      () =>
         new PublisherConfigError(
            "publisher.config.json",
            new SyntaxError("Unexpected token } in JSON"),
         ),
   ],
   [
      "ServerConfigurationError",
      () => new ServerConfigurationError("credentials path is a directory"),
   ],
   ["FrozenConfigError", () => new FrozenConfigError()],
   ["AccessDeniedError", () => new AccessDeniedError("access refused")],
   [
      "AccessDeniedError authorize denial",
      () =>
         new AccessDeniedError(
            'Access denied for source "orders": its authorize gate was not satisfied.',
            { source: "orders" },
         ),
   ],
   [
      "QueryCompileError",
      () =>
         new QueryCompileError("query does not compile", [
            problem("field-not-found", "'revenu' is not defined"),
         ]),
   ],
   [
      "QueryCompileError restricted",
      () =>
         new QueryCompileError("query does not compile", [
            problem(
               "restricted-construct-forbidden",
               "import statements cannot be used in a restricted query",
            ),
            problem("field-not-found", "'x' is not defined"),
         ]),
   ],
   [
      "NotQueryableError",
      () => new NotQueryableError("No queryable source named orders"),
   ],
   [
      "OffSurfaceError",
      () =>
         new OffSurfaceError(
            "Model staging.malloy is not published by this package.",
         ),
   ],
   [
      "MaterializationNotFoundError",
      () => new MaterializationNotFoundError("No materialization m1"),
   ],
   [
      "MaterializationConflictError",
      () => new MaterializationConflictError("a build is running"),
   ],
   ["WriteConflictError", () => new WriteConflictError("file changed")],
   [
      "WriteRolledBackError",
      () =>
         new WriteRolledBackError("the reload failed; restored", {
            cause: new Error("compile failed"),
         }),
   ],
   [
      "WriteRollbackFailedError",
      () => new WriteRollbackFailedError("the restore failed"),
   ],
   ["WriteVerifyError", () => new WriteVerifyError("written text differs")],
   [
      "InvalidStateTransitionError",
      () => new InvalidStateTransitionError("cannot go from built to pending"),
   ],
   [
      "ServiceUnavailableError",
      () => new ServiceUnavailableError("memory limit reached"),
   ],
   [
      "PackageAdmissionRefusedError",
      () => new PackageAdmissionRefusedError("package load refused"),
   ],
   [
      "DryRunCapacityError",
      () => new DryRunCapacityError("dry-run capacity reached"),
   ],
   [
      "PayloadTooLargeError",
      () => new PayloadTooLargeError("result exceeds the byte cap"),
   ],
   [
      "ResponseUnserializableError",
      () => new ResponseUnserializableError("result cannot be serialized"),
   ],
   ["QueryTimeoutError", () => new QueryTimeoutError("query timed out")],
   // Not classes of errors.ts, but inputs the mappers branch on.
   [
      "AuthorizeGrammarError (service/authorize_grammar.ts)",
      () => new AuthorizeGrammarError("compound_boolean", "authorize: or"),
   ],
   [
      "MalloyError",
      () =>
         new MalloyError("model error", [
            problem("field-not-found", "'x' is not defined"),
         ]),
   ],
   ["Error", () => new Error("something broke at /srv/secret/path")],
   ["Error EACCES", () => errno("EACCES", "mkdir", "/srv/publisher_data/env")],
   [
      "body-parser entity.too.large",
      () =>
         Object.assign(new Error("request entity too large"), {
            type: "entity.too.large",
            status: 413,
            expose: true,
         }),
   ],
   ["non-Error value", () => "a thrown string"],
];

type Logged = Array<[string, string]>;

/** Runs `fn`, returning what it logged at warn and error, by summary. */
async function capturingLogs<T>(
   fn: () => T | Promise<T>,
): Promise<{ value: T; logs: Logged }> {
   const logs: Logged = [];
   const warn = spyOn(logger, "warn").mockImplementation(((summary: string) => {
      logs.push(["warn", summary]);
      return logger;
   }) as never);
   const error = spyOn(logger, "error").mockImplementation(((
      summary: string,
   ) => {
      logs.push(["error", summary]);
      return logger;
   }) as never);
   try {
      return { value: await fn(), logs };
   } finally {
      warn.mockRestore();
      error.mockRestore();
   }
}

async function mapError(make: () => unknown) {
   const sample = make();
   let http: unknown = null; // internalErrorToHttpError takes an Error only
   if (sample instanceof Error) {
      let logged = false;
      const { value, logs } = await capturingLogs(() =>
         internalErrorToHttpError(make() as Error, {
            onLogged: () => (logged = true),
         }),
      );
      http = { ...value, logged, logs };
   }
   const mcp = await capturingLogs(() =>
      classifyToolError(OPERATION, IDENTIFIER, make()),
   );
   const setup = await capturingLogs(async () => {
      const thrown = make();
      const store = fakeOf<PackageLookup>({
         getEnvironment: async () => {
            throw thrown;
         },
      });
      const result = await getModelForQuery(store, ENV, PKG, MODEL);
      if (!("error" in result)) throw new Error("expected a setup failure");
      expect(result.cause).toBe(thrown);
      return result.error;
   });
   return {
      class: sample instanceof Error ? sample.constructor.name : null,
      http,
      mcp: { ...mcp.value, logs: mcp.logs },
      mcpSetup: { ...setup.value, logs: setup.logs },
   };
}

describe("error mapping snapshot", () => {
   let rendered: string;
   let snapshot: Record<string, Awaited<ReturnType<typeof mapError>>>;

   beforeAll(async () => {
      snapshot = {};
      for (const [name, make] of CASES) {
         expect(snapshot[name]).toBeUndefined();
         snapshot[name] = await mapError(make);
      }
      rendered = `${JSON.stringify(snapshot, null, 2)}\n`;
      if (process.env.UPDATE_GOLDEN === "1") {
         fs.mkdirSync(path.dirname(GOLDEN_PATH), { recursive: true });
         fs.writeFileSync(GOLDEN_PATH, rendered);
      }
   });

   it("matches the committed golden", () => {
      const golden = fs
         .readFileSync(GOLDEN_PATH, "utf8")
         .replace(/\r\n/g, "\n");
      expect(rendered).toBe(golden);
   });

   it("covers every error class errors.ts exports", () => {
      const exported = Object.entries(errors).filter(
         ([, value]) =>
            typeof value === "function" &&
            (value as { prototype?: unknown }).prototype instanceof Error,
      );
      expect(exported.length).toBeGreaterThan(50);
      const covered = new Set(
         CASES.map(([, make]) => make())
            .filter((e): e is Error => e instanceof Error)
            .map((e) => e.constructor),
      );
      // A new class needs a CASES entry (and a regenerated golden) here.
      const missing = exported
         .filter(([, cls]) => !covered.has(cls as ErrorConstructor))
         .map(([name]) => name);
      expect(missing).toEqual([]);
   });

   it("gives every exported error class its own kind and table row", () => {
      // The typecheck holds the table to the kinds; this holds the classes to
      // the kinds. A subclass that does not override `kind` would silently
      // answer from its parent's row.
      const registered = new Map<unknown, string>(
         Object.entries(errors.ERROR_CLASSES_FOR_TESTS).map(([kind, cls]) => [
            cls,
            kind,
         ]),
      );
      const problems: string[] = [];
      for (const [name, value] of Object.entries(errors)) {
         if (
            typeof value !== "function" ||
            !((value as { prototype?: unknown }).prototype instanceof Error)
         ) {
            continue;
         }
         const kind = registered.get(value);
         const own = Object.getOwnPropertyDescriptor(value.prototype, "kind");
         if (kind === undefined) problems.push(`${name}: not registered`);
         else if (!own?.get) problems.push(`${name}: no own kind`);
         else if (own.get.call(value.prototype) !== kind) {
            problems.push(`${name}: kind is not ${kind}`);
         } else if (!(kind in errors.ERROR_TABLE)) {
            problems.push(`${name}: no row for ${kind}`);
         }
      }
      expect(problems).toEqual([]);
      for (const [, make] of CASES) {
         const error = make();
         const kind = errors.errorKind(error);
         if (error instanceof Error && registered.has(error.constructor)) {
            expect(kind as string).toBe(registered.get(error.constructor)!);
         }
      }
   });
});
