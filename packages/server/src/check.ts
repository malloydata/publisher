// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Check mode (`--check`, `PUBLISHER_CHECK`): boot as the server would, load
 * and compile every configured package, print each problem, and exit. Binds
 * no port. What server.ts runs instead of the server when `check` is on.
 *
 * Output, so CI can read it:
 * - stdout: one line per finding, `<where>: <severity>: <message>`, where
 *   `<where>` is `file:line:col` (the file relative to the working
 *   directory) when the finding has a position, and `<env>/<package>` when
 *   it has none. A finding in a file ends with ` [<env>/<package>]`.
 * - stderr: one summary line, `PUBLISHER_CHECK_OK` or
 *   `PUBLISHER_CHECK_FAILED`, with the counts as `key=value` pairs, like
 *   `PUBLISHER_READY`.
 * - exit code: 0 when the check passed, 1 when it did not.
 */

import path from "path";
import { fileURLToPath } from "url";
import type { components } from "./api";
import { logger } from "./logger";
import type { AppSettings } from "./main_server";
import type { EnvironmentStore } from "./service/environment_store";

type LoadError = NonNullable<
   components["schemas"]["ServerStatus"]["loadErrors"]
>[number];

export interface CheckFinding {
   severity: "error" | "warning";
   /** Absent for a finding about the server itself (its storage, its config). */
   environment?: string;
   package?: string;
   /** Relative to the working directory when the package is local; else package-relative. */
   file?: string;
   /** One-based. */
   line?: number;
   /** One-based. */
   column?: number;
   message: string;
   code?: string;
}

export interface CheckReport {
   findings: CheckFinding[];
   /** Environments that loaded. */
   environments: number;
   /** Packages serving. */
   packages: number;
   /** Configured environments and packages that did not load, and stale ones. */
   loadErrors: number;
   /** Why the check could not look at anything: storage failed to open, or no config was found. */
   fatal: boolean;
}

export interface CheckOptions {
   /** Fail on a `warn`-severity package finding too (`checkFailOnWarnings`). */
   failOnWarnings: boolean;
}

/** One line of text: a load error's message spans several. */
function oneLine(text: string): string {
   return text
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0)
      .join(" / ");
}

function owner(finding: CheckFinding): string | undefined {
   if (finding.environment === undefined) return undefined;
   return finding.package === undefined
      ? finding.environment
      : `${finding.environment}/${finding.package}`;
}

/** One finding as one line: `file:line:col: error: message [env/pkg]`. */
export function formatFinding(finding: CheckFinding): string {
   const message =
      oneLine(finding.message) +
      (finding.code !== undefined ? ` (${finding.code})` : "");
   const who = owner(finding);
   if (finding.file !== undefined) {
      const where = [finding.file, finding.line, finding.column]
         .filter((part) => part !== undefined)
         .join(":");
      return `${where}: ${finding.severity}: ${message}${who ? ` [${who}]` : ""}`;
   }
   return `${who ?? "publisher"}: ${finding.severity}: ${message}`;
}

function count(report: CheckReport, severity: CheckFinding["severity"]) {
   return report.findings.filter((f) => f.severity === severity).length;
}

/** Whether the check passed: nothing failed to load, nothing is an error. */
export function checkPassed(
   report: CheckReport,
   options: CheckOptions,
): boolean {
   if (report.fatal || report.loadErrors > 0) return false;
   if (count(report, "error") > 0) return false;
   return !(options.failOnWarnings && count(report, "warning") > 0);
}

/** 0 when the check passed, 1 when it did not. */
export function checkExitCode(
   report: CheckReport,
   options: CheckOptions,
): 0 | 1 {
   return checkPassed(report, options) ? 0 : 1;
}

/** The summary line, printed last, to stderr. */
export function checkSummaryLine(
   report: CheckReport,
   options: CheckOptions,
): string {
   return (
      `${checkPassed(report, options) ? "PUBLISHER_CHECK_OK" : "PUBLISHER_CHECK_FAILED"} ` +
      `environments=${report.environments} packages=${report.packages} ` +
      `load_errors=${report.loadErrors} errors=${count(report, "error")} ` +
      `warnings=${count(report, "warning")}`
   );
}

/**
 * A file as a finding names it: relative to `cwd` when it is inside it (a
 * CI checkout, so an annotation lands on the file), else absolute.
 */
export function displayPath(cwd: string, absolute: string): string {
   const relative = path.relative(cwd, absolute);
   return relative === "" ||
      relative.startsWith("..") ||
      path.isAbsolute(relative)
      ? absolute
      : relative;
}

interface MalloyProblemShape {
   severity?: string;
   message?: string;
   code?: string;
   at?: {
      url?: string;
      range?: { start?: { line?: number; character?: number } };
   };
}

/**
 * A Malloy problem as a finding. Its file comes from `at.url` when that is a
 * file, else the model it was reported on; either way relative to `cwd`, so
 * a CI annotation lands on the file in the checkout.
 */
export function problemFinding(
   raw: unknown,
   where: {
      environment: string;
      package: string;
      packageDir: string;
      modelPath: string;
      cwd: string;
   },
): CheckFinding {
   const problem = (raw ?? {}) as MalloyProblemShape;
   const url = problem.at?.url;
   let absolute = path.join(where.packageDir, where.modelPath);
   if (url?.startsWith("file:")) {
      try {
         absolute = fileURLToPath(url);
      } catch {
         // An unparseable url: the model the problem was reported on stands.
      }
   }
   const start = problem.at?.range?.start;
   return {
      severity: problem.severity === "error" ? "error" : "warning",
      environment: where.environment,
      package: where.package,
      file: displayPath(where.cwd, absolute),
      ...(typeof start?.line === "number" ? { line: start.line + 1 } : {}),
      ...(typeof start?.character === "number"
         ? { column: start.character + 1 }
         : {}),
      message: problem.message ?? "The model did not compile.",
      ...(problem.code !== undefined ? { code: problem.code } : {}),
   };
}

/** De-duplicated: two models importing one broken file report it twice. */
function dedupe(findings: CheckFinding[]): CheckFinding[] {
   const seen = new Set<string>();
   return findings.filter((finding) => {
      const key = formatFinding(finding);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
   });
}

/**
 * A package that did not load, as findings. Where it is published from a
 * local directory, every model there is compiled, so each error is reported
 * at its file, line and column rather than only the first the load stopped
 * at. Otherwise, or when that compile finds nothing (the load failed for
 * another reason, such as its publisher.json), the load error itself.
 */
async function failedPackageFindings(
   store: EnvironmentStore,
   loadError: LoadError,
   cwd: string,
): Promise<CheckFinding[]> {
   const environmentName = loadError.environment;
   const packageName = loadError.package;
   const fallback: CheckFinding = {
      severity: "error",
      ...(environmentName !== undefined
         ? { environment: environmentName }
         : {}),
      ...(packageName !== undefined ? { package: packageName } : {}),
      message:
         (loadError.stale
            ? "Serving an older model: its latest reload failed. "
            : "") + (loadError.message ?? "Did not load."),
   };
   if (
      environmentName === undefined ||
      packageName === undefined ||
      loadError.stale
   ) {
      return [fallback];
   }
   const environment = store.peekEnvironment(environmentName);
   const packageDir = await store.configuredLocalSource(
      environmentName,
      packageName,
   );
   if (!environment || !packageDir) return [fallback];
   try {
      const { Package } = await import("./service/package");
      const models = await Package.compileErrorsOf(
         environmentName,
         packageName,
         packageDir,
         () => environment.getEnvironmentMalloyConfig(),
      );
      const findings = models.flatMap(({ modelPath, problems }) =>
         problems.map((problem) =>
            problemFinding(problem, {
               environment: environmentName,
               package: packageName,
               packageDir,
               modelPath,
               cwd,
            }),
         ),
      );
      return findings.some((f) => f.severity === "error")
         ? findings
         : [fallback, ...findings];
   } catch (error) {
      logger.info(
         `Check: could not compile ${environmentName}/${packageName} model by model; reporting its load error`,
         { error: error instanceof Error ? error.message : String(error) },
      );
      return [fallback];
   }
}

/**
 * The findings of a loaded package's `warnings`, which carry no position. A
 * finding's model is named relative to `cwd` when the package is local, as a
 * compile error's file is, else package-relative.
 */
function warningFindings(
   environmentName: string,
   pkg: components["schemas"]["Package"],
   packageDir: string | undefined,
   cwd: string,
): CheckFinding[] {
   const fileOf = (model: string) =>
      packageDir ? displayPath(cwd, path.join(packageDir, model)) : model;
   return (pkg.warnings ?? []).map((warning) => ({
      severity: warning.severity === "error" ? "error" : "warning",
      environment: environmentName,
      package: pkg.name,
      ...(warning.model !== undefined ? { file: fileOf(warning.model) } : {}),
      message:
         (warning.subject ? `${warning.subject}: ` : "") +
         (warning.message ?? ""),
      ...(warning.code !== undefined ? { code: warning.code } : {}),
   }));
}

/** Read the store, once initialization has settled, into a report. */
export async function collectCheckReport(
   store: EnvironmentStore,
   cwd: string,
): Promise<CheckReport> {
   await store.finishedInitialization;
   const findings: CheckFinding[] = [];
   const initError = store.getInitError();
   if (initError) {
      findings.push({ severity: "error", message: initError });
      return {
         findings,
         environments: 0,
         packages: 0,
         loadErrors: 0,
         fatal: true,
      };
   }
   const status = await store.getStatus();
   const loadErrors = store.getLoadProblems();
   for (const loadError of loadErrors) {
      findings.push(...(await failedPackageFindings(store, loadError, cwd)));
   }
   let packages = 0;
   for (const { name } of status.environments) {
      if (name === undefined) continue;
      const environment = store.peekEnvironment(name);
      if (!environment) continue;
      packages += environment.getServingPackageCount();
      for (const pkg of await environment.listPackages()) {
         if ((pkg.warnings ?? []).length === 0) continue;
         const packageDir =
            pkg.name === undefined
               ? undefined
               : await store.configuredLocalSource(name, pkg.name);
         findings.push(...warningFindings(name, pkg, packageDir, cwd));
      }
   }
   // Nothing was checked: CI pointed at the wrong path reads as a pass otherwise.
   const empty = status.emptyReason !== undefined;
   if (empty) {
      findings.push({ severity: "error", message: status.emptyReason! });
   }
   return {
      findings: dedupe(findings),
      environments: status.environments.length,
      packages,
      loadErrors: loadErrors.length,
      fatal: empty,
   };
}

function write(stream: NodeJS.WriteStream, text: string): Promise<void> {
   return new Promise((resolve) => {
      if (text.length === 0) return resolve();
      stream.write(text, () => resolve());
   });
}

/**
 * Print the report, findings then the summary, and return the exit code.
 * Waits for both streams to drain, since the caller exits straight after
 * and a pipe on macOS is written asynchronously.
 */
export async function printCheckReport(
   report: CheckReport,
   options: CheckOptions,
): Promise<0 | 1> {
   await write(
      process.stdout,
      report.findings.map((f) => `${formatFinding(f)}\n`).join(""),
   );
   await write(process.stderr, `${checkSummaryLine(report, options)}\n`);
   return checkExitCode(report, options);
}

/**
 * What server.ts runs when `check` is on: prepare the boot as the server
 * does, load every configured environment into a store with no listener,
 * report, and return the exit code.
 */
export async function runCheck(settings: AppSettings): Promise<0 | 1> {
   const options: CheckOptions = {
      failOnWarnings: settings.get("checkFailOnWarnings"),
   };
   try {
      const { prepareBoot } = await import("./main_server");
      const { assertDuckDBResourceConfig } = await import("./config");
      const { EnvironmentStore } = await import("./service/environment_store");
      prepareBoot();
      assertDuckDBResourceConfig();
      const store = new EnvironmentStore(
         path.resolve(process.cwd(), settings.get("root")),
      );
      const report = await collectCheckReport(store, process.cwd());
      return await printCheckReport(report, options);
   } catch (error) {
      return printCheckReport(
         {
            findings: [
               {
                  severity: "error",
                  message:
                     error instanceof Error ? error.message : String(error),
               },
            ],
            environments: 0,
            packages: 0,
            loadErrors: 0,
            fatal: true,
         },
         options,
      );
   } finally {
      try {
         const { getPackageLoadPool } = await import(
            "./package_load/package_load_pool"
         );
         await getPackageLoadPool().shutdown();
      } catch {
         // The process exits next either way.
      }
   }
}
