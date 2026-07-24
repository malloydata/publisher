// The scenario contract + a tiny assertion recorder. A scenario declares the
// Postgres source tables and packages it needs (so the orchestrator can seed +
// register them before booting a server), then drives the server itself via
// `ctx.use(mode)` — because PERSIST_STORAGE_MODE is read at server startup, a
// scenario that spans modes (migration) or exercises the mode matrix asks for
// the mode it wants and gets a server in that mode (reused if already running).

import type { PostgresHandle } from "../lib/postgres";
import type { PersistStorageMode } from "../lib/server";
import type { ModelFile } from "../lib/packages";
import type { Rest } from "../lib/rest";

export interface SourceTable {
   /** Database to seed into (defaults to the shared source db). */
   db?: string;
   sql: string;
}

export interface PackageSpec {
   name: string;
   /** The environment this package is registered under (defaults to the primary). */
   env?: string;
   models: ModelFile[];
}

/**
 * A scenario-declared connection to wire into the generated config (beyond the
 * always-present `orders_pg` source + `lake` destination), so a scenario can
 * exercise a second destination, a second source, or a different source dialect.
 * The harness fills in the details with defaults: a `postgres` connection points
 * at the run's source warehouse; a `ducklake` connection gets its OWN catalog +
 * storage (so it is a genuinely separate destination). Registered under `env`.
 */
export interface ConnectionDecl {
   env?: string;
   name: string;
   kind: "postgres" | "ducklake" | "duckdb";
}

/** Controls the server lifecycle a scenario runs against. */
export interface ServerControl {
   /**
    * Get-or-start the NAMED publisher at `mode` (a distinct server process on its
    * own ports, sharing the run's config so all publishers see the same source
    * warehouse + DuckLake tier). Reuses it if already running at that mode;
    * otherwise (re)boots with `--init` (re-copying packages, which is how a
    * mid-scenario edit takes effect). Most scenarios use only `"default"`; a
    * multi-publisher scenario addresses several (`p1`, `p2`, …).
    */
   usePublisher(
      name: string,
      mode: PersistStorageMode,
      opts?: { init?: boolean; extraEnv?: Record<string, string> },
   ): Promise<Rest>;
   /** Shorthand for the single `"default"` publisher (used by hooks). */
   use(
      mode: PersistStorageMode,
      opts?: { init?: boolean; extraEnv?: Record<string, string> },
   ): Promise<Rest>;
   /**
    * Force a fresh boot of a publisher (default `"default"`), preserving the
    * materialization store unless `init:true` — restart-survival tests. Pass
    * `name`/`mode` to target/retune a specific publisher.
    */
   reboot(opts?: {
      name?: string;
      mode?: PersistStorageMode;
      init?: boolean;
   }): Promise<Rest>;
   /** The REST client of an already-running publisher (for cross-publisher steps). */
   restOf(name: string): Rest;
}

export interface ScenarioContext extends ServerControl {
   pg: PostgresHandle;
   env: string;
   /** The shared Postgres source database name. */
   sourceDb: string;
   /**
    * Overwrite a model file in the generated SOURCE package dir. Takes effect on
    * the next `--init` boot (which re-copies the package). Used by the migration
    * scenario to add `storage=` between phases. `env` selects which environment's
    * copy of the package to edit (defaults to the primary environment).
    */
   editPackageModel(
      pkg: string,
      modelPath: string,
      text: string,
      env?: string,
   ): Promise<void>;
   /**
    * Run operator/orchestrator DDL against a destination connection out-of-band.
    * For the DuckLake destination this uses a READ-WRITE attach (the publisher's
    * serve attach is read-only), mirroring how a orchestrator provisions the
    * catalog (e.g. `CREATE SCHEMA`).
    */
   operatorSql(conn: string, sql: string): Promise<void>;
   /**
    * Act as the orchestrator's manifest store: write a build manifest
    * (`{ entries }`) to a local file and return a `file://` URI the publisher can
    * fetch. The caller PATCHes the package's `manifestLocation` to this URI so
    * the publisher binds the orchestrator-authoritative manifest — the production serve
    * path, distinct from the publisher's own post-build local-store rebind.
    */
   writeManifest(name: string, entries: Record<string, unknown>): Promise<string>;
}

export interface Scenario {
   id: string;
   /** Free-form labels for filtering (`--tags`) and grouping the report. */
   tags: string[];
   title: string;
   /**
    * Capability tokens this scenario needs BEYOND the always-present defaults
    * (`connection:orders_pg`/`dialect:postgres`, `connection:lake`/
    * `dialect:duckdb`). If the run can't provide one — e.g. `dialect:snowflake`
    * with no Snowflake connection wired — the scenario is SKIPPED, not failed.
    */
   requires: string[];
   /**
    * An author's callout (from a `## Note` section) surfaced in the report's
    * "needs attention" block — the open question or thing to review. `since` is
    * the date it was raised (`## Note (since=YYYY-MM-DD)`), used to age it.
    */
   note?: { since?: string; text: string };
   sourceTables?: SourceTable[];
   packages: PackageSpec[];
   /** Connections to wire into the config beyond the defaults (see {@link ConnectionDecl}). */
   connections?: ConnectionDecl[];
   run(ctx: ScenarioContext, assert: Assert): Promise<void>;
}

/** The single tag that marks a scenario as carrying an open question / follow-up. */
export const ATTENTION_TAG = "needs-attention";

export interface Check {
   name: string;
   ok: boolean;
   detail?: string;
}

/** Records named checks; never throws, so one failure doesn't abort the rest. */
export class Assert {
   readonly checks: Check[] = [];
   constructor(readonly scenarioId: string) {}

   private push(name: string, ok: boolean, detail?: string): void {
      this.checks.push({ name, ok, detail });
   }

   ok(name: string, cond: boolean, detail?: string): void {
      this.push(name, cond, cond ? undefined : detail);
   }

   eq<T>(name: string, actual: T, expected: T): void {
      const ok = JSON.stringify(actual) === JSON.stringify(expected);
      this.push(
         name,
         ok,
         ok ? undefined : `expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`,
      );
   }

   ne<T>(name: string, actual: T, notExpected: T, detail?: string): void {
      const ok = JSON.stringify(actual) !== JSON.stringify(notExpected);
      this.push(name, ok, ok ? undefined : (detail ?? `unexpectedly equal to ${JSON.stringify(notExpected)}`));
   }

   includes(name: string, haystack: string, needle: string): void {
      const ok = haystack.includes(needle);
      this.push(name, ok, ok ? undefined : `"${needle}" not found in: ${truncate(haystack)}`);
   }

   excludes(name: string, haystack: string, needle: string): void {
      const ok = !haystack.includes(needle);
      this.push(name, ok, ok ? undefined : `"${needle}" unexpectedly present in: ${truncate(haystack)}`);
   }

   fail(name: string, detail: string): void {
      this.push(name, false, detail);
   }

   get passed(): boolean {
      return this.checks.every((c) => c.ok);
   }
}

function truncate(s: string, n = 300): string {
   return s.length > n ? `${s.slice(0, n)}…` : s;
}
