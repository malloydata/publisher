// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `OracleGuard`: the pre-compile checks, run on a request's TEXT before
 * anything compiles. ADVISORY, never the authority.
 *
 * They exist only so a refused caller hears the gate's 403 (or the boundary's
 * 404) instead of the compiler's opinion of a column on a source it may not
 * read: "'secret' is not defined" would confirm the schema of a locked source.
 * They read surface syntax, so they can only over-deny; whatever they admit is
 * decided again, on the compiled query, by `QueryGovernor.govern`, which runs
 * unconditionally. Removing a check here may leak schema through compile
 * errors; it must never change which rows a query can read.
 *
 * Moved verbatim from `Model.runQueryResults`, `Model.runNotebookCell` and
 * `Model.assertAuthorizedForText`; the checks themselves stay on
 * `QueryGovernor`, which hands them in as {@link OracleChecks}.
 */

import type { GivenValue, QueryMaterializer } from "@malloydata/malloy";
import { hasCallerAuthorizeAnnotation } from "../authorize";
import type { CallerRegion } from "../caller_joins";
import type { GraftScope } from "../gate_classification";
import { extractRunTargetSourceName } from "../query_text";

/** The checks an {@link OracleGuard} sequences; `QueryGovernor` supplies them. */
export interface OracleChecks {
   /** Name-keyed gate on one declared source (`QueryGovernor.assertAuthorized`). */
   assertAuthorized(
      sourceName: string | undefined,
      givens: Record<string, GivenValue>,
      bypassAuthorize?: boolean,
      graftScope?: GraftScope,
   ): Promise<void>;
   assertCallerJoinBasesEarly(
      query: string,
      givens: Record<string, GivenValue>,
      phase: "boundary" | "locks",
      decided?: Set<string>,
      includeHiddenFilesAndSources?: boolean,
   ): Promise<void>;
   assertLocksOnAppearingNames(
      text: string,
      givens: Record<string, GivenValue>,
      bypassAuthorize: boolean,
      decided: Set<string>,
      includeHiddenFilesAndSources?: boolean,
   ): Promise<void>;
   assertLocksOnRequestDeclaredBases(
      entryPoint: string,
      query: string,
      givens: Record<string, GivenValue>,
      bypassAuthorize?: boolean,
   ): Promise<void>;
   probeEntryPointGates(
      runnable: QueryMaterializer,
      givens: Record<string, GivenValue>,
      graftScope: GraftScope | undefined,
   ): Promise<unknown>;
   /** The model's declared sources and queries, by name. */
   sources(): ReadonlyArray<{ name?: string }> | undefined;
   queries(): ReadonlyArray<{ name?: string; sourceName?: string }> | undefined;
}

export class OracleGuard {
   constructor(private readonly checks: OracleChecks) {}

   /**
    * The query route's pre-compile checks, after the early query boundary and
    * before the caller's text compiles: the boundary for the caller's own
    * joins, the run target's lock read off surface syntax, the locks those
    * joins reach, then every other locked name the text mentions. Each lock is
    * decided (and booked) once. Returns the surface run target, which the
    * caller also uses to tag the query when the compiled one is unreadable.
    */
   public async beforeQueryCompile(request: {
      sourceName?: string;
      queryName?: string;
      query?: string;
      givens: Record<string, GivenValue> | undefined;
      bypassAuthorize: boolean;
      includeHiddenFilesAndSources: boolean;
      /** Where caller text sits (ad-hoc text only). */
      callerRegion: CallerRegion | undefined;
   }): Promise<{ earlySource: string | undefined }> {
      const {
         sourceName,
         queryName,
         query,
         givens,
         bypassAuthorize,
         includeHiddenFilesAndSources,
         callerRegion,
      } = request;
      // Text carrying an annotation is left to the forgery rejecter.
      const readCallerJoinText =
         !!callerRegion && !hasCallerAuthorizeAnnotation(callerRegion.text);
      // Under `includeHiddenFilesAndSources` there is no boundary to check a join against.
      if (readCallerJoinText && !includeHiddenFilesAndSources) {
         await this.checks.assertCallerJoinBasesEarly(
            callerRegion.text,
            givens ?? {},
            "boundary",
         );
      }

      // Early authorize gate from surface syntax, BEFORE compilation, so a
      // denied caller cannot learn a gated source's columns from "field not
      // found" errors. The authoritative compiled-source gate still runs.
      const surfaceName = extractRunTargetSourceName(query);
      const earlySource =
         sourceName ||
         (queryName
            ? this.checks.queries()?.find((q) => q.name === queryName)
                 ?.sourceName
            : undefined) ||
         // A run target can be a declared QUERY (`run: locked_q + { … }`):
         // resolve it to its source, or nothing gates it early and the
         // refinement's compile errors leak the source.
         (surfaceName &&
         !this.checks.sources()?.some((s) => s.name === surfaceName)
            ? this.checks.queries()?.find((q) => q.name === surfaceName)
                 ?.sourceName
            : undefined) ||
         surfaceName;
      if (earlySource) {
         await this.checks.assertAuthorized(
            earlySource,
            givens ?? {},
            bypassAuthorize,
         );
      }
      // Each lock is decided (and booked) once before compile.
      const decided = new Set(earlySource ? [earlySource] : []);
      // Not under `earlySource`: a join needs no readable run target.
      if (readCallerJoinText && !bypassAuthorize) {
         await this.checks.assertCallerJoinBasesEarly(
            callerRegion.text,
            givens ?? {},
            "locks",
            decided,
            includeHiddenFilesAndSources,
         );
      }
      // Every other locked name the text mentions, wherever it sits.
      if (query && !hasCallerAuthorizeAnnotation(query)) {
         await this.checks.assertLocksOnAppearingNames(
            query,
            givens ?? {},
            bypassAuthorize,
            decided,
            includeHiddenFilesAndSources,
         );
      }
      return { earlySource };
   }

   /**
    * Gate ad-hoc `/compile` text before compiling. Caller text (`append`
    * scope) has every locked name that appears anywhere in it decided, after
    * the last `run:` target and the locks its caller-written joins reach — a
    * caller-owned fragment has no legitimate need to name a base it may not
    * read. At file and package scope (`wholeFile`) the text IS the author's
    * file, so only the last `run:` target is checked, plus a derivation walk
    * from it (`assertLocksOnRequestDeclaredBases`); a mention that is
    * not what Malloy will run must still compile.
    *
    * Takes no bypass argument, deliberately: `/compile` returns schema and SQL,
    * and no caller needs to compile through a gate.
    */
   public async beforeCompileText(
      text: string,
      givens: Record<string, GivenValue>,
      /** `wholeFile`: file or package scope, where the text is the author's file. */
      options?: { wholeFile?: boolean },
   ): Promise<void> {
      const target = extractRunTargetSourceName(text);
      await this.checks.assertAuthorized(target, givens);
      // Text carrying an annotation is left to the forgery rejecter, whose
      // refusal is the specific one.
      if (hasCallerAuthorizeAnnotation(text)) return;
      if (options?.wholeFile) {
         // `/compile` answers WITH the compiler's diagnostics, so a lock not
         // decided first makes them readable for a source the caller is
         // refused. Text with no `run:` resolves no target and is walked anyway.
         await this.checks.assertLocksOnRequestDeclaredBases(
            target ?? "",
            text,
            givens,
         );
         return;
      }
      // Locks only: `/compile` is exempt from the query boundary.
      const decided = new Set(target ? [target] : []);
      await this.checks.assertCallerJoinBasesEarly(
         text,
         givens,
         "locks",
         decided,
      );
      await this.checks.assertLocksOnAppearingNames(
         text,
         givens,
         false,
         decided,
      );
   }

   /**
    * A notebook cell's gate on its UNREFINED runnable, before a `#(filter)`
    * refinement rebuilds it, so a denied caller gets the 403 rather than a
    * 400 from a broken refinement or bad `filterParams`. Locks and rejected
    * gates throw; row-level gates are left to `govern` on the refined
    * runnable.
    */
   public async beforeCellRefinement(
      runnable: QueryMaterializer,
      givens: Record<string, GivenValue>,
      graftScope: GraftScope | undefined,
   ): Promise<void> {
      await this.checks.probeEntryPointGates(runnable, givens, graftScope);
   }
}
