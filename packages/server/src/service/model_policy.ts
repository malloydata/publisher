// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a {@link Model} is told by whoever owns it: whether discovery is
 * curated, the query boundary it enforces, how it resolves a sibling file's
 * IR, and the per-query build-manifest resolvers. A Model is constructed with
 * one, so there is no window in which a Model holds a policy nobody chose.
 *
 * Two owners exist:
 *  - a {@link Package}, which builds a {@link PackageModelPolicy} before it
 *    hydrates its models and fills it as the package's surface, reports and
 *    build manifest are known;
 *  - anything that drives one Model directly (a fixture, the `/compile` gate
 *    model, a tool), which passes {@link ModelPolicy.standalone} or a
 *    {@link ModelPolicy.fixed} policy, by name.
 */

import type { SiblingModelDefResolver } from "./filter_binding_guard";
import type { MaterializedTableCatalog } from "./materialization/serve/query_path";
import type { Snapshot } from "./materialization/domain/binding";
import type { ReportLayout } from "./report";

/** The package's published surface, as an explained refusal describes it:
 *  `indexModel` when the package is curated by its root index.malloy rather
 *  than an explicit "explores" list, and the surface files either way. */
export interface OffSurfaceContext {
   indexModel: boolean;
   files: readonly string[];
}

/** The query boundary one model file enforces (see `Model.assertQueryBoundaryEarly`). */
export interface QueryBoundaryPolicy {
   mode: "declared" | "all";
   exploresDeclared: boolean;
   isQueryEntryPoint: boolean;
   /** The PACKAGE-wide export closure (union over explores-listed models),
    *  keyed name -> DEFINITION IDENTITIES (`Model.definitionIdentity`), never
    *  the bare name: a name is admitted only when THIS model resolves it to
    *  the very definition a listed model exported, so a same-named source in
    *  another file is not admitted by the collision. Absent when uncurated. */
   packageCuratedSources?: ReadonlyMap<string, ReadonlySet<string>>;
   packageCuratedQueries?: ReadonlyMap<string, ReadonlySet<string>>;
   /** Which files are the surface, for an explained refusal. */
   offSurface?: OffSurfaceContext;
   /** This file is a served report with this layout: an entry point that,
    *  like a `.malloynb`, admits nothing of its own, since a grid report's
    *  tiles and a linear report's cells read only the package surface. A
    *  linear report keeps `modelType` "model". */
   layout?: ReportLayout;
   /** The served report's own compiled file text, so a source it declares
    *  on top of a surface source (`source: big is orders extend {...}`) can
    *  be proven to derive from it. Author text, not caller text. */
   derivationText?: string;
}

/** The policy a {@link Model} reads. Every member is read per use, so an owner may change what it answers. */
export interface ModelPolicy {
   /** Who built it; for diagnostics and specs, never for a decision. */
   readonly origin: "standalone" | "fixed" | "package";
   /** Whether discovery accessors curate to the `export {}` closure. */
   discoveryCurationEnabled(): boolean;
   /** The query boundary this file enforces. */
   queryBoundary(): QueryBoundaryPolicy;
   /**
    * Resolves the `ModelDef` the owner independently compiled for the file a
    * `file://` URL names. `undefined` is safe: every guard treats
    * "unresolvable" as deny. See `SiblingModelDefResolver`.
    */
   siblingModelDefResolver(): SiblingModelDefResolver | undefined;
   /**
    * Every table the package serves now, as one immutable snapshot, which a
    * query pins once (freshness included). Undefined serves nothing stored.
    */
   snapshot(): Snapshot | undefined;
   /** The `sourceEntityId`s pre-aggregation synthesized, stripped from the
    *  author model's manifest. Undefined or empty strips nothing. */
   preaggregateEntityIds(): ReadonlySet<string> | undefined;
   /**
    * The package's materialized tables (its build plan and bound database
    * manifest), for the path report. Undefined outside a package, or before
    * the package wires it; the report then names no database table.
    */
   materializedTables(): MaterializedTableCatalog | undefined;
   /**
    * Report that a stored read found these tables (by virtual handle) not as
    * their bind found them (the drift handler's failed-read trigger). A
    * model outside a package reports to nobody.
    */
   reportIncompatible?(handles: readonly string[]): void;
}

/**
 * The boundary of a model nobody curates: every name the file declares is
 * queryable by name. Gates (`#(authorize)`, `#(access_filter)`) still apply;
 * this lifts curation, never a lock.
 */
const UNCURATED_BOUNDARY: QueryBoundaryPolicy = Object.freeze({
   mode: "all",
   exploresDeclared: false,
   isQueryEntryPoint: true,
});

/**
 * The boundary a Package-owned model holds before its package has decided it:
 * a curated surface with nothing on it, so every query is refused. Never
 * observed by a query in practice (a package installs the decided boundary in
 * the same synchronous step that makes the model reachable); it exists so a
 * missed install fails closed rather than admitting.
 */
const UNDECIDED_PACKAGE_BOUNDARY: QueryBoundaryPolicy = Object.freeze({
   mode: "declared",
   exploresDeclared: true,
   isQueryEntryPoint: false,
   packageCuratedSources: new Map<string, ReadonlySet<string>>(),
   packageCuratedQueries: new Map<string, ReadonlySet<string>>(),
});

/** Fields of a {@link ModelPolicy.fixed} policy; anything omitted is the standalone answer. */
export interface FixedModelPolicy {
   discoveryCuration?: boolean;
   queryBoundary?: QueryBoundaryPolicy;
   siblingModelDefResolver?: SiblingModelDefResolver;
   /** The materialized tables the path report reads; omitted names none. */
   materializedTables?: MaterializedTableCatalog;
   /** The tables served; omitted serves nothing stored. Read per query. */
   snapshot?: () => Snapshot | undefined;
   /** The rollups' `sourceEntityId`s, stripped from the author's manifest. */
   preaggregateEntityIds?: ReadonlySet<string>;
}

function fixedPolicy(
   origin: "standalone" | "fixed",
   fields: FixedModelPolicy,
): ModelPolicy {
   const discoveryCuration = fields.discoveryCuration ?? false;
   const boundary = fields.queryBoundary ?? UNCURATED_BOUNDARY;
   const sibling = fields.siblingModelDefResolver;
   const tables = fields.materializedTables;
   return Object.freeze({
      origin,
      discoveryCurationEnabled: () => discoveryCuration,
      queryBoundary: () => boundary,
      siblingModelDefResolver: () => sibling,
      snapshot: () => fields.snapshot?.(),
      preaggregateEntityIds: () => fields.preaggregateEntityIds,
      materializedTables: () => tables,
   });
}

export const ModelPolicy = {
   /**
    * The policy of a Model outside a Package — what such a Model has always
    * done: discovery is uncurated, the query boundary admits every name the
    * file declares (no package surface exists to curate against; gates still
    * apply), a sibling file's IR is unresolvable (so the filter-binding guards
    * deny), and queries serve live with no freshness override.
    */
   standalone(): ModelPolicy {
      return fixedPolicy("standalone", {});
   },
   /**
    * A fixed policy for a Model driven directly (fixtures, tools) that needs a
    * particular curation or boundary. Omitted fields take the
    * {@link ModelPolicy.standalone} answer.
    */
   fixed(fields: FixedModelPolicy): ModelPolicy {
      return fixedPolicy("fixed", fields);
   },
};

/**
 * One package's model policy: built by the Package before it hydrates its
 * models (each gets {@link forModel}), and filled by the Package as the
 * surface, reports and build manifest become known. A reload builds a fresh
 * one for its new models and the Package adopts it when it installs them.
 *
 * Until {@link setQueryBoundaries} runs, every model's boundary refuses
 * everything (fail closed); discovery is uncurated, siblings unresolvable and
 * queries live, as a model hydrated but not yet installed has always been.
 */
export class PackageModelPolicy {
   private discoveryCuration = false;
   private boundaries: ReadonlyMap<string, QueryBoundaryPolicy> | undefined;
   private siblingResolver: SiblingModelDefResolver | undefined;
   private serving:
      | {
           snapshot: () => Snapshot;
           preaggregateEntityIds: () => ReadonlySet<string>;
           materializedTables?: () => MaterializedTableCatalog | undefined;
           reportIncompatible?: (handles: readonly string[]) => void;
        }
      | undefined;

   /** The policy that replaced this one ({@link handOffTo}), if any. */
   private successor: PackageModelPolicy | undefined;

   /** The live view one model reads. */
   public forModel(modelPath: string): ModelPolicy {
      const live = () => this.current();
      return Object.freeze({
         origin: "package" as const,
         discoveryCurationEnabled: () => live().discoveryCuration,
         // A path the package did not decide is refused, never admitted.
         queryBoundary: () =>
            live().boundaries?.get(modelPath) ?? UNDECIDED_PACKAGE_BOUNDARY,
         siblingModelDefResolver: () => live().siblingResolver,
         snapshot: () => live().serving?.snapshot(),
         preaggregateEntityIds: () => live().serving?.preaggregateEntityIds(),
         materializedTables: () => live().serving?.materializedTables?.(),
         reportIncompatible: (handles: readonly string[]) =>
            live().serving?.reportIncompatible?.(handles),
      });
   }

   /**
    * Make every model hydrated under this policy read `next` from now on: a
    * reload that keeps a model it could not recompile installs it beside
    * models hydrated under `next`, and all of them read the package's one
    * current policy.
    */
   public handOffTo(next: PackageModelPolicy): void {
      if (next !== this) this.successor = next;
   }

   /**
    * The policy in force: the last one handed off to. Points this one
    * straight at it, so a model kept across many reloads holds no chain of
    * the policies between.
    */
   private current(): PackageModelPolicy {
      if (!this.successor) return this;
      this.successor = this.successor.current();
      return this.successor;
   }

   /** Whether discovery curates to each model's `export {}` closure (`explores` declared). */
   public setDiscoveryCuration(enabled: boolean): void {
      this.discoveryCuration = enabled;
   }

   /** Each model path's query boundary, replaced as a whole. */
   public setQueryBoundaries(
      boundaries: ReadonlyMap<string, QueryBoundaryPolicy>,
   ): void {
      this.boundaries = boundaries;
   }

   /** Resolve a sibling file's `ModelDef` by `file://` URL. */
   public setSiblingModelDefResolver(resolver: SiblingModelDefResolver): void {
      this.siblingResolver = resolver;
   }

   /**
    * Install the package's serving resolvers. Left unset until the package
    * wires them, so a model serves nothing stored until then.
    */
   public setServingResolvers(resolvers: {
      snapshot: () => Snapshot;
      preaggregateEntityIds: () => ReadonlySet<string>;
      materializedTables?: () => MaterializedTableCatalog | undefined;
      /**
       * A stored read found these tables (by virtual handle) not as their
       * bind found them: the drift handler's failed-read trigger.
       */
      reportIncompatible?: (handles: readonly string[]) => void;
   }): void {
      this.serving = resolvers;
   }
}
