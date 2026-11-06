// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which sources the entities mark queryable, file by file. This server's query
 * route decides what runs; the marker follows it, with one deliberate
 * difference (the last paragraph below).
 *
 * The rule is re-derived here from the compiled models rather than read from
 * the query route, so it must agree with these; re-read them when they change:
 *  - ../package.ts, `applyQueryBoundaryToModels`: which files are entry
 *    points, and the package-wide map of exported declarations;
 *  - ../model.ts, `assertFileOnSurface`, `definitionIdentity`,
 *    `admittedByPackage`, `isCuratedSource`, `ownCuratedSourceNames` and
 *    `derivesFromCurated`: which names a file admits;
 *  - ../package.ts, `discoverReportsIn`, and ../report_manifest.ts,
 *    `buildReportManifest` and `readTileEntries`: which files are served as
 *    reports, and with which layout;
 *  - ../report.ts: the one reader of a report tag.
 * `__fixtures__/query_surface_cases.ts` holds the cases, with what the query
 * route answers and what the marker says for each.
 *
 * The query route's rule, when the package has a surface (`explores`, or a root
 * `index.malloy`) and `queryableSources` is "declared":
 *  1. A file is an entry point if `explores` lists it, or it is a served
 *     report. Every source in any other file is refused.
 *  2. In an entry point, a source is admitted if it is one of these:
 *     - exported by this file, unless the file is a report, whose own exports
 *       admit nothing;
 *     - the same declaration (file and position) that some listed, non-report
 *       file exports;
 *     - in a report, a source the file declares on top of admitted sources
 *       only (`source: open_orders is orders extend {...}`).
 *
 * A served report is a `.malloy` file outside `public/`, other than the root
 * `index.malloy`, whose own model-level `## artifact` tag parses and sets no
 * `kind=`. Where it lives means nothing: `dashboards/`, `notebooks/` and
 * `reports/` are ordinary directory names. Its layout is `layout=` when set,
 * else grid when the tag lists `tiles` or a `dashboard` tag is present, else
 * linear. A linear report is always served. A grid report is served only when
 * its tile list names at least one tile.
 *
 * The query route answers query text this way (`run: open_orders -> {...}`),
 * which is what an agent sends. A request that names the source in the
 * `sourceName` field admits a report's derived source too
 * (`Model.isReportSourceOnSurface`), through the same `derivesFromCurated`
 * walk over the report's own text, so the two routes agree.
 *
 * Not reproduced: the server drops a grid report whose manifest build throws.
 * A file like that is an entry point here only, so its derived sources are
 * marked queryable where the query route answers 404.
 *
 * The deliberate difference: in a report, every source the query route runs is
 * marked NOT_QUERYABLE, and its marker names the served source it is or
 * derives from. Two kinds:
 *  - the same declaration a listed non-report file exports (the report's plain
 *    re-import of `orders`). See {@link servedElsewhere};
 *  - a source the report declares on top of served sources
 *    (`source: open_orders is orders extend {...}`), directly or through a
 *    chain. See {@link derivedFromServed}.
 * The entities advertise only what the listed non-report files serve, once
 * each, so a search over them shows one `orders` result rather than one per
 * report that imports or extends it. A report is a view onto the served
 * surface, not an addition to it.
 */
import type { Model } from "@malloydata/malloy";
import { ownModelNoteObjects } from "../annotations";
import { motlyTag, tagText } from "../motly";
import {
   buildDerivationBaseMap,
   stripMalloyCommentsAndLiterals,
} from "../query_text";
import {
   isReportEligiblePath,
   readReportTag,
   type ReportLayout,
} from "../report";

/**
 * The package's discovery surface, as `publisher.json` resolves it. Both fields
 * optional: a package with no surface keeps every compiled source queryable.
 */
export type PackageManifest = {
   /**
    * Model file paths (relative to the package root) that form the surface:
    * `explores` as written, or `["index.malloy"]` when the key is absent and
    * the package has a root `index.malloy`.
    */
   explores?: string[];
   /** `"declared"` (default) makes the discovery surface a query boundary; `"all"` decouples them. */
   queryableSources?: "declared" | "all";
};

/** A compiled `.malloy` file of the package, with the text it compiled from. */
export type SurfaceModelFile = {
   relativePath: string;
   model: Model;
   text: string;
};

/**
 * The package's query surface, resolved once from every compiled file.
 * `inert` when the package curates nothing: then every source is queryable.
 */
export type PackageQuerySurface =
   | { inert: true }
   | {
        inert: false;
        /** `explores`, normalized. */
        explores: Set<string>;
        /** Files served as reports, normalized, with their layout. */
        reports: Map<string, ReportLayout>;
        /** Source name → identities of the declarations listed files export. */
        exportedDeclarations: Map<string, Set<string>>;
        /**
         * Declaration identity → the listed non-report file that exports it and
         * the name it exports it under. When two listed files export one
         * declaration, the first by path. The file path is as the package
         * lists it (`./index.malloy` stays `./index.malloy`), so it matches
         * the path the file's rows are indexed under.
         */
        servingFiles: Map<string, ServedSourceAddress>;
     };

/** Where the index advertises a served declaration: its file and exported name. */
export type ServedSourceAddress = { modelFilePath: string; sourceName: string };

/** `REQUEST_CHAIN_MAX_NAMES` in ../model.ts: a longer chain proves nothing. */
const MAX_CHAIN_NAMES = 64;

/** Strip a leading `./` or `/` so `explores` entries and relative paths compare equal. */
export const normalizeRelpath = (p: string): string =>
   p.replace(/^\.\//, "").replace(/^\/+/, "");

/**
 * Whether a grid tag names at least one tile: `readTileEntries`
 * (../report_manifest.ts) keeps each `tiles` entry that has a text value, and
 * `buildReportManifest` builds a grid report only from a non-empty list.
 * `readTileEntries` is not exported, so this one line restates it.
 */
const namesATile = (modelNotes: string[]): boolean =>
   motlyTag(modelNotes)
      ?.tag("artifact")
      ?.array("tiles")
      ?.some((entry) => tagText(entry) !== undefined) ?? false;

/**
 * What `discoverReportsIn` (../package.ts) serves this file as: a report of
 * this layout, or undefined for an ordinary model. Reads only the file's own
 * model-level notes, as `readReportModelFacts` does, so an
 * `## artifact` in an imported file does not make the importer a report.
 */
export function reportLayoutOf(
   file: SurfaceModelFile,
): ReportLayout | undefined {
   const relativePath = normalizeRelpath(file.relativePath);
   if (!relativePath.endsWith(".malloy")) return undefined;
   if (!isReportEligiblePath(relativePath)) return undefined;
   const notes = ownModelNoteObjects(file.model._modelDef).map(
      (note) => note.text,
   );
   const tag = readReportTag(notes);
   if (tag.status !== "report") return undefined;
   if (tag.layout === "linear") return "linear";
   return namesATile(notes) ? "grid" : undefined;
}

/**
 * The files served as reports, normalized, with their layout. A
 * report is an entry point whose own exports admit nothing. Two reports may
 * share a basename; neither hides the other.
 */
export function servedReports(
   files: SurfaceModelFile[],
   onFileError: SurfaceFileErrorHandler = rethrow,
): Map<string, ReportLayout> {
   const served = new Map<string, ReportLayout>();
   for (const file of files) {
      let layout: ReportLayout | undefined;
      try {
         layout = reportLayoutOf(file);
      } catch (error) {
         onFileError(file, error);
         continue;
      }
      if (layout) served.set(normalizeRelpath(file.relativePath), layout);
   }
   return served;
}

/**
 * The declaration `name` resolves to in this model: the file that declares it
 * and its position there. Two files that resolve a name to the same
 * declaration get the same key; two same-named declarations do not. Undefined
 * when Malloy attached no location, which admits nothing
 * (`Model.definitionIdentity` in ../model.ts).
 */
export function definitionIdentity(
   model: Model,
   name: string,
): string | undefined {
   const def = model._modelDef.contents[name] as
      | {
           location?: {
              url?: string;
              range?: { start?: { line?: number; character?: number } };
           };
        }
      | undefined;
   const url = def?.location?.url;
   if (!url) return undefined;
   const start = def?.location?.range?.start;
   return `${url}#${start?.line ?? -1}:${start?.character ?? -1}`;
}

/** The names in this file's `export {}` closure (the model's curated `getSources()`). */
const exportedSourceNames = (model: Model): string[] =>
   model.exportedExplores.map((e) => e.name);

/**
 * Told when reading one file's part of the surface throws (the report-tag
 * parser, a `_modelDef` read). That file then contributes nothing to the
 * surface, like a file that failed to compile.
 */
export type SurfaceFileErrorHandler = (
   file: SurfaceModelFile,
   error: unknown,
) => void;

const rethrow: SurfaceFileErrorHandler = (_file, error) => {
   throw error;
};

/**
 * Resolve the package's surface from every file that compiled. A file that
 * failed to compile contributes nothing, as on the query route. Neither does a file
 * whose surface read throws, when `onFileError` is given: it is told, and the
 * rest of the package resolves without that file. Without a handler the throw
 * propagates.
 */
export function buildPackageQuerySurface(
   manifest: PackageManifest | undefined,
   files: SurfaceModelFile[],
   onFileError: SurfaceFileErrorHandler = rethrow,
): PackageQuerySurface {
   const declared = (manifest?.queryableSources ?? "declared") === "declared";
   const explorePaths = manifest?.explores;
   if (!declared || !explorePaths || explorePaths.length === 0) {
      return { inert: true };
   }
   const explores = new Set(explorePaths.map(normalizeRelpath));
   // A file whose report check throws is ERRORed and contributes no rows, so
   // its exports must not admit or serve anything either.
   const failed = new Set<string>();
   const reports = servedReports(files, (file, error) => {
      failed.add(normalizeRelpath(file.relativePath));
      onFileError(file, error);
   });
   const exportedDeclarations = new Map<string, Set<string>>();
   const servingFiles = new Map<string, ServedSourceAddress>();
   const byPath = [...files].sort((a, b) =>
      normalizeRelpath(a.relativePath) < normalizeRelpath(b.relativePath)
         ? -1
         : 1,
   );
   for (const file of byPath) {
      const relpath = normalizeRelpath(file.relativePath);
      if (!explores.has(relpath) || failed.has(relpath)) continue;
      // A report reads the surface; it never adds to it, even when `explores`
      // lists it.
      if (reports.has(relpath)) continue;
      let exported: { name: string; identity: string }[];
      try {
         exported = exportedSourceNames(file.model).flatMap((name) => {
            const identity = definitionIdentity(file.model, name);
            return identity ? [{ name, identity }] : [];
         });
      } catch (error) {
         onFileError(file, error);
         continue;
      }
      for (const { name, identity } of exported) {
         const known = exportedDeclarations.get(name);
         if (known) known.add(identity);
         else exportedDeclarations.set(name, new Set([identity]));
         if (!servingFiles.has(identity)) {
            // As written, not normalized: the file's rows are indexed under
            // `relativePath`, and a host looks the served source up by it.
            servingFiles.set(identity, {
               modelFilePath: file.relativePath,
               sourceName: name,
            });
         }
      }
   }
   return {
      inert: false,
      explores,
      reports,
      exportedDeclarations,
      servingFiles,
   };
}

/**
 * Whether `name`, in this file, resolves to the very declaration some listed
 * non-report file exports (`Model.admittedByPackage` in ../model.ts).
 */
function admittedByPackage(
   file: SurfaceModelFile,
   surface: Extract<PackageQuerySurface, { inert: false }>,
   name: string,
): boolean {
   const identities = surface.exportedDeclarations.get(name);
   if (!identities) return false;
   const mine = definitionIdentity(file.model, name);
   return mine !== undefined && identities.has(mine);
}

/**
 * The sources in `file` the query route refuses to run a query against. Empty
 * when the surface is inert. This is the query route's rule with nothing added; the
 * marker is {@link nonQueryableSources}.
 */
export function refusedByPublisher(
   file: SurfaceModelFile,
   surface: PackageQuerySurface,
): Set<string> {
   const names = file.model.explores.map((e) => e.name);
   if (surface.inert) return new Set();
   const relpath = normalizeRelpath(file.relativePath);
   const report = surface.reports.has(relpath);
   const entryPoint = report || surface.explores.has(relpath);
   if (!entryPoint) return new Set(names);

   // A report's own exports admit nothing: it can see a hidden file's sources
   // through an import.
   const own = new Set(report ? [] : exportedSourceNames(file.model));
   // Only a report brings its own text's declarations as derivation edges.
   const basesOf = report
      ? buildDerivationBaseMap(stripMalloyCommentsAndLiterals(file.text))
      : new Map<string, Set<string>>();

   const admitted = (name: string): boolean =>
      own.has(name) || admittedByPackage(file, surface, name);
   return new Set(
      names.filter(
         (name) => admittedRootsOf(name, admitted, basesOf) === undefined,
      ),
   );
}

/**
 * The admitted names `root` bottoms out in, following the bases `basesOf`
 * declares; undefined when the chain proves nothing. This is
 * `Model.derivesFromCurated`, returning what it reached rather than a yes or
 * no: `root` is proven when it is admitted itself, or every base declared for
 * it is proven. A cycle, a name with no declared base, or a chain past
 * MAX_CHAIN_NAMES proves nothing. The query route runs one walk per source, so the
 * memo and the chain length start fresh on each call.
 *
 * The one walk behind both {@link refusedByPublisher} (is it proven?) and
 * {@link derivedFromServed} (which served source does it reach?), so the two
 * cannot disagree about which chains count.
 */
function admittedRootsOf(
   root: string,
   admitted: (name: string) => boolean,
   basesOf: Map<string, Set<string>>,
): Set<string> | undefined {
   const proven = new Map<string, Set<string>>();
   const inProgress = new Set<string>();
   const walk = (name: string): Set<string> | undefined => {
      if (admitted(name)) return new Set([name]);
      const known = proven.get(name);
      if (known) return known;
      if (inProgress.has(name)) return undefined;
      if (inProgress.size >= MAX_CHAIN_NAMES) return undefined;
      const bases = basesOf.get(name);
      if (!bases || bases.size === 0) return undefined;
      inProgress.add(name);
      const roots = new Set<string>();
      let ok = true;
      for (const base of bases) {
         const reached = walk(base);
         if (!reached) {
            ok = false;
            break;
         }
         for (const r of reached) roots.add(r);
      }
      inProgress.delete(name);
      if (!ok) return undefined;
      proven.set(name, roots);
      return roots;
   };
   return walk(root);
}

/**
 * The sources in a report that are the same declaration a listed non-report
 * file exports: a report's plain re-import of a served source, like
 * `dashboards/overview.malloy`'s `orders`. Empty for any file that is not a
 * report.
 *
 * The query route runs a query addressed to these. The entities do not
 * advertise them, because the same declaration is already advertised from the
 * file that exports it, and a second result would only duplicate it. A
 * report's own derived sources (`open_orders is orders extend {...}`) are not
 * in this set; {@link derivedFromServed} covers them.
 */
export function servedElsewhere(
   file: SurfaceModelFile,
   surface: PackageQuerySurface,
): Set<string> {
   if (surface.inert) return new Set();
   if (!surface.reports.has(normalizeRelpath(file.relativePath))) {
      return new Set();
   }
   return new Set(
      file.model.explores
         .map((e) => e.name)
         .filter((name) => admittedByPackage(file, surface, name)),
   );
}

/**
 * The sources a served report declares on top of served sources, directly or
 * through a chain (`source: open_orders is orders extend {...}`), each mapped
 * to the served source it derives from: the file that serves it and the name
 * it is served under. Empty for any file that is not a served report.
 *
 * Exactly the sources the query route admits in a report through derivation
 * alone ({@link refusedByPublisher}'s third admission). A plain re-import is not
 * here ({@link servedElsewhere}), and neither is a source the route refuses.
 *
 * Each source reaches exactly one served source. Malloy refuses to compile a
 * file that declares a name twice ("Cannot redefine"), so every name in a
 * compiled file has one declared base, and its chain is a straight line to
 * one admitted name. `query_surface.integ.spec.ts` pins that refusal.
 */
export function derivedFromServed(
   file: SurfaceModelFile,
   surface: PackageQuerySurface,
): Map<string, ServedSourceAddress> {
   const out = new Map<string, ServedSourceAddress>();
   if (surface.inert) return out;
   if (!surface.reports.has(normalizeRelpath(file.relativePath))) return out;
   const basesOf = buildDerivationBaseMap(
      stripMalloyCommentsAndLiterals(file.text),
   );
   // A report's own exports admit nothing, so this is the whole of what
   // refusedByPublisher admits in a report.
   const admitted = (name: string): boolean =>
      admittedByPackage(file, surface, name);
   for (const { name } of file.model.explores) {
      // A plain re-import is servedElsewhere's.
      if (admitted(name)) continue;
      const roots = admittedRootsOf(name, admitted, basesOf);
      // Undefined: the query route refuses it.
      if (!roots) continue;
      const [root] = roots;
      const identity = definitionIdentity(file.model, root);
      const serving = identity ? surface.servingFiles.get(identity) : undefined;
      if (serving) out.set(name, serving);
   }
   return out;
}

/**
 * For each source in a served report that the query route runs or that is a
 * served declaration, the served source the entities advertise in its place: the file
 * that serves it and the name it is served under. Two kinds:
 *  - the same declaration a listed non-report file exports, plain
 *    ({@link servedElsewhere}) or aliased (`import { o is orders }`, which
 *    the query route refuses because it admits by name). Matched by declaration
 *    identity, never by name, so a report's own source that only shares a
 *    name with a served one maps to nothing here;
 *  - a source the report derives from served sources
 *    ({@link derivedFromServed}), mapped to the one it derives from.
 * Empty for any file that is not a served report.
 *
 * A host reads it so a search scoped to the report returns the served sources
 * the report reads, from the file that serves them.
 */
export function servedFrom(
   file: SurfaceModelFile,
   surface: PackageQuerySurface,
): Map<string, ServedSourceAddress> {
   const out = new Map<string, ServedSourceAddress>();
   if (surface.inert) return out;
   if (!surface.reports.has(normalizeRelpath(file.relativePath))) return out;
   for (const { name } of file.model.explores) {
      const identity = definitionIdentity(file.model, name);
      const serving = identity ? surface.servingFiles.get(identity) : undefined;
      if (serving) out.set(name, serving);
   }
   for (const [name, serving] of derivedFromServed(file, surface)) {
      out.set(name, serving);
   }
   return out;
}

/**
 * The sources in `file` the entities mark NOT_QUERYABLE: those the query route
 * refuses, plus every source a served report reads from the served surface:
 * its re-imports of served sources ({@link servedElsewhere}) and the sources
 * it derives from them ({@link derivedFromServed}). So nothing declared in a
 * report is its own search result. Empty when the surface is inert.
 *
 * Imported sources that fall out here keep their `#(index)` dimensions:
 * `getEntitiesFromExplore` re-roots each one under every queryable source that
 * joins it, and that copy is what a dimension index samples.
 */
export function nonQueryableSources(
   file: SurfaceModelFile,
   surface: PackageQuerySurface,
): Set<string> {
   return new Set([
      ...refusedByPublisher(file, surface),
      ...servedElsewhere(file, surface),
      ...derivedFromServed(file, surface).keys(),
   ]);
}
