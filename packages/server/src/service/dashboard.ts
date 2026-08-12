/**
 * Dashboard discovery: the Malloyyo `# artifact` grammar, read out of a
 * package's `dashboards/*.malloy` files into the manifest the REST surface
 * serves.
 *
 * Ported from Malloyyo's `@malloyyo/mcp-engine` (`artifacts.ts`,
 * `given-specs.ts` — MIT) rather than depended on: that is a private workspace
 * package. The *grammar* is the contract and is kept byte-compatible, so one
 * model repo works unchanged in both products; the code is an implementation
 * detail a shared malloydata package is expected to absorb later. See
 * `docs/malloyyo-dashboards-design.md` §Sourcing strategy.
 *
 * Two forms of dashboard, distinguished only by where the tag sits:
 *
 * - `# artifact` on a `query:` — a single-query dashboard. The renderer grid
 *   comes from the sibling `# dashboard {columns=N}` render tag.
 * - `## artifact { tiles=[…] }` at model level — a composite dashboard, whose
 *   named views run separately and combine into one grid.
 *
 * A `dashboards/*.malloy` carrying neither is a shared include: skipped, not an
 * error.
 */

import { isSourceDef } from "@malloydata/malloy";
import type {
   ModelDef,
   NamedModelObject,
   NamedQueryDef,
   TurtleDef,
} from "@malloydata/malloy";
import type { Tag } from "@malloydata/malloy-tag";
import { ownModelNotes } from "./annotations";
import {
   readGivenControlSpec,
   type GivenControlKind,
   type GivenSuggestSpec,
   type MalloyGivenApi,
} from "./given";
import {
   docCommentText,
   docCommentTitle,
   motlyAnnotations,
   motlyParseErrors,
   motlyTag,
   quoteFilterLiterals,
   readAutorun,
   readStartingGivens,
   tagNumeric,
   tagText,
   unwrapFilterLiteral,
} from "./motly";

// Re-exported because the MOTLY primitives moved to `motly.ts` but read as part
// of the dashboard grammar from the outside.
export {
   docCommentText,
   motlyAnnotations,
   quoteFilterLiterals,
   unwrapFilterLiteral,
};

/** The package-relative directory a dashboard file must live in. */
export const DASHBOARDS_DIR = "dashboards";

/**
 * Suffixes Malloyyo treats as a dashboard's custom component. Publisher does
 * not render these, and keeps the list only to warn that such a file was found
 * and ignored — see `unsupportedComponentWarnings`.
 */
export const COMPONENT_FILE_SUFFIXES = [".jsx", ".tsx"] as const;

const MODEL_FILE_SUFFIX = ".malloy";

/**
 * The control contract is a property of the `given:` declaration, not of the
 * dashboard that filters by it, so these are the shared types — a notebook
 * renders the same control for the same given. See `given.ts`.
 */
export type DashboardControlKind = GivenControlKind;
export type DashboardSuggestSpec = GivenSuggestSpec;

/**
 * One filter control on a dashboard: the given it binds plus the presentation
 * the given's own annotations ask for. The control contract is derived, never
 * authored per dashboard — a given declaration is a model concern, and each
 * dashboard simply references the ones it filters by.
 *
 * This is the ordinary API `Given`, not a dashboard-shaped copy of one. Its
 * `default` is therefore the *declaration's* default; a dashboard's own
 * starting values live in {@link DashboardManifest.givens} and are already
 * unwrapped to run shape.
 */
export type DashboardGivenSpec = MalloyGivenApi;

/** One tile of a composite dashboard. */
export interface DashboardTileSpec {
   /** The run expression exactly as written in `tiles=[…]` (`"orders -> by_month"`). */
   query: string;
   /**
    * Given names this tile references and the entry file can bind, or undefined
    * when the tile expression is not one discovery can resolve (see
    * {@link resolveTileGivens}). Running a tile with the dashboard's whole
    * control row also works — an unreferenced given is ignored — so this list is
    * for scoping: which tiles a changed control actually needs to re-run.
    */
   givenNames?: string[];
}

export interface DashboardManifest {
   /** The filename basename — the slug, the `# drill` target, the URL segment. */
   name: string;
   title: string;
   description?: string;
   /** The tagged query's name. Set for the single-query form only. */
   query?: string;
   /** Set for the composite (`tiles=[…]`) form only. */
   tiles?: DashboardTileSpec[];
   /** Grid width: `# dashboard {columns=N}`, or `dashboard_columns=N` on a composite. */
   dashboardColumns?: number;
   /**
    * Per-dashboard starting values for controls, in the shape the query
    * endpoint accepts (a `filter<…>` value is the filter body, not the `f'…'`
    * literal — see {@link unwrapFilterLiteral}). Overridden by URL params.
    */
   startingGivens?: Record<string, string>;
   /** False batches control changes behind an Apply button. Defaults true. */
   autorun: boolean;
   /** Package-relative path of the file the tag was read from. */
   entryFile: string;
   /**
    * The control row: one entry per given the dashboard's query references.
    *
    * `givens` is the declarations, here as everywhere else. Values keyed by
    * name are {@link startingGivens}; a bare list of names is `givenNames`.
    */
   givens: DashboardGivenSpec[];
}

/**
 * MOTLY tags the dashboard grammar owns.
 *
 * These share the `#` namespace with `@malloydata/render`'s tags but mean
 * nothing to the renderer, which reports every tag property it did not consume
 * as `Unknown render tag '<name>' on field '<field>'`. Without this list every
 * dashboard query would answer with a spurious render warning — `artifact` sits
 * on the query itself, and `drill` on the dimensions it makes clickable.
 *
 * The renderer's own suppression mechanism (marking a tag path as read) is
 * internal to it, so filtering the log afterwards is the available route. Drop
 * this once the grammar moves into a shared malloydata package the renderer can
 * know about (`docs/malloyyo-dashboards-design.md` §Follow-ups).
 */
const PUBLISHER_OWNED_TAGS = ["artifact", "drill"];

const UNKNOWN_RENDER_TAG = /^Unknown render tag '([^']+)'/;

/**
 * Drop "unknown render tag" complaints about tags Publisher owns, leaving every
 * other render finding intact. Matches on the tag's root property, so nested
 * paths (`artifact.title`) are covered too.
 */
export function filterPublisherOwnedRenderLogs<T extends { message?: string }>(
   logs: T[],
): T[] {
   return logs.filter((log) => {
      const match = UNKNOWN_RENDER_TAG.exec(log.message ?? "");
      if (!match) return true;
      return !PUBLISHER_OWNED_TAGS.includes(match[1].split(".")[0]);
   });
}

/**
 * True for a package-relative path that discovery considers, i.e. a `.malloy`
 * directly inside the top-level `dashboards/` directory. Nested subdirectories
 * are deliberately excluded: the slug is the basename, so `dashboards/a/x.malloy`
 * and `dashboards/b/x.malloy` would collide on one URL and one `# drill` target.
 */
export function isDashboardModelPath(modelPath: string): boolean {
   if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) return false;
   const segments = modelPath.split("/");
   return segments.length === 2 && segments[0] === DASHBOARDS_DIR;
}

/** `dashboards/overview.malloy` → `overview`. */
export function dashboardSlug(modelPath: string): string {
   const basename = modelPath.slice(modelPath.lastIndexOf("/") + 1);
   return basename.slice(0, -MODEL_FILE_SUFFIX.length);
}

/**
 * Whether a slug can actually be served at a URL, matching the `dashboardName`
 * pattern `api-doc.yaml` declares for the path parameter.
 *
 * Nothing validated it, and the slug is derived from a filename rather than
 * chosen, so a file could produce one that does not round-trip:
 * ADVISORY ONLY. A name outside the pattern is still served, because this
 * server routes it: the Express param matches `v1.2` and nothing validates the
 * pattern at runtime, which was measured rather than assumed. What the pattern
 * governs is what a client GENERATED from the spec will accept, so a name
 * outside it is worth reporting and not worth refusing.
 *
 * Two earlier justifications for this were both wrong, which is why the
 * reasoning is spelled out. `dashboards/.malloy` (an empty slug) cannot occur
 * at all, because `listPackageFiles` enumerates with `ignoreDotfiles` so a
 * leading-dot filename never becomes a model. And a dot mid-name does not break
 * routing, so withholding one broke working dashboards for teams that version a
 * filename. There is no traversal risk either way: the lookup is a `Map` and
 * never touches the filesystem.
 */
export function isServableDashboardSlug(slug: string): boolean {
   return /^[a-zA-Z0-9_-]+$/.test(slug);
}

/** The `artifact` sub-tag read into the manifest fields it contributes. */
interface ArtifactTagData {
   title?: string;
   tiles?: string[];
   dashboardColumns?: number;
   givens?: Record<string, string>;
   autorun: boolean;
}

function readArtifactTag(tag: Tag): ArtifactTagData | undefined {
   const artifact = tag.tag("artifact");
   if (!artifact) return undefined;

   const givens = readStartingGivens(artifact);
   const autorun = readAutorun(artifact);

   return {
      title: tagText(artifact, "title"),
      tiles: artifact
         .array("tiles")
         ?.map((tile) => tagText(tile))
         .filter((tile): tile is string => tile !== undefined),
      // The composite form carries its grid width inside the artifact tag; the
      // single-query form gets it from the `# dashboard` render tag, which the
      // renderer reads too.
      //
      // Read through the same strict reader the lint validates with, and
      // require a positive integer here as well. `Tag.numeric()` is parseFloat,
      // so it took `12abc` as 12 and `1e999` as Infinity, which JSON renders as
      // null against a field the spec declares an integer. That put a value on
      // the wire in the very case the lint was reporting as dropped, so the two
      // disagreed about the same tag.
      dashboardColumns:
         positiveInteger(tagNumeric(artifact, "dashboard_columns")) ??
         positiveInteger(tagNumeric(tag.tag("dashboard"), "columns")),
      givens,
      autorun,
   };
}

/**
 * A grid width, or undefined when the value is not one. Kept beside the lint's
 * own check so the manifest and the finding can never disagree about a tag.
 */
function positiveInteger(value: number | undefined): number | undefined {
   return value !== undefined && Number.isInteger(value) && value >= 1
      ? value
      : undefined;
}

/** One given declaration, as discovery needs it. */
export interface DashboardGivenDeclaration {
   name: string;
   type: string;
   default?: string;
   annotations: string[];
}

/**
 * The facts one compiled dashboard file contributes, lifted off its `ModelDef`
 * so the manifest build stays a pure function over plain data (and so the
 * grammar is unit-testable without a compile).
 */
export interface DashboardModelFacts {
   modelPath: string;
   /** Model-level (`##`) annotation texts, folded across the import lineage. */
   modelAnnotations: string[];
   /**
    * Every named query in the file, uncurated — `explores` curation is about
    * the discovery surface for agents, and a dashboard is a separate artifact.
    */
   queries: {
      name: string;
      annotations: string[];
      /** Names of the givens this query references. */
      givens: string[];
   }[];
   /**
    * Given declarations this file can actually *bind*, keyed by name.
    *
    * Malloy's given namespace is per-file and explicit: a query can reference a
    * given reachable through an import chain, but the entry file can only be
    * *run* with the givens it imports itself. Binding any other name fails with
    * "unknown given". So this map is the model's surfaced givens, not every
    * declaration in the compile closure — a control the server would reject is
    * worse than no control. A tile referencing a given the file does not import
    * is an authoring mistake for the lint to name.
    */
   givens: Map<string, DashboardGivenDeclaration>;
   /**
    * Given names each source view references, keyed by the run expression that
    * names it (`"orders -> by_month"`, normalized). This is what resolves a
    * composite dashboard's tiles, whose `tiles=[…]` entries are written in
    * exactly that form. Reference, not bindability: intersect with
    * {@link DashboardModelFacts.givens} before promising a control.
    *
    * Its key set doubles as "every view this file can run as a tile", which is
    * what lets the lint call out a tile that names nothing.
    */
   viewGivens: Map<string, string[]>;
   /** Field names per source, for validating `suggest { source= dimension= }`. */
   sourceFields: Map<string, Set<string>>;
   /**
    * Every `# drill` tag reachable in this file, one per tagged dimension. Drill
    * is declared on model dimensions rather than on a dashboard, so a file's
    * drills come from the sources it imports.
    */
   drills: DashboardDrill[];
}

/** A `# drill { to=[…] given=… }` tag on a source dimension. */
export interface DashboardDrill {
   source: string;
   dimension: string;
   /** Destinations: a dashboard slug each, or the literal `self`. */
   to: string[];
   /** Overrides the given name, which otherwise upper-cases the dimension. */
   given?: string;
}

/**
 * The run expression form a tile is written in, normalized so `"orders->by_x"`
 * and `"orders  ->  by_x"` are the same key.
 */
function normalizeTileExpression(tile: string): string {
   return tile
      .trim()
      .replace(/\s*->\s*/g, " -> ")
      .replace(/\s+/g, " ");
}

/**
 * Collect the given names referenced anywhere in a slice of Malloy IR.
 *
 * A view's `TurtleDef` carries no `givenUsage` summary the way a `Query` does,
 * but its pipeline holds the `{ node: 'given', refName }` reference nodes
 * themselves, so a structural walk answers the same question exactly. Used to
 * resolve a composite tile without compiling the tile expression.
 */
function collectGivenRefs(value: unknown, into: Set<string>): void {
   if (Array.isArray(value)) {
      for (const item of value) collectGivenRefs(item, into);
      return;
   }
   if (value === null || typeof value !== "object") return;
   const node = value as Record<string, unknown>;
   if (node.node === "given" && typeof node.refName === "string") {
      into.add(node.refName);
   }
   for (const child of Object.values(node)) collectGivenRefs(child, into);
}

/**
 * Read the dashboard-relevant facts off a compiled model.
 *
 * The two interesting fields are Malloy's own: `ModelDef.givens` is the
 * registry of given declarations (carrying their annotations, so the control
 * tags come along), and `Query.givenUsage` is the list of given ids a query
 * references. Together they answer "which givens does this dashboard filter
 * by, and how should each render" exactly, for the single-query form.
 *
 * Source views get the same answer the long way (`collectGivenRefs`), because
 * only a `Query` carries the precomputed `givenUsage` summary — a `TurtleDef`
 * does not. That covers composite tiles.
 *
 * `surfacedGivenNames` narrows the registry to what this file can be run with;
 * see {@link DashboardModelFacts.givens}. Pass every registry name only when the
 * caller genuinely wants the closure.
 */
export function readDashboardModelFacts(
   modelPath: string,
   modelDef: ModelDef,
   surfacedGivenNames: string[],
): DashboardModelFacts {
   const registry = modelDef.givens ?? {};
   const surfaced = new Set(surfacedGivenNames);
   const givens = new Map<string, DashboardGivenDeclaration>();
   for (const given of Object.values(registry)) {
      if (!surfaced.has(given.name)) continue;
      const type = given.type;
      givens.set(given.name, {
         name: given.name,
         type:
            type.type === "filter expression"
               ? `filter<${type.filterType}>`
               : type.type,
         default: given.defaultText,
         annotations: (given.annotations?.blockNotes ?? [])
            .concat(given.annotations?.notes ?? [])
            .map((note) => note.text),
      });
   }

   const isNamedQuery = (obj: NamedModelObject): obj is NamedQueryDef =>
      obj.type === "query";
   const queries = Object.values(modelDef.contents)
      .filter(isNamedQuery)
      .map((query) => ({
         name: query.as || query.name,
         annotations: [
            ...(query.annotations?.blockNotes ?? []),
            ...(query.annotations?.notes ?? []),
         ].map((note) => note.text),
         givens: (query.givenUsage ?? [])
            .map((usage) => registry[usage.id]?.name)
            .filter((name): name is string => name !== undefined),
      }));

   const viewGivens = new Map<string, string[]>();
   const sourceFields = new Map<string, Set<string>>();
   const drills: DashboardDrill[] = [];
   for (const obj of Object.values(modelDef.contents)) {
      if (!isSourceDef(obj)) continue;
      const sourceName = obj.as || obj.name;
      const fieldNames = new Set<string>();
      sourceFields.set(sourceName, fieldNames);

      // A source-level `where:` referencing a given scopes every view of that
      // source, but it lives on the source rather than in any view's pipeline,
      // so walking the pipeline alone would miss it — and a composite over a
      // given-scoped source would show an empty control row while its tiles
      // were silently filtered by the given's default. Collected once here and
      // unioned into each of the source's views.
      const sourceGivens = new Set<string>();
      collectGivenRefs(obj.filterList, sourceGivens);

      for (const field of obj.fields) {
         const fieldName = field.as || field.name;
         fieldNames.add(fieldName);
         if (field.type === "turtle") {
            const refs = new Set(sourceGivens);
            collectGivenRefs((field as TurtleDef).pipeline, refs);
            viewGivens.set(
               normalizeTileExpression(`${sourceName} -> ${fieldName}`),
               Array.from(refs),
            );
            continue;
         }
         const drill = readDrillTag(
            [
               ...(field.annotations?.blockNotes ?? []),
               ...(field.annotations?.notes ?? []),
            ].map((note) => note.text),
         );
         if (drill) {
            drills.push({ ...drill, source: sourceName, dimension: fieldName });
         }
      }
   }

   return {
      modelPath,
      // This file's own `##` only. An `## artifact` in a shared include
      // describes that include, not everything importing it.
      modelAnnotations: ownModelNotes(modelDef),
      queries,
      givens,
      viewGivens,
      sourceFields,
      drills,
   };
}

/**
 * Read a `# drill { to=[…] given=… }` tag off a dimension's annotations, or
 * undefined when it carries none. A bare `# drill` with no `to=` is treated as
 * absent: there is no destination to navigate to, and the lint reports it
 * separately rather than this returning a half-formed drill.
 */
function readDrillTag(
   annotations: string[],
): Omit<DashboardDrill, "source" | "dimension"> | undefined {
   const drill = motlyTag(annotations)?.tag("drill");
   if (!drill) return undefined;
   // Malloyyo accepts a single destination unbracketed (`to=overview`) as well
   // as the list form, so fall back to reading `to` as text.
   const to =
      tagTextArray(drill, "to") ?? [tagText(drill, "to")].filter(isString);
   const given = tagText(drill, "given");
   return given === undefined ? { to } : { to, given };
}

function isString(value: string | undefined): value is string {
   return value !== undefined;
}

/**
 * `Tag.textArray()` throws on a bad literal exactly as `text()` does, so read
 * the list with `array()` (which does not) and take each element through
 * {@link tagText}. One unreadable entry is dropped rather than costing the
 * whole list.
 */
function tagTextArray(
   tag: Tag | undefined,
   ...path: string[]
): string[] | undefined {
   const items = tag?.array(...path);
   return items?.map((item) => tagText(item)).filter(isString);
}

/**
 * The givens a composite tile references, or undefined when the tile expression
 * is not one we can resolve statically.
 *
 * Resolvable: Malloyyo's documented `source -> view` form, and a bare
 * model-level named query. Anything else (an inline refinement, a multi-stage
 * pipeline) leaves `givens` absent rather than guessing — a consumer that finds
 * it absent can fall back to sending the whole control row, which the server
 * accepts: a surfaced given a tile doesn't reference is simply ignored. The
 * per-tile list is precision, letting a viewer re-run only the tiles a changed
 * control affects.
 */
function resolveTileGivens(
   tile: string,
   facts: DashboardModelFacts,
): string[] | undefined {
   // Narrowed to what the entry file can bind: a tile may reference a given
   // reachable only through an import chain, and sending that name back would
   // guarantee an "unknown given" error.
   return referencedTileGivens(tile, facts)?.filter((name) =>
      facts.givens.has(name),
   );
}

/** {@link resolveTileGivens} before the bindability filter; the lint wants both. */
function referencedTileGivens(
   tile: string,
   facts: DashboardModelFacts,
): string[] | undefined {
   const normalized = normalizeTileExpression(tile);
   return (
      facts.viewGivens.get(normalized) ??
      facts.queries.find((query) => query.name === normalized)?.givens
   );
}

/** Build the control row for a set of given names, in declaration-tag order. */
function buildGivenSpecs(
   names: readonly string[],
   declarations: Map<string, DashboardGivenDeclaration>,
): DashboardGivenSpec[] {
   const specs: DashboardGivenSpec[] = [];
   for (const name of new Set(names)) {
      const declaration = declarations.get(name);
      // Absent when the given is referenced but not imported by the entry file,
      // so it is not bindable and gets no control. `lintDashboard` names that
      // case for both dashboard forms.
      if (!declaration) continue;
      specs.push(givenSpec(declaration));
   }
   return specs;
}

function givenSpec(declaration: DashboardGivenDeclaration): DashboardGivenSpec {
   return {
      name: declaration.name,
      type: declaration.type,
      default: declaration.default,
      ...readGivenControlSpec(declaration.annotations),
   };
}

/**
 * Build the manifest for one dashboard file, or undefined when the file carries
 * no artifact tag (a shared include).
 *
 * The model-level (`##`) tag is checked first so a composite declaration wins
 * over an incidental query-level tag in the same file; Malloyyo treats the two
 * forms as alternatives, not a combination.
 */
export function buildDashboardManifest(
   facts: DashboardModelFacts,
): DashboardManifest | undefined {
   const name = dashboardSlug(facts.modelPath);
   const base = {
      name,
      entryFile: facts.modelPath,
   };

   const modelTag = motlyTag(facts.modelAnnotations);
   const composite = modelTag ? readArtifactTag(modelTag) : undefined;
   if (composite?.tiles?.length) {
      const tiles = composite.tiles.map((query) => {
         const givenNames = resolveTileGivens(query, facts);
         return givenNames ? { query, givenNames } : { query };
      });
      return {
         ...base,
         title:
            composite.title ?? docCommentTitle(facts.modelAnnotations) ?? name,
         description: docCommentText(facts.modelAnnotations),
         tiles,
         dashboardColumns: composite.dashboardColumns,
         startingGivens: composite.givens,
         autorun: composite.autorun,
         // A composite's control row is the union across its tiles: each tile
         // runs with only the givens it references, but the row a viewer shows
         // is every given any tile can filter by.
         givens: buildGivenSpecs(
            tiles.flatMap((tile) => tile.givenNames ?? []),
            facts.givens,
         ),
      };
   }

   for (const query of facts.queries) {
      const tag = motlyTag(query.annotations);
      const artifact = tag ? readArtifactTag(tag) : undefined;
      if (!artifact) continue;
      const doc = docCommentText(query.annotations);
      const docTitle = docCommentTitle(query.annotations);
      return {
         ...base,
         title: artifact.title ?? docTitle ?? name,
         description: doc,
         query: query.name,
         dashboardColumns: artifact.dashboardColumns,
         startingGivens: artifact.givens,
         autorun: artifact.autorun,
         givens: buildGivenSpecs(query.givens, facts.givens),
      };
   }

   return undefined;
}

/**
 * One load-time lint finding, in the shape the package-warnings surface serves.
 * `model` is filled in by the caller, which knows the file.
 */
export interface DashboardLintFinding {
   /**
    * What the finding is about: a dashboard slug, a tile, a dimension. For a
    * `# drill` finding this is where the tag is *declared* — deliberately not
    * called `target`, which in this file means where a drill points.
    */
   subject: string;
   message: string;
   severity: "error" | "warn";
}

/** A tile written as a plain `source -> view`, or a bare name. */
function plainTileParts(
   tile: string,
): { source: string; view: string } | { name: string } | undefined {
   const piped = /^(\w+)\s*->\s*(\w+)$/.exec(tile.trim());
   if (piped) return { source: piped[1], view: piped[2] };
   const bare = /^\w+$/.exec(tile.trim());
   return bare ? { name: tile.trim() } : undefined;
}

/**
 * Lint one dashboard file: Malloyyo's `lint` verb, on Publisher's existing
 * load-time package-warnings surface rather than as a new verb. Loud where
 * authors see it, instead of dead-ending a viewer at click time.
 *
 * Deliberately conservative about what it calls broken. A tile expression can
 * be any runnable Malloy (`orders -> by_brand + { limit: 5 }`), which discovery
 * does not resolve statically, so only *plain* `source -> view` and bare-name
 * tiles are checked — a form that can be wrong in a way we can prove. Anything
 * else is left alone rather than warned about speculatively.
 *
 * Compilation itself is not rechecked here: every dashboard file is compiled as
 * its own entry by the ordinary package load, and a failure there already
 * surfaces as that model's compilation error. Drill targets are package-wide,
 * so they are checked once by {@link lintDrillTargets} rather than here.
 */
export function lintDashboard(
   facts: DashboardModelFacts,
   manifest: DashboardManifest,
): DashboardLintFinding[] {
   const findings: DashboardLintFinding[] = [];
   const slug = manifest.name;
   const add = (message: string, severity: "error" | "warn" = "warn") =>
      findings.push({ subject: slug, message, severity });

   const ownTag = manifest.tiles
      ? motlyTag(facts.modelAnnotations)
      : motlyTag(
           facts.queries.find((query) => query.name === manifest.query)
              ?.annotations ?? [],
        );
   const artifactTag = ownTag?.tag("artifact");

   // The grid width has two spellings and BOTH reach the manifest (see
   // readArtifactTag), so both are checked here. Checking only the artifact-tag
   // spelling left `# dashboard { columns=… }`, the form the shipped examples
   // use, silently dropped on a bad value.
   for (const [tag, key, written] of [
      [artifactTag, "dashboard_columns", "dashboard_columns"],
      [ownTag?.tag("dashboard"), "columns", "# dashboard { columns=… }"],
   ] as const) {
      // A bad value is invisible in the manifest, because `positiveInteger`
      // drops it there too, which is exactly why it needs saying aloud. Both
      // sides go through the same helper so they cannot disagree about a tag.
      if (!tag?.has(key)) continue;
      if (positiveInteger(tagNumeric(tag, key)) !== undefined) continue;
      // What actually happens depends on the OTHER spelling, because the two
      // are read with `??`. Saying "falls back to the renderer default" was
      // right only when neither spelling supplied a usable value; when this one
      // is bad and the other is good, the grid quietly uses the other, and a
      // finding naming a default the author never sees is worse than none.
      add(
         `${written} must be a positive integer, got ` +
            `${JSON.stringify(tagText(tag, key))}. ` +
            (manifest.dashboardColumns === undefined
               ? `The grid falls back to the renderer default.`
               : `The grid uses ${manifest.dashboardColumns} instead.`),
      );
   }

   for (const { query: tile } of manifest.tiles ?? []) {
      const parts = plainTileParts(tile);
      if (!parts) continue;
      const resolved =
         referencedTileGivens(tile, facts) !== undefined ||
         ("name" in parts &&
            facts.queries.some((query) => query.name === parts.name));
      if (!resolved) {
         const detail =
            "source" in parts
               ? facts.sourceFields.has(parts.source)
                  ? `source "${parts.source}" has no view "${parts.view}"`
                  : `no source "${parts.source}" in this file`
               : `no query named "${parts.name}" in this file`;
         add(`tile "${tile}" does not resolve: ${detail}.`, "error");
      }

      // A tile can reference a given through an import chain that the entry
      // file never imported itself, which leaves it with no control: the value
      // silently stays at its default. Bindability is per-file, so the fix is an
      // import in this file.
      const unbindable = (referencedTileGivens(tile, facts) ?? []).filter(
         (name) => !facts.givens.has(name),
      );
      for (const name of unbindable) {
         add(
            `tile "${tile}" filters by given "${name}", which this file does ` +
               `not import, so no control is shown for it and it stays at its ` +
               `default. Add it to an import in ${facts.modelPath}.`,
         );
      }
   }

   // The same loss, on the single-query form. It used to be reported for tiles
   // only, so a single-query dashboard dropped the control in silence while
   // `buildGivenSpecs` claimed the lint covered it.
   if (manifest.query !== undefined) {
      const query = facts.queries.find((q) => q.name === manifest.query);
      for (const name of query?.givens ?? []) {
         if (facts.givens.has(name)) continue;
         add(
            `query "${manifest.query}" filters by given "${name}", which this ` +
               `file does not import, so no control is shown for it and it ` +
               `stays at its default. Add it to an import in ` +
               `${facts.modelPath}.`,
         );
      }
   }

   for (const spec of manifest.givens) {
      const suggest = spec.suggest;
      if (!suggest) {
         // `readGivenControlSpec` drops a `suggest` block that cannot fetch
         // options. `source` with no `dimension` names no column, and `dimension`
         // with neither `query` nor `source` names nothing to read it from, so
         // an author who wrote one of those gets no suggest AND, without this,
         // no finding either. The declaration is still on the given's tags, so
         // read it back to say what was dropped and why.
         const declared = motlyTag(
            facts.givens.get(spec.name)?.annotations ?? [],
         )?.tag("suggest");
         if (declared) {
            const has = (key: string) => tagText(declared, key) !== undefined;
            const missing = has("dimension")
               ? `it names a dimension but no query= or source= to read it from`
               : has("source")
                 ? `source= alone names no column to read options from`
                 : `it names neither a query= nor a source=`;
            add(
               `given "${spec.name}" declares a suggest that cannot fetch ` +
                  `options: ${missing}. Use query=, source= with dimension=, ` +
                  `or query= with dimension=. No control options are offered ` +
                  `for it.`,
               "error",
            );
         }
         continue;
      }
      if (
         suggest.query !== undefined &&
         !facts.queries.some((query) => query.name === suggest.query)
      ) {
         add(
            `given "${spec.name}" suggests options from query ` +
               `"${suggest.query}", which this file does not define.`,
            "error",
         );
      }
      if (suggest.source !== undefined) {
         const fields = facts.sourceFields.get(suggest.source);
         if (!fields) {
            add(
               `given "${spec.name}" suggests options from source ` +
                  `"${suggest.source}", which this file does not define.`,
               "error",
            );
         } else if (
            suggest.dimension !== undefined &&
            !fields.has(suggest.dimension)
         ) {
            add(
               `given "${spec.name}" suggests options from ` +
                  `"${suggest.source} -> ${suggest.dimension}", but that ` +
                  `source has no field "${suggest.dimension}".`,
               "error",
            );
         }
      }
   }

   return findings;
}

/**
 * Explain a `dashboards/*.malloy` that produced no dashboard.
 *
 * A file with no artifact tag is a shared include and silent, as Malloyyo
 * treats it. But a file whose tag *failed to parse* is a dashboard that simply
 * vanished: MOTLY discards the whole tag on a syntax error, so the file falls
 * through to the include path and never appears in a listing or at a URL. That
 * is the least debuggable outcome in the whole feature, so it gets a finding.
 */
export function lintUndiscoveredDashboard(
   facts: DashboardModelFacts,
): DashboardLintFinding[] {
   const subject = dashboardSlug(facts.modelPath);
   const errors = [
      ...motlyParseErrors(facts.modelAnnotations),
      ...facts.queries.flatMap((query) => motlyParseErrors(query.annotations)),
   ];
   const findings = errors.map((message) => ({
      subject,
      message:
         `Tag does not parse (${message}), so the whole tag is discarded and ` +
         `this file is treated as a shared include rather than a dashboard.`,
      severity: "error" as const,
   }));
   if (findings.length > 0) return findings;

   // A tag that PARSES but describes no dashboard vanishes just as completely,
   // and the checks above cannot see it because there is no parse error to
   // report. The common case is a model-level `## artifact` with no `tiles=`:
   // the composite branch is gated on having at least one tile, and no
   // query-level artifact tag follows, so the file produces nothing at all.
   const modelArtifact = motlyTag(facts.modelAnnotations)?.tag("artifact");
   if (modelArtifact) {
      const tiles = modelArtifact.array("tiles");
      findings.push({
         subject,
         message:
            (tiles === undefined
               ? `Model-level '## artifact' declares no tiles=, `
               : `Model-level '## artifact' declares an empty tiles=, `) +
            `so this file produces no dashboard. A composite dashboard needs ` +
            `tiles=["source -> view", …]; for a single-query dashboard put ` +
            `the '# artifact' tag on the query instead.`,
         severity: "error" as const,
      });
   }
   return findings;
}

/**
 * Report a given whose own annotations do not parse as MOTLY.
 *
 * This is the only signal that exists for it. A failed parse yields an EMPTY
 * tag rather than `undefined`, so `readGivenControlSpec` cannot detect one: it
 * returns `{}` and the given silently loses its whole control contract: label,
 * control kind, range and suggest, with nothing reported anywhere. The commonest
 * cause is a bare filter literal, which is rescued before parsing, so what
 * reaches here is the residue that rescue could not save.
 *
 * Package-wide rather than per-dashboard because a given is declared on a model
 * and reached through imports, so the same broken declaration would otherwise be
 * reported once per dashboard that imports it, or not at all when no dashboard
 * does. Deduplicated on name and message for that reason.
 *
 * Bounded by what {@link motlyParseErrors} can honestly claim: syntax only, no
 * position, and silent on a line that only parses after rescue. So this catches
 * *a* malformed tag, and its absence is not proof the tags are well formed.
 */
export function lintGivenTags(
   facts: readonly DashboardModelFacts[],
): DashboardLintFinding[] {
   const findings = new Map<string, DashboardLintFinding>();
   for (const file of facts) {
      for (const [name, declaration] of file.givens) {
         for (const message of motlyParseErrors(declaration.annotations)) {
            findings.set(`${name}|${message}`, {
               subject: name,
               message:
                  `given "${name}" has an annotation that does not parse ` +
                  `(${message}), so the whole line is discarded and the given ` +
                  `loses any label, control, range or suggest it declared. ` +
                  `It still accepts values; only its presentation is lost.`,
               severity: "error",
            });
         }
      }
   }
   return Array.from(findings.values());
}

/**
 * Validate every `# drill { to=… }` in the package against the set of
 * dashboards that exist, so a dead-ended click is caught at load.
 *
 * Package-wide rather than per-dashboard because drill is declared on a model
 * *dimension*: the same tagged dimension is reachable from every dashboard that
 * imports its source, and a single broken target should be reported once, not
 * once per importer. Keyed on the dimension and destination for that reason.
 */
export function lintDrillTargets(
   facts: readonly DashboardModelFacts[],
   knownSlugs: ReadonlySet<string>,
): DashboardLintFinding[] {
   const findings = new Map<string, DashboardLintFinding>();
   for (const file of facts) {
      for (const drill of file.drills) {
         const where = `${drill.source}.${drill.dimension}`;
         if (drill.to.length === 0) {
            findings.set(where, {
               subject: where,
               message: `# drill on ${where} names no destination (to=…).`,
               severity: "error",
            });
            continue;
         }
         for (const destination of drill.to) {
            if (destination === "self" || knownSlugs.has(destination)) continue;
            findings.set(`${where}|${destination}`, {
               subject: where,
               message:
                  `# drill on ${where} targets "${destination}", which is not ` +
                  `a dashboard in this package.`,
               severity: "error",
            });
         }
      }
   }
   return Array.from(findings.values());
}

/**
 * Validate every `# drill { to=self }` against the givens the package's
 * documents actually surface.
 *
 * `to=self` filters the document the click came from, which means it needs a
 * control to write the clicked value into. Unlike a drill *target*, whose
 * validity is a package-wide fact, self-validity is per-document: the tag is on
 * a dimension many dashboards and notebooks import, and a given one of them
 * surfaces another may not, which is an authoring choice rather than a defect.
 *
 * So this reports only the half that is provable without knowing which document
 * was clicked — a given **no** model in the package declares, which leaves the
 * tag dead wherever it fires rather than merely unhonored in one place. Warning
 * per-document instead would fire on every document that imports the source
 * without ever grouping that dimension, which is most of them. The narrower
 * case stays a click-time console warning naming the given and the fix.
 */
export function lintSelfDrills(
   facts: readonly DashboardModelFacts[],
): DashboardLintFinding[] {
   const surfaced = new Set<string>();
   for (const file of facts) {
      for (const name of file.givens.keys()) surfaced.add(name);
   }

   const findings = new Map<string, DashboardLintFinding>();
   for (const file of facts) {
      for (const drill of file.drills) {
         if (!drill.to.includes("self")) continue;
         const given = drill.given ?? drill.dimension.toUpperCase();
         if (surfaced.has(given)) continue;
         const where = `${drill.source}.${drill.dimension}`;
         findings.set(where, {
            subject: where,
            message:
               `# drill on ${where} has to=self, but no model in this ` +
               `package declares a given "${given}" for the clicked value to ` +
               `filter on, so the drill cannot fire anywhere. Declare it, or ` +
               `point the tag at a given that exists with given=.`,
            severity: "error",
         });
      }
   }
   return Array.from(findings.values());
}
