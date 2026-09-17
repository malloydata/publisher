// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The dashboard builder's working representation of a `dashboards/*.malloy`
 * file.
 *
 * This is an editing PROJECTION, not a complete representation of the file, and
 * the distinction is load-bearing. The builder writes changes by SPLICING the
 * source text — patching the ranges it owns and leaving every other byte alone —
 * so anything this type does not model survives an edit untouched. A `where:` in
 * a source extension, an inline computed dimension, a `#(doc)` tag, a reader's
 * comments: none of them appear below, and none of them are lost.
 *
 * What that buys is the reason the projection exists. A writer that regenerated
 * the whole file would have to model everything or destroy it, which is why the
 * earlier design could only ever have edited files it wrote itself.
 *
 * The document is still what decides whether a file can be opened at all: the
 * reader refuses a file it cannot fully turn into one of these, rather than
 * opening it half-understood. See {@link readDashboardDocument}.
 */

/** One `import` line. The file needs both forms, and for different reasons. */
export type DashboardImport =
   /** `import "../givens.malloy"` — the whole file's declarations. */
   | { kind: "all"; from: string }
   /**
    * `import { products, regions } from "../storefront.malloy"` — specific
    * names. Not interchangeable with the bare form: a bare import is not
    * transitive, so a dashboard whose control has `suggest { source=products }`
    * must name `products` here or the package load reports an error for every
    * such given.
    */
   | { kind: "names"; names: string[]; from: string };

/**
 * A given this dashboard declares itself, rather than taking from the model.
 *
 * This is the CONVENTION for a dashboard the builder edits, not the exception:
 * the builder adds and removes filters, and a filter it can add has to be a
 * declaration in the one file it writes. A given imported from the model is
 * still read and still bindable, but it is the model's, and the builder never
 * edits imports or model files.
 *
 * Measured, and the reason the convention has to be this way round: a local
 * given does NOT drive a model-level `where:` of the same name. Binding is per
 * declaration, not per name, so a dashboard that declares `CATEGORY` and extends
 * a source whose own `where:` reads the model's `CATEGORY` gets a control that
 * moves nothing. The filtering therefore lives on the dashboard's tiles, as
 * {@link DashboardTile.filters} — which is what the builder writes anyway.
 *
 * The fields are the declaration and its control contract, spelled as in the
 * file. A tag this does not model survives an edit like any other unmodelled
 * Malloy, because the writer patches rather than regenerates.
 */
export interface LocalGiven {
   name: string;
   /** The declared type, spelled as in the file: `filter<string>`, `date`, … */
   type: string;
   /** The default, spelled as in the file: `f'Jeans'`, `@2023-01-01`, … */
   default: string;
   label?: string;
   /** `# description="…"`: helper text under the control. */
   description?: string;
   /** `control=select` / `multiselect`. */
   control?: string;
   /**
    * `suggest { source=products dimension=category }`, or `{ query=… }`: where
    * a picker gets its options. The source or query has to resolve in this file,
    * and the builder does not add imports — so a given it creates suggests over
    * a source the file already reads, or over nothing.
    */
   suggest?: { source?: string; query?: string; dimension: string };
   /** `range_min=` / `range_max=` on a `filter<number>`: a slider's bounds. */
   rangeMin?: number;
   rangeMax?: number;
}

/** A `# drill` dimension the dashboard declares in its own source extension. */
export interface DashboardDrill {
   /** The source this dimension is declared on. */
   source: string;
   /** `dimension: <name> is <expression>`. */
   name: string;
   expression: string;
   /** Dashboard slugs, or the literal `self`. */
   to: string[];
   /** The given a click seeds. Absent means the dimension's own name. */
   given?: string;
}

/**
 * Where a tile's view is declared, and therefore what the builder may edit.
 *
 * Three forms, all of them found in dashboards that ship in this repository, and
 * the distinction decides which affordances a tile gets:
 *
 * - `reference` — `view: revenue_trend is sales_by_month` in this file. Fully
 *   editable: the layout tags sit above that line and the builder owns them.
 * - `inline` — `view: order_tile is { aggregate: order_count }` in this file.
 *   Its tags are editable for the same reason, and so is a FILTER: a binding is
 *   a depth-1 `where:` statement in the body's own first stage rather than a `+
 *   { … }` refinement, but it is still a declaration this file owns and the
 *   reader reads back — see {@link DashboardTile.filters} and BINDING_CLAUSE in
 *   readDocument.ts. The QUERY otherwise is not editable, because authoring a
 *   query body is a separate feature; the body is never rewritten beyond its
 *   binding lines, so the rest of it cannot be damaged. A body shaped so that
 *   its first stage is ambiguous — a multi-stage `->` pipeline, a `{ … } + { …
 *   }` compound refinement — is read as-is but refuses a filter CHANGE; see
 *   `planInlineFilters` in spliceDocument.ts.
 * - `inherited` — not declared in this file at all, as in a dashboard whose only
 *   tile is `orders -> by_brand` against an imported source. Nothing about it is
 *   editable here, because its tags live on the model's own view and the builder
 *   does not write model files. Read and shown, not changed.
 */
export type TileDeclaration =
   | { kind: "reference"; from: string }
   | { kind: "inline" }
   | { kind: "inherited" };

/** One tile: a view shown on the page, plus how it is presented. */
export interface DashboardTile {
   /**
    * The view name. For a tile declared in this file, assigned ONCE when the
    * tile is added and never recomputed — deriving it from position would
    * rename every later tile on a reorder, turning a pure-layout edit into a
    * large diff.
    */
   name: string;
   /**
    * The source it reads, by NAME — an index would break on reorder. May name a
    * source this file declares in {@link DashboardDocument.sources}, or one it
    * merely imports.
    */
   source: string;
   declaration: TileDeclaration;
   /**
    * Given bindings: a `+ { where: … }` refinement on a `reference` tile, or a
    * depth-1 `where:` statement in an `inline` tile's own first stage — see
    * {@link TileDeclaration}.
    *
    * `op` is the comparison, and absent means `~`, which is how a `filter<…>`
    * given binds. A plain `date` or `number` given is a VALUE, not a filter,
    * and compares with `>=`, `<=` or `=` — measured: `created_at >= $SINCE`
    * compiles and `created_at ~ $SINCE` does not, on a `date` given.
    */
   filters?: Array<{ field: string; given: string; op?: string }>;
   label?: string;
   subtitle?: string;
   colspan?: number;
   break?: boolean;
   borderless?: boolean;
}

/**
 * A source extension the dashboard DECLARES, holding tiles that read it.
 *
 * A LIST, not one source, because a composite dashboard exists precisely to
 * combine queries no single Malloy result can span. Measured: a dashboard whose
 * tiles come from two sources loads clean and scopes givens per source. A picker
 * over a package catalog offers every source's views side by side, so the second
 * tile a reader adds can already need this.
 *
 * May be EMPTY. A dashboard whose tiles all read imported sources declares no
 * extension at all — `tiles=["orders -> by_brand"]` over an imported `orders` is
 * a complete dashboard with nothing else in the file.
 */
export interface DashboardSource {
   name: string;
   /** What it extends: `source: <name> is <base> extend { … }`. */
   base: string;
   /** A `#(doc)` description on the source. */
   doc?: string;
   /**
    * The dimensions this extension declares itself, in file order. These are
    * the only fields a `# drill` can be authored on here: a drill is a tag on
    * the dimension's declaration, and the model's dimensions are declared in
    * the model.
    */
   dimensions?: DashboardDimension[];
}

/** A tile's identity across a reorder: what the grid keys on, a drag names and the writer matches. */
export const tileKey = (tile: { source: string; name: string }) =>
   `${tile.source}.${tile.name}`;

/** `dimension: <name> is <expression>` in the dashboard's own extension. */
export interface DashboardDimension {
   name: string;
   expression: string;
}

export interface DashboardDocument {
   title: string;
   /** The narrative header, as markdown. Emitted as `##"` lines. */
   description?: string;
   /** `# dashboard { columns=N }`. */
   columns?: number;
   /** `## artifact { autorun=false }`. Absent means autorun. */
   autorun?: boolean;
   imports: DashboardImport[];
   sources: DashboardSource[];
   localGivens?: LocalGiven[];
   /** `## artifact { givens { … } }` — where the controls open. */
   startingGivens?: Record<string, string>;
   drills?: DashboardDrill[];
   /**
    * Tiles in layout order. The grid flows in declaration order, so this array
    * IS the layout, and there is no separate ordering field to fall out of step
    * with it.
    *
    * One array drives both halves of the file: the `## artifact { tiles=[…] }`
    * list and the `view:` declarations inside each extension. They therefore
    * cannot disagree, which they can and do in hand-written files.
    */
   tiles: DashboardTile[];
}
