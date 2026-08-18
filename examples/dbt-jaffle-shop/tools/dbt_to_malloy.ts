/*
 * dbt -> Malloy converter (prototype of `malloy-pub dbt bind` + the semantic-layer converter).
 *
 * Reads three dbt artifacts and writes two Malloy files:
 *
 *   catalog.json           -> the column list and types for each model (the binding contract)
 *   manifest.json          -> model and column descriptions (the docs)
 *   semantic_manifest.json -> entities, dimensions, metrics, saved queries (the semantics)
 *
 *   bindings.malloy    one query source per dbt model: every column, with dbt's
 *                      descriptions as #(doc). Generated; never hand-edit. Schema
 *                      drift shows up here as a reviewable file diff.
 *   jaffle_shop.malloy the semantic layer: primary keys, joins, measures, and views
 *                      derived from dbt's semantic manifest.
 *
 * Read `semantic_manifest.json`, not `osi_document.json`. The OSI export flattens
 * every metric to a SQL string and loses information the converter needs: filtered
 * metrics come out referencing MetricFlow's internal qualified dimension names
 * (`order_id__order_total_dim`), which are not real columns, and a derived metric's
 * `offset_window` is dropped entirely, so `revenue_growth_mom` becomes
 * `(SUM(x) - SUM(x))*100/SUM(x)` -- a silent zero. The structured manifest keeps both.
 *
 * Usage:
 *   bun run tools/dbt_to_malloy.ts --target <dbt-target-dir> [--check]
 *
 * `--check` regenerates in memory and diffs against the committed files, exiting
 * non-zero on drift, without writing anything.
 */

import { existsSync, readFileSync, writeFileSync } from "fs";
import path from "path";

// ── dbt artifact shapes (only the fields this converter reads) ──────────────

interface CatalogColumn {
  name: string;
  type: string;
  index: number;
}
interface Catalog {
  nodes: Record<string, { columns: Record<string, CatalogColumn> }>;
}
interface Manifest {
  nodes: Record<
    string,
    {
      name: string;
      description?: string;
      columns?: Record<string, { name: string; description?: string }>;
    }
  >;
}
interface Entity {
  name: string;
  type: "primary" | "foreign" | "unique" | "natural";
  expr: string | null;
  description?: string;
}
interface Dimension {
  name: string;
  type: "categorical" | "time";
  expr: string | null;
  description?: string;
  type_params?: { time_granularity?: string } | null;
}
interface SemanticModel {
  name: string;
  description?: string;
  node_relation: { alias: string; schema_name: string; database: string };
  defaults?: { agg_time_dimension?: string } | null;
  entities: Entity[];
  dimensions: Dimension[];
}
interface MetricInput {
  name: string;
  alias: string | null;
  offset_window: { count: number; granularity: string } | null;
  offset_to_grain: string | null;
}
interface Metric {
  name: string;
  description?: string;
  label?: string;
  type: "simple" | "ratio" | "derived" | "cumulative" | "conversion";
  type_params: {
    expr: string | null;
    numerator: MetricInput | null;
    denominator: MetricInput | null;
    metrics: MetricInput[];
    cumulative_type_params?: unknown;
    metric_aggregation_params: {
      semantic_model: string;
      agg: string;
      agg_time_dimension?: string;
    } | null;
  };
  filter?: { where_filters: { where_sql_template: string }[] } | null;
}
interface SavedQuery {
  name: string;
  description?: string;
  label?: string;
  query_params: {
    metrics: string[];
    group_by: string[];
    where: unknown;
    limit: number | null;
  };
}
interface SemanticManifest {
  semantic_models: SemanticModel[];
  metrics: Metric[];
  saved_queries: SavedQuery[];
}

// ── Helpers ────────────────────────────────────────────────────────────────

/** Records every dbt construct this converter chose not to emit, and why. */
interface Gap {
  kind: string;
  name: string;
  reason: string;
}
const gaps: Gap[] = [];
const note = (kind: string, name: string, reason: string) =>
  gaps.push({ kind, name, reason });

/** A `#(doc)` block. dbt descriptions are free text and may span lines. */
function docLines(description: string | undefined, indent: string): string[] {
  const text = (description ?? "").trim();
  if (!text) return [];
  return text
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean)
    .map((l) => `${indent}#(doc) ${l}`);
}

/**
 * Malloy rejects a field that redefines an existing name ("Cannot redefine"), so a
 * dbt metric named after its own column -- `order_total` summing `order_total` --
 * cannot keep that name unless the column yields it. The binding renames the
 * passthrough column to `<name>_raw`; the metric keeps the name dbt published,
 * which is the name analysts and agents search for.
 */
const rawName = (column: string) => `${column}_raw`;

// ── Load artifacts ─────────────────────────────────────────────────────────

const argv = process.argv.slice(2);
const argOf = (flag: string) => {
  const i = argv.indexOf(flag);
  return i >= 0 ? argv[i + 1] : undefined;
};
const checkOnly = argv.includes("--check");
const targetDir = argOf("--target");
if (!targetDir) {
  console.error(
    "Invalid --target: expected a dbt target directory containing catalog.json, " +
      "manifest.json, and semantic_manifest.json.\n" +
      "Fix: bun run tools/dbt_to_malloy.ts --target ../jaffle-shop/target",
  );
  process.exit(2);
}
const readJson = <T>(file: string): T => {
  const p = path.join(targetDir, file);
  if (!existsSync(p)) {
    console.error(
      `Missing ${file} in ${targetDir}. Run \`dbt build\` and \`dbt docs generate\` first ` +
        `(docs generate is what writes catalog.json).`,
    );
    process.exit(2);
  }
  return JSON.parse(readFileSync(p, "utf8")) as T;
};

const catalog = readJson<Catalog>("catalog.json");
const manifest = readJson<Manifest>("manifest.json");
const semantic = readJson<SemanticManifest>("semantic_manifest.json");

const modelsByName = new Map(semantic.semantic_models.map((m) => [m.name, m]));

/** dbt node ids are `model.<project>.<name>`; index by the trailing name. */
const nodeByModelName = new Map<string, string>();
for (const id of Object.keys(manifest.nodes)) {
  const parts = id.split(".");
  if (parts[0] === "model") nodeByModelName.set(parts[parts.length - 1], id);
}

const columnsOf = (modelName: string): CatalogColumn[] => {
  const id = nodeByModelName.get(modelName);
  const node = id ? catalog.nodes[id] : undefined;
  if (!node) return [];
  return Object.values(node.columns).sort((a, b) => a.index - b.index);
};
const columnDocs = (modelName: string): Map<string, string> => {
  const id = nodeByModelName.get(modelName);
  const cols = (id && manifest.nodes[id]?.columns) || {};
  const out = new Map<string, string>();
  for (const c of Object.values(cols)) {
    if (c.description?.trim()) out.set(c.name, c.description);
  }
  return out;
};
const modelDoc = (modelName: string): string | undefined => {
  const id = nodeByModelName.get(modelName);
  return id ? manifest.nodes[id]?.description : undefined;
};

// ── Which columns must be renamed to free a name for a metric? ─────────────

/** model name -> set of columns the binding must rename, because a metric owns the name. */
const renames = new Map<string, Set<string>>();
for (const metric of semantic.metrics) {
  const agg = metric.type_params.metric_aggregation_params;
  if (!agg) continue; // only simple metrics bind to a model
  const cols = new Set(columnsOf(agg.semantic_model).map((c) => c.name));
  if (cols.has(metric.name)) {
    if (!renames.has(agg.semantic_model))
      renames.set(agg.semantic_model, new Set());
    renames.get(agg.semantic_model)!.add(metric.name);
  }
}
/** Resolve a physical column to the name the binding exposes it under. */
const boundColumn = (model: string, column: string) =>
  renames.get(model)?.has(column) ? rawName(column) : column;

// ── Emit bindings.malloy ───────────────────────────────────────────────────

function emitBindings(): string {
  const out: string[] = [];
  out.push(
    "// GENERATED by tools/dbt_to_malloy.ts from dbt artifacts -- do not edit by hand.",
    "//",
    "// One binding source per dbt model. Every column dbt built is listed here with its",
    "// dbt description as #(doc), so documentation is authored once in schema.yml and",
    "// flows to Malloy, the Explorer UI, and any agent reading the model over MCP.",
    "//",
    "// Regenerate with:",
    "//   bun run tools/dbt_to_malloy.ts --target <dbt-target-dir>",
    "// Check for drift (exits non-zero if this file is stale):",
    "//   bun run tools/dbt_to_malloy.ts --target <dbt-target-dir> --check",
    "",
  );

  for (const model of [...semantic.semantic_models].sort((a, b) =>
    a.name.localeCompare(b.name),
  )) {
    const cols = columnsOf(model.name);
    if (!cols.length) {
      note(
        "model",
        model.name,
        "no columns in catalog.json -- run `dbt docs generate`",
      );
      continue;
    }
    const docs = columnDocs(model.name);
    out.push(...docLines(modelDoc(model.name), ""));
    out.push(
      `source: ${model.name}_binding is duckdb.table('data/${model.node_relation.alias}.parquet') -> {`,
      "  select:",
    );
    cols.forEach((c, i) => {
      out.push(...docLines(docs.get(c.name), "    "));
      const renamed = renames.get(model.name)?.has(c.name);
      if (renamed) {
        out.push(
          `    #(doc) Renamed from \`${c.name}\`: the dbt metric \`${c.name}\` owns that name in the semantic layer.`,
          `    ${rawName(c.name)} is ${c.name}`,
        );
      } else {
        out.push(`    ${c.name}`);
      }
      if (i < cols.length - 1) out.push("");
    });
    out.push("}", "");
  }
  return out.join("\n");
}

// ── Metric translation ─────────────────────────────────────────────────────

/**
 * Resolve a MetricFlow filter template to a Malloy boolean expression.
 * `{{ Dimension('order_id__order_total_dim') }} >= 20` -> `order_total_raw >= 20`
 *
 * The qualified name is `<entity path>__<dimension>`; the dimension's `expr` (when
 * set) is the real column. This resolution is exactly what the OSI export skips,
 * which is why its filtered metrics reference columns that do not exist.
 */
function resolveFilter(model: SemanticModel, template: string): string | null {
  let expr = template.trim();
  const dimRef = /\{\{\s*Dimension\(\s*'([^']+)'\s*\)\s*\}\}/g;
  let unresolved: string | null = null;

  expr = expr.replace(dimRef, (_all, qualified: string) => {
    const parts = String(qualified).split("__");
    const dimName = parts[parts.length - 1];
    const dim = model.dimensions.find((d) => d.name === dimName);
    if (!dim) {
      unresolved = `dimension '${dimName}' not found on semantic model '${model.name}'`;
      return qualified;
    }
    const column = dim.expr ?? dim.name;
    return boundColumn(model.name, column);
  });

  if (unresolved) return null;
  if (/\{\{/.test(expr)) return null; // TimeDimension / Entity / Metric refs
  // MetricFlow writes SQL booleans; Malloy accepts `= true`, but the bare column reads better.
  expr = expr.replace(/\s*=\s*true\b/gi, "");
  return expr.replace(/\s+/g, " ").trim();
}

interface EmittedMeasure {
  model: string;
  name: string;
  doc?: string;
  label?: string;
  expr: string;
}

const measures: EmittedMeasure[] = [];
const metricByName = new Map(semantic.metrics.map((m) => [m.name, m]));

/** Which semantic model a metric's value lives on, following derived/ratio inputs. */
function hostModel(metric: Metric): string | null {
  const agg = metric.type_params.metric_aggregation_params;
  if (agg) return agg.semantic_model;
  const cumulative = metric.type_params.cumulative_type_params as
    | { metric?: MetricInput | null }
    | null
    | undefined;
  const inputs = [
    metric.type_params.numerator,
    metric.type_params.denominator,
    cumulative?.metric ?? null,
    ...metric.type_params.metrics,
  ].filter(Boolean) as MetricInput[];
  for (const input of inputs) {
    const parent = metricByName.get(input.name);
    if (parent) {
      const host = hostModel(parent);
      if (host) return host;
    }
  }
  return null;
}

/** A reference to another metric from `host`, using a join path when needed. */
function metricRef(host: string, metricName: string): string | null {
  const metric = metricByName.get(metricName);
  if (!metric) return null;
  const owner = hostModel(metric);
  if (!owner) return null;
  if (owner === host) return metricName;
  // Cross-model: only a direct join from host to owner is safe to emit.
  const hostModelDef = modelsByName.get(host);
  const joins = (hostModelDef?.entities ?? []).filter(
    (e) => e.type === "foreign",
  );
  const reachable = joins.some((e) => joinTargetOf(e) === owner);
  return reachable ? `${owner}.${metricName}` : null;
}

/** The semantic model whose primary entity matches this foreign entity. */
function joinTargetOf(entity: Entity): string | null {
  for (const model of semantic.semantic_models) {
    if (
      model.entities.some((e) => e.type === "primary" && e.name === entity.name)
    ) {
      return model.name;
    }
  }
  return null;
}

for (const metric of semantic.metrics) {
  const host = hostModel(metric);
  if (!host) {
    note("metric", metric.name, "could not resolve a host semantic model");
    continue;
  }
  const model = modelsByName.get(host)!;
  const agg = metric.type_params.metric_aggregation_params;
  const tp = metric.type_params;

  // Filter, shared by every metric type that supports one.
  let where: string | null = null;
  const filters = metric.filter?.where_filters ?? [];
  if (filters.length) {
    const parts = filters.map((f) =>
      resolveFilter(model, f.where_sql_template),
    );
    if (parts.some((p) => p === null)) {
      note(
        "metric",
        metric.name,
        "filter uses a MetricFlow construct this converter does not resolve " +
          "(TimeDimension, Entity, or a metric reference)",
      );
      continue;
    }
    where = parts.join(" and ");
  }

  let expr: string | null = null;

  if (metric.type === "simple" && agg) {
    const column = tp.expr ?? metric.name;
    const bound = (c: string) => boundColumn(host, c);
    switch (agg.agg) {
      case "sum":
        if (tp.expr === "1") {
          expr = "count()";
        } else if (/^[a-z_][a-z0-9_]*$/i.test(column)) {
          expr = `${bound(column)}.sum()`;
        } else {
          // A SQL expression. The `case when <cond> then <col> else 0 end` shape
          // is a filtered sum, which Malloy states directly.
          const m = column.match(
            /^case\s+when\s+(.+?)\s+then\s+([a-z_][a-z0-9_]*)\s+else\s+0\s+end$/i,
          );
          if (m) {
            const cond = m[1].replace(/\s*=\s*true\b/gi, "").trim();
            expr = `${bound(m[2])}.sum() { where: ${bound(cond)} }`;
          } else {
            note(
              "metric",
              metric.name,
              `agg expression is SQL this converter does not translate: \`${column}\``,
            );
            continue;
          }
        }
        break;
      case "count_distinct":
        expr = `count(${bound(column)})`;
        break;
      case "count":
        expr = "count()";
        break;
      case "average":
        expr = `${bound(column)}.avg()`;
        break;
      case "min":
      case "max":
        expr = `${bound(column)}.${agg.agg}()`;
        break;
      case "median":
      case "percentile":
        note(
          "metric",
          metric.name,
          "no scalar median/percentile aggregate in this Malloy build; " +
            "raw-SQL aggregates do not compile as measures. Deferred, not substituted with avg.",
        );
        continue;
      default:
        note("metric", metric.name, `unhandled dbt agg '${agg.agg}'`);
        continue;
    }
    if (where) {
      // A filtered aggregate already carries a `{ where: }`; merge instead of nesting.
      expr = expr.includes("{ where:")
        ? expr.replace(/\{ where: (.+) \}$/, `{ where: $1 and ${where} }`)
        : `${expr} { where: ${where} }`;
    }
  } else if (metric.type === "ratio") {
    const num = tp.numerator && metricRef(host, tp.numerator.name);
    const den = tp.denominator && metricRef(host, tp.denominator.name);
    if (!num || !den) {
      note(
        "metric",
        metric.name,
        "ratio input is on a semantic model with no direct join from the host",
      );
      continue;
    }
    expr = `${num} / nullif(${den}, 0)`;
    if (where) expr = `${expr} { where: ${where} }`;
  } else if (metric.type === "derived") {
    const offset = tp.metrics.find((m) => m.offset_window || m.offset_to_grain);
    if (offset) {
      const w = offset.offset_window;
      note(
        "metric",
        metric.name,
        `derived metric offsets an input by ${w ? `${w.count} ${w.granularity}` : offset.offset_to_grain} -- ` +
          "a period-over-period comparison is a windowed view in Malloy " +
          "(`calculate:` with `lag()`), not a measure. Emitted as a view instead.",
      );
      continue;
    }
    let body = tp.expr ?? "";
    let ok = true;
    for (const input of tp.metrics) {
      const ref = metricRef(host, input.name);
      if (!ref) {
        ok = false;
        note(
          "metric",
          metric.name,
          `input metric '${input.name}' is not reachable from '${host}' by a direct join`,
        );
        break;
      }
      const token = input.alias ?? input.name;
      body = body.replace(
        new RegExp(
          `\\b${token.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`,
          "g",
        ),
        ref,
      );
    }
    if (!ok) continue;
    // Guard a bare division so a zero denominator yields null, not an error.
    const div = body.match(/^(.+?)\s*\/\s*(.+)$/);
    expr = div ? `${div[1].trim()} / nullif(${div[2].trim()}, 0)` : body;
    if (where) expr = `${expr} { where: ${where} }`;
  } else if (metric.type === "cumulative") {
    note(
      "metric",
      metric.name,
      "a cumulative metric is a running total over an ordered window -- " +
        "a windowed view in Malloy (`calculate:` with a running sum), not a measure",
    );
    continue;
  } else {
    note("metric", metric.name, `unhandled dbt metric type '${metric.type}'`);
    continue;
  }

  if (expr) {
    measures.push({
      model: host,
      name: metric.name,
      doc: metric.description,
      label: metric.label,
      expr,
    });
  }
}

// ── Emit the semantic layer ────────────────────────────────────────────────

/** Order sources so a join target is always defined before the source joining it. */
function topoSortModels(): SemanticModel[] {
  const sorted: SemanticModel[] = [];
  const seen = new Set<string>();
  const visit = (model: SemanticModel, stack: Set<string>) => {
    if (seen.has(model.name) || stack.has(model.name)) return;
    stack.add(model.name);
    for (const entity of model.entities.filter((e) => e.type === "foreign")) {
      const target = joinTargetOf(entity);
      if (target && target !== model.name) {
        const dep = modelsByName.get(target);
        if (dep) visit(dep, stack);
      }
    }
    stack.delete(model.name);
    if (!seen.has(model.name)) {
      seen.add(model.name);
      sorted.push(model);
    }
  };
  for (const model of [...semantic.semantic_models].sort((a, b) =>
    a.name.localeCompare(b.name),
  )) {
    visit(model, new Set());
  }
  return sorted;
}

function emitSemanticLayer(): string {
  const out: string[] = [];
  out.push(
    "// GENERATED by tools/dbt_to_malloy.ts from dbt's semantic manifest -- do not edit by hand.",
    "//",
    "// dbt's semantic models, entities, dimensions, and metrics as a Malloy semantic layer.",
    "// The physical tables are named only in bindings.malloy; this file names no dataset.",
    "//",
    "// What did not convert is listed in ../COVERAGE.md, with the reason for each.",
    "",
    'import "bindings.malloy"',
    "",
  );

  for (const model of topoSortModels()) {
    const primary = model.entities.find((e) => e.type === "primary");
    const foreign = model.entities.filter((e) => e.type === "foreign");
    const mine = measures
      .filter((m) => m.model === model.name)
      .sort((a, b) => a.name.localeCompare(b.name));

    out.push(...docLines(modelDoc(model.name), ""));
    out.push(`source: ${model.name} is ${model.name}_binding extend {`);

    if (primary) {
      const column = boundColumn(model.name, primary.expr ?? primary.name);
      out.push(`  primary_key: ${column}`);
    } else {
      note(
        "model",
        model.name,
        "no primary entity in dbt, so no Malloy primary_key -- it cannot be a join target",
      );
    }

    const joins: string[] = [];
    for (const entity of foreign) {
      const target = joinTargetOf(entity);
      if (!target) {
        note(
          "join",
          `${model.name}.${entity.name}`,
          "foreign entity has no semantic model declaring it as primary",
        );
        continue;
      }
      const localColumn = boundColumn(model.name, entity.expr ?? entity.name);
      joins.push(...docLines(entity.description, "    "));
      joins.push(`    ${target} with ${localColumn}`);
    }
    if (joins.length) {
      out.push("", "  join_one:");
      out.push(...joins);
    }

    // dbt dimensions that rename or compute. A dimension that only aliases its own
    // column (`order_total_dim is order_total`) exists so MetricFlow has a name to
    // filter on; Malloy filters the column directly, so the alias is dropped and the
    // filter resolved through it instead.
    const emittedDims: string[] = [];
    for (const dim of model.dimensions) {
      if (dim.type === "time") continue; // Malloy truncates timestamps natively
      const column = dim.expr ?? dim.name;
      if (dim.expr && /^[a-z_][a-z0-9_]*$/i.test(dim.expr)) {
        note(
          "dimension",
          `${model.name}.${dim.name}`,
          `alias of column \`${dim.expr}\`; MetricFlow needs a named dimension to filter on, Malloy does not`,
        );
        continue;
      }
      if (!dim.expr) continue; // plain passthrough column, already in the binding
      emittedDims.push(...docLines(dim.description, "    "));
      emittedDims.push(`    ${dim.name} is ${boundColumn(model.name, column)}`);
    }
    if (emittedDims.length) {
      out.push("", "  dimension:");
      out.push(...emittedDims);
    }

    if (mine.length) {
      out.push("", "  measure:");
      mine.forEach((m, i) => {
        out.push(...docLines(m.doc, "    "));
        if (m.label && m.label !== m.name) out.push(`    # label="${m.label}"`);
        out.push(`    ${m.name} is ${m.expr}`);
        if (i < mine.length - 1) out.push("");
      });
    }

    // Saved queries whose metrics all live on this model become views.
    const views = emitSavedQueryViews(model);
    if (views.length) out.push("", ...views);

    out.push("}", "");
  }
  return out.join("\n");
}

/** dbt saved queries -> Malloy views, when every metric resolves on this model. */
function emitSavedQueryViews(model: SemanticModel): string[] {
  const out: string[] = [];
  for (const sq of semantic.saved_queries) {
    const metricNames = sq.query_params.metrics;
    const emitted = metricNames.filter((n) =>
      measures.some((m) => m.name === n && m.model === model.name),
    );
    if (!emitted.length) continue;
    if (emitted.length !== metricNames.length) {
      const missing = metricNames.filter((n) => !emitted.includes(n));
      note(
        "saved_query",
        sq.name,
        `metrics not available on '${model.name}': ${missing.join(", ")}`,
      );
    }

    const groupBys: string[] = [];
    let skip = false;
    for (const g of sq.query_params.group_by) {
      const time = g.match(/TimeDimension\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)/);
      const entity = g.match(/Entity\(\s*'([^']+)'\s*\)/);
      const dim = g.match(/Dimension\(\s*'([^']+)'\s*\)/);
      if (time) {
        const grain = time[2];
        const column = model.defaults?.agg_time_dimension;
        if (!column) {
          note(
            "saved_query",
            sq.name,
            `groups by ${time[1]} but '${model.name}' declares no agg_time_dimension`,
          );
          skip = true;
          break;
        }
        groupBys.push(
          `    ${column}_${grain} is ${boundColumn(model.name, column)}.${grain}`,
        );
      } else if (entity) {
        const ent = model.entities.find((e) => e.name === entity[1]);
        if (!ent) {
          note(
            "saved_query",
            sq.name,
            `entity '${entity[1]}' not on '${model.name}'`,
          );
          skip = true;
          break;
        }
        groupBys.push(`    ${boundColumn(model.name, ent.expr ?? ent.name)}`);
      } else if (dim) {
        const parts = dim[1].split("__");
        const d = model.dimensions.find(
          (x) => x.name === parts[parts.length - 1],
        );
        if (!d) {
          note(
            "saved_query",
            sq.name,
            `dimension '${dim[1]}' not on '${model.name}'`,
          );
          skip = true;
          break;
        }
        groupBys.push(`    ${boundColumn(model.name, d.expr ?? d.name)}`);
      } else {
        note("saved_query", sq.name, `unparsed group_by '${g}'`);
        skip = true;
        break;
      }
    }
    if (skip) continue;

    out.push(...docLines(sq.description, "  "));
    out.push(`  view: ${sq.name} is {`);
    if (groupBys.length) {
      out.push("    group_by:");
      out.push(...groupBys.map((g) => `  ${g}`));
    }
    out.push("    aggregate:");
    out.push(...emitted.map((m) => `      ${m}`));
    if (sq.query_params.limit) out.push(`    limit: ${sq.query_params.limit}`);
    out.push("  }");
  }
  return out.join("\n").length ? out : [];
}

// ── Write or check ─────────────────────────────────────────────────────────

const outDir = path.join(import.meta.dir, "..", "semantic-layer");
const files: Record<string, string> = {
  "bindings.malloy": emitBindings(),
  "jaffle_shop.malloy": emitSemanticLayer(),
};

let drift = false;
for (const [name, content] of Object.entries(files)) {
  const dest = path.join(outDir, name);
  const existing = existsSync(dest) ? readFileSync(dest, "utf8") : null;
  if (checkOnly) {
    if (existing !== content) {
      drift = true;
      console.error(
        `DRIFT ${name}: the committed file does not match what dbt's artifacts generate.\n` +
          "Fix: re-run without --check and commit the result.",
      );
    } else {
      console.log(`ok    ${name}`);
    }
  } else {
    writeFileSync(dest, content);
    console.log(
      `wrote ${path.relative(process.cwd(), dest)} (${content.split("\n").length} lines)`,
    );
  }
}

console.log(
  `\n${measures.length} of ${semantic.metrics.length} dbt metrics became Malloy measures.`,
);
if (gaps.length) {
  console.log(`\n${gaps.length} dbt construct(s) did not convert:`);
  for (const g of gaps)
    console.log(`  [${g.kind}] ${g.name}\n      ${g.reason}`);
}
if (checkOnly && drift) process.exit(1);
