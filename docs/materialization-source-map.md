# The source map (materialization v2, phase 3)

The source map is one read-only description of a model's sources, built from
the compiled `ModelDef` through the governance IR's source graph
(`governance/ir/source_graph.ts`). Every materialization reader that today
walks the IR on its own reads the map instead: the build planner, the serve
shape, the path report, the serve verdicts, and in phase 3 the build-file
generator, the re-root rewriter and the router's catalog.

It answers three questions about any source:

1. **What is it built from?** Its chain of segments down to a table or SQL
   source, through `extend`, renames, query-shaped segments and composites.
2. **What does it add on each segment?** Fields, joins, filters, views, and
   the annotations that apply there.
3. **Which stored table can stand for which segment?** The tables the build
   plan addresses, linked to the segments they materialize.

It records structure. It computes no content address (that is the build
plan's, phase 3 PR 2), writes no Malloy text (the rewriter's and generator's,
PR 3 and PR 2), and edits nothing in the IR.

## What this pull request builds, and what later ones add

This note describes the map phase 3 needs. Phase 3's first pull request builds
the part today's readers use; the rest lands with the pull request that first
reads it.

| Part | Status |
| --- | --- |
| Nodes by `sourceID`, a name index per model, kind, base edge (`extends`, rename, query base), a composite's members | Built (PR 1) |
| The chain walk (path report), reachability (unreachable verdict), the build-side reach (chained builds) | Built (PR 1), replacing three hand-written walkers |
| The keying rule for storage eligibility, the storage bind filter and the verdicts' bind check | Built (PR 1); the ratchet spec lists the name-keyed uses left to migrate |
| Pipeline references on each node | Read on demand by the walks (`pipelineReferences`); stored on the node with PR 2's generator |
| Joins (relationship, target, added or inherited, cross-connection mark) | PR 3 (rewriter) and PR 4 (joins) |
| Annotations per segment, origin connection | PR 2 (generator) |
| Links to stored tables by `(sourceEntityId, destination)`, and a package-wide union by `sourceID` | PR 3, with the router's catalog |
| Hop readers moved into `governance/ir/source_graph.ts` | The base edge (`sourceBaseEdge`, `nextSourceInChain`, with the inferred edge of a table-backed extension) moved with extension tables; the pipeline reader lives in `source_map.ts` until governance needs it. The map still walks pipelines itself (`pipelineReferences`, `reachableSourceIDs`) and holds no text ranges; moving those walks into `source_graph.ts`, and one reader of author text, are phase 5's simplification pass (decided 2026-11-12) |

## The keying rule

Three identities, each used for one thing:

| Identity | Key | Used for |
| --- | --- | --- |
| A declaration | `sourceID` (`name@<model URL>`) | Every map, gate and lookup about a source |
| A stored table | `(sourceEntityId, destination)`; destination is null for colocated | Bindings, the catalog, verdicts and the path report |
| A name | the name in one model's namespace | Display and logs only |

A name is never a map key or a gate. Names are neither package-unique (two
models may declare `orders`) nor stable across import (a re-export can rename),
and keying by name is the most frequent bug class in phases 0–2 (#6644, #7078,
and findings in the #1606, #1610 and #1611 reviews). A test fails if a
materialization map is keyed by a source's `.name`.

`sourceID` survives import and re-export: a source declared in `model.malloy`
and re-exported by `index.malloy` keeps `orders@…/model.malloy` in both. So the
map gives a re-exported source the same node, the same chain and the same
tables in every model that can see it. That is what lets a rollup over a
re-exported source route identically through `index.malloy` and its declaring
file (the review's bug 1 and bug 8 are this class).

## What a node records

One node per declaration, keyed by `sourceID`:

- **Identity:** `sourceID`; the declaring model (the URL in the `sourceID`);
  and, per model that can see it, the name it has there. Names are an index
  onto nodes (`name → sourceID` per model namespace), never the other way.
- **Kind:** table, SQL, `extend`, rename, query-shaped (`x is y -> { … }`) or
  composite (`compose(a, b)`, with its members in order; a composite has no
  single base).
- **Base edge:** the one source it is built from, with the hop that reaches
  it: `referenceID` (a rename, read first: Malloy copies the referent's own
  `extends` onto a rename, so a rename's `extends` names the grandparent),
  `extends` (an `extend`), or `query.structRef` (a query-shaped segment,
  including a query over a composite, whose base is the composite). Malloy
  records `extends` only on an extension of a query or SQL source; for an
  extension of a table-backed source the base is read from the compiled
  structure, never from text: the annotations object it shares with its base
  (or its own object's `inherits`), the same table, its base's fields copied
  unchanged or renamed, its base's filters opening its own, and a declaration
  order the model shows. Two candidates neither of which holds the other, or
  one of unknown order, give no edge. A table, SQL source or composite has none; the member Malloy
  resolves for a query over a composite (`query.compositeResolvedSourceDef`)
  is read by reachability, not recorded as a base. When a hop does not
  resolve in the model, the chain falls through to the next hop, as the walks
  before the map did. A base the registry
  cannot resolve is recorded by its id as unresolved, never dropped.
- **Pipeline references:** the other sources a query-shaped segment's pipeline
  reads, and whether the pipeline was too deep to read (which counts as
  reaching an unidentified source, never as reaching nothing).
- **Joins:** name, relationship (one, many, cross), target (a `sourceID`, or an
  embedded definition as a child node), whether this node added it or
  inherited it from its base, and whether the target is on a different origin
  connection.
- **Annotations:** which annotations apply on this segment, read through the
  annotation reader (`annotation_reader.ts`): `#@ persist` with `storage=`,
  `#@ preaggregate`, `#(authorize)`, `#(access_filter)`, `#(carries)`,
  `#(classification)`. Each records whether the node wrote it or inherited it.
  `#@ persist` is inherited along `extend` and renames, never across a
  query-shaped segment, so a persisted source's extension shares its table.
- **Origin connection.**

A **segment** is the run of nodes between two query-shaped boundaries:
an extend chain over one base. Query-shaped segments are where the rewriter
can re-root, and where a stored table stands for everything below it.

## Stored tables

The map links nodes to the build plan's tables, keyed by
`(sourceEntityId, destination)`:

- a persisted node's own table, from the plan entry for its `sourceID`;
- a rollup's table, recorded under the rollup's own synthesized `sourceID` and
  its base node's `sourceID`, not under a generated name;
- for each node, the tables up its chain that can stand for one of its
  segments: the candidates the router and rewriter consider.

Several nodes share one table when they share an address (a base and its
extension, or two sources with identical SQL). The map keeps that many-to-one
link; it does not decide which node "declared" the table, since `#@ persist` is
inherited and nothing on a definition says so.

## How it reads the IR

Read-only, through `source_graph.ts`'s hop readers
(`resolveDeclaredSource`, `resolveExtendedSource`, `resolveQuerySourceBase`,
`resolveCompositeResolvedBase`). The map adds the readers it needs that don't
exist yet (renames, pipeline references, joins) to `source_graph.ts` as pure
functions over compiler-set IR, so governance and materialization keep one
reader of the chain. No IR edits, no compiler internals beyond what governance
already reads (see the note on Malloy text over compiler internals).

**The rule: the source map never walks segments itself.** Structural reading of a
source's segments (its extends chain, renames, query bases, composites, joins and
what a struct reaches) belongs in `governance/ir/source_graph.ts`, which every part
of Publisher that needs segments reads. The source map is materialization's view
over that structure: it adds only what materialization needs (keying by `sourceID`,
stored tables, addresses, which annotations apply where). A walk the map needs and
`source_graph.ts` lacks is added there first, as a pure function, and the map calls it.

Built once per model per load (and per rebind where a binding generation
matters), memoized, and unioned per package by `sourceID`, so a source several
models import is one node.

## Cross-file chains, imports and re-exports

- **Imports:** an imported source's node is its declaring model's, found
  through `ModelDef.sourceRegistry` or the inline definition on
  `query.structRef`. Its chain continues into the declaring file.
- **Re-exports through `index.malloy`:** the re-exported name maps to the
  declaring `sourceID`. Curation decides visibility (what the package
  publishes); it never changes identity.
- **Aliases** (`source: misc is orders extend {}`) are their own nodes with
  their own `sourceID`, whose base edge is the aliased source.

## Query-shaped segments

`source: daily is orders -> { group_by: …; aggregate: … }` is a node with base
`orders` and a pipeline. A persisted query-shaped source is the unit the
build-file generator emits and the rewriter re-roots at. Its pipeline
references are recorded so the build side knows which other sources (and
which of their tables) a build reads.

## Cross-connection joins

A join whose target is on a different origin connection is marked. Phase 0's
probe (Evidence 7) showed the consequences the map has to carry:

- the author's file with such a join does not compile live, so the generator
  plans builds from the file with those joins removed, and the map for build
  planning is built over that compile;
- the build view (the author's text with those joins left out) is the compile
  of record for discovery and gates, and the served copy is what the rewriter
  and router read (decided 2026-11-12, #1643);
- a query that reaches a side not yet built is refused from build state (the
  join is left out of the served copy while that side is unbuilt, #1643), so
  the map reports which joins depend on which tables.

## Who consumes it

- **Build side (PR 2):** the generator walks a persisted node's segments to
  emit its query-shaped build source. A chained build's "which persisted
  sources and which raw leaves does this reach" is a walk over the map that
  stops at persisted nodes (replacing `reachedPersistedSources`).
- **Serving core (PR 3):** the rewriter takes a query's source node, walks its
  chain to the nearest table that can stand for each segment, and re-roots the
  text onto it.
- **Router and catalog:** the per-table catalog the phases 0–2 review
  recommends is the map's table links joined with the plan and the bindings,
  computed at bind time. Verdicts, the path report's `considered` and
  `notConsidered` (siblings and below), reachability for the unreachable
  warning, and the router all read it.

## What this pull request changes

No routing or serving behaviour changes. R4, hammer, the path report and
verdict tests stay green.

- The map module, on `source_graph.ts`.
- The three hand-written chain walkers are replaced by map walks:
  - `reachedPersistedSources` (`materialization_serve_transform.ts`), the
    build-side reach that stops at persisted sources;
  - `sourceChainOf` (`query_path.ts`), the chain the path report and router
    candidates read;
  - `sourceIDsReachedFrom` (`serve_verdict.ts`), reachability for the
    unreachable verdict and warning. Today this scans every nested object for
    any `sourceID`, `referenceID` or `extends` string, conservatively. The map
    walk follows declared edges; a differential test over the bundled
    packages, the test fixtures and the hammer models asserts it reaches at
    least everything the scan did, so no package gains a false `unreachable`.
- The keying rule where the map is consumed: storage eligibility and the bind
  filter move from source names to `sourceID`s, and rollups are recorded by
  their synthesized `sourceID` instead of passing on their generated names.

## Where phase 0's remaining checks are verified

| Check | Verified in |
| --- | --- |
| Rolling-window reads at the window's edges with a pinned clock | PR 5 (rolling windows), over the map's segment that carries the window filter |
| The text reader for a query that starts from a view | PR 3 (rewriter), which reads a view-start query through the map's view on the node |
| The served copy's answers equal the hand-written `->` form's on the #7098 fixture | PR 6 (v1 cutover), the public gate fixture |

## Open points

1. **One listing per rollup.** Today a rollup appears once per model file that
   declares or imports its base (`model.malloy.preagg.malloy`,
   `index.malloy.preagg.malloy`), all sharing one table. The map dedupes by
   `sourceID`, so the build plan could list each rollup once. That changes
   which keys `GET …/build-plan` returns, so it belongs with PR 2's build-side
   changes rather than here.
2. **Pipeline references and inheritance.** `#@ persist` is not inherited
   across a query-shaped segment; a pipeline reference reads another source's
   table but never inherits its annotations. The map records the reference
   and nothing else.
