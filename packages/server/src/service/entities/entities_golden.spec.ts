// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What `GET …/entities` decides about each source, for every curated package
 * the entities specs compile: the query-surface package (every report shape,
 * a derivation chain, shadowed and aliased imports), the same package under
 * `queryableSources: "all"`, a package whose `explores` lists reports beside
 * ordinary files, a re-rooting topology, and a package of gate shapes.
 * `to_api.snapshot.spec.ts` pins the whole response for the uncurated rich
 * package.
 *
 * A golden, and deliberately a PROJECTION of the response rather than all of
 * it: per model, whether it compiled, and per source entry its name, whether
 * it is join-only, whether it is advertised as queryable, the served source
 * it names (`servedFrom`), and its gates as reported (route, expression,
 * given names, access-filter terms). Those are what the rules behind
 * queryability and gate reading decide: the query route
 * (`Model.admitsSourceByName`, `Package.entitiesQueryRoute`,
 * `advertisedQueryability`) and the authorize grammar
 * (`parseGateAnnotation`, `accessFilterTerms`). The fields, schemas, docs and
 * givens those packages also carry are the field extraction's, pinned by its
 * own specs; snapshotting them here made a ~22k-line file in which a change
 * to one of these decisions was hard to find. A change to any projected field
 * shows up here as a small snapshot diff.
 *
 * The projection was checked equal to the full response's same fields when it
 * replaced the whole-response golden.
 */

import { expect, test } from "bun:test";
import { GOLDEN_PACKAGES } from "./__fixtures__/golden_packages";
import { compileFixture } from "./test_support";
import { toApiEntities } from "./to_api";

type ApiModelEntities = ReturnType<typeof toApiEntities>[number];

/** The decisions this golden pins, per model and source (see the header). */
function projectDecisions(models: readonly ApiModelEntities[]) {
   return models.map((model) =>
      model.error !== undefined
         ? { modelPath: model.modelPath, error: true }
         : {
              modelPath: model.modelPath,
              sources: (model.sources ?? []).map((source) => ({
                 name: source.name,
                 ...(source.modelPath !== model.modelPath && {
                    modelPath: source.modelPath,
                 }),
                 ...(source.joinOnly && { joinOnly: true }),
                 queryable: source.queryable,
                 ...(source.servedFrom && {
                    servedFrom: `${source.servedFrom.modelPath}::${source.servedFrom.sourceName}`,
                 }),
                 ...(source.authorize && { authorize: source.authorize }),
              })),
           },
   );
}

for (const [name, pkg] of Object.entries(GOLDEN_PACKAGES)) {
   test(`the ${name} package's queryability and gates`, async () => {
      const results = await compileFixture(pkg.files, pkg.manifest);
      expect(projectDecisions(toApiEntities(results))).toMatchSnapshot();
   }, 120_000);
}
