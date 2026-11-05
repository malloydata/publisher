// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A published source whose `#(authorize)` or `#(access_filter)` reads a
 * given that is not declared `#(secure)` REFUSES the package load.
 *
 * A plain given is whatever the request supplies, so a gate keyed on one is a
 * filter the caller can set, with a policy-shaped name. A `#(secure)` given is
 * the host's to set: the trusted tier in front of Publisher resolves it from
 * the caller's verified identity and replaces anything the request carried
 * (docs/materialization.md, "Where the per-caller value comes from"). Only a
 * gate keyed on secure givens is a control, so a package that writes one on a
 * plain given does not load, the way a `#(carries)` leak or a join bypass does:
 * serving it would serve a surface that claims a boundary it does not have.
 * The message names the source and the givens to mark, once per source.
 */

import { isSourceDef, type ModelDef, type SourceDef } from "@malloydata/malloy";
import { referencedGivenNames } from "./authorize";
import { ACCESS_FILTER_ROUTE, AUTHORIZE_ROUTE } from "./authorize_routes";
import { gateExprsForRoute } from "./gate_registry_walk";
import type { Model } from "./model";

const isSentinel = (expr: string) =>
   ["false", "true"].includes(expr.trim().toLowerCase());

/**
 * The package model a gate note's document URL names: the longest model path
 * the URL ends with, since the package directory is not known here.
 */
function modelWrittenAt(
   url: string,
   models: ReadonlyMap<string, Model>,
): Model | undefined {
   let path: string;
   try {
      path = decodeURIComponent(new URL(url).pathname);
   } catch {
      return undefined;
   }
   let best: string | undefined;
   for (const modelPath of models.keys()) {
      if (
         path.endsWith(`/${modelPath}`) &&
         (best === undefined || modelPath.length > best.length)
      ) {
         best = modelPath;
      }
   }
   return best === undefined ? undefined : models.get(best);
}

/**
 * The given names a source's gate on one route reads that are not `#(secure)`
 * in the model the gate note is written in, which is where its `$GIVEN`
 * binds, not the model the source is published through. A gate whose model
 * cannot be identified judges nothing secure, so the load refuses.
 */
function insecureGivenNames(
   source: SourceDef | undefined,
   modelDef: ModelDef | undefined,
   route: string,
   models: ReadonlyMap<string, Model>,
): string[] {
   const declaredAt = new Set<string>();
   const groups = source
      ? gateExprsForRoute(source, modelDef, route, declaredAt)
      : [];
   const read = new Set<string>();
   for (const expr of groups.flat()) {
      if (isSentinel(expr)) continue;
      for (const name of referencedGivenNames(expr)) read.add(name);
   }
   if (read.size === 0) return [];
   const secureIn = Array.from(declaredAt, (url) => {
      const declaring = modelWrittenAt(url, models);
      return new Set(
         (declaring?.getGivens() ?? [])
            .filter((g) => g.secure === true && g.name)
            .map((g) => g.name as string),
      );
   });
   return Array.from(read)
      .filter(
         (name) =>
            secureIn.length === 0 ||
            !secureIn.every((names) => names.has(name)),
      )
      .sort();
}

/** One rejection message per published source gated on a plain given; empty when none. */
export function insecureGateGivens(
   models: ReadonlyMap<string, Model>,
   isListed: (modelPath: string) => boolean,
): string[] {
   const messages: string[] = [];
   for (const [modelPath, model] of models) {
      if (!isListed(modelPath)) continue;
      let apiSources: ReturnType<Model["getSources"]>;
      try {
         apiSources = model.getSources();
      } catch {
         continue;
      }
      const modelDef = model.getModelDef();
      for (const source of apiSources ?? []) {
         const contents = source.name
            ? modelDef?.contents[source.name]
            : undefined;
         const struct =
            contents && isSourceDef(contents) ? contents : undefined;
         const insecure = Array.from(
            new Set(
               [AUTHORIZE_ROUTE, ACCESS_FILTER_ROUTE].flatMap((route) =>
                  insecureGivenNames(struct, modelDef, route, models),
               ),
            ),
         ).sort();
         if (insecure.length === 0) continue;
         messages.push(
            `source ${source.name} (${modelPath}) is gated on $${insecure.join(", $")}, ` +
               `which ${insecure.length === 1 ? "is" : "are"} not declared ` +
               `#(secure), so a caller can set ${insecure.length === 1 ? "it" : "them"} ` +
               `and the gate is a filter, not a control. Write "#(secure)" on ` +
               `the given: line so the trusted tier in front of Publisher sets ` +
               `it from the caller's verified identity (docs/authorize.md, ` +
               `Security model).`,
         );
      }
   }
   return messages;
}
