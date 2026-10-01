// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { EmbeddableEntity } from "../mcp/tools/embedding_index";
import type { KeyphraseField } from "./prompts/keyphrase";
import type { EgressClasses } from "./retrieval_config";

/**
 * The one place an entity becomes text bound for an LLM provider.
 *
 * Everything an index-time prompt says about an entity is built here, from an
 * explicit list of data classes the operator has switched on. That is the whole
 * point of the module: what leaves the machine is decided by reading one
 * function against one config, not by auditing every prompt builder.
 *
 * Two rules hold whatever the classes say:
 *
 * 1. A description is always the entity's `#(doc)` text (`embedDoc`), never its
 *    `doc`, which falls back to raw annotation lines and so can carry
 *    `#(access_filter)` and `#(authorize)` predicates. There is no class that
 *    turns predicates on, and no field on the shapes below that could hold one.
 * 2. Field `code` has every annotation line stripped before it goes out (see
 *    {@link sanitizeCode}), because a view's definition is sliced from the
 *    model file and can carry the annotations written above its body.
 *
 * A class that is off does not drop the field silently: the prompt says "(not
 * provided)", so the model knows the input was withheld rather than empty and
 * does not invent it.
 */

const NOT_PROVIDED = "(not provided)";

/** What enrichment needs to know about an entity beyond what embedding does. */
export interface EnrichableEntity extends EmbeddableEntity {
   dataType?: string;
   code?: string;
   joinPath?: string;
   relationship?: string;
}

/** The kinds whose fields get an LLM keyphrase. A source has a summary instead. */
export const KEYPHRASE_KINDS: ReadonlySet<string> = new Set([
   "dimension",
   "measure",
   "view",
   "query",
]);

/**
 * Strip Malloy annotation lines from code. An annotation is a line whose first
 * non-blank character is `#`, and the only place predicates can live, so
 * removing whole such lines removes them without parsing Malloy.
 */
export function sanitizeCode(code: string, maxChars: number): string {
   const kept = code
      .split("\n")
      .filter((line) => !/^\s*#/.test(line))
      .join("\n")
      .trim();
   return kept.length > maxChars ? `${kept.slice(0, maxChars)}…` : kept;
}

/** A stable label for the classes an enrichment call used, for the cache. */
export function egressSignature(classes: EgressClasses): string {
   return (["names", "docs", "schemaContext", "code"] as const)
      .filter((c) => classes[c])
      .join("+");
}

const words = (s: string): number =>
   s.trim() ? s.trim().split(/\s+/).length : 0;

/**
 * Whether a field gets an LLM keyphrase.
 *
 * `when-sparse` covers the two cases where the name and doc facets are weak
 * handles: no doc at all (the model infers a phrase from the name, type and
 * code), and a doc long enough that its meaning is diluted across chunks (the
 * model condenses it). A short doc is already its own best keyphrase.
 */
export function needsKeyphrase(
   entity: EnrichableEntity,
   mode: "when-sparse" | "always" | "never",
   wordThreshold: number,
   viewWordThreshold: number,
): boolean {
   if (!KEYPHRASE_KINDS.has(entity.kind) || mode === "never") return false;
   if (mode === "always") return true;
   const n = words(entity.embedDoc);
   const limit =
      entity.kind === "view" || entity.kind === "query"
         ? viewWordThreshold
         : wordThreshold;
   return n === 0 || n > limit;
}

/** Sibling fields of one source, one per line, as the keyphrase prompt shows them. */
export function schemaLines(
   siblings: readonly EnrichableEntity[],
   self: EnrichableEntity,
   maxLines = 40,
): string {
   const lines = siblings
      .filter(
         (s) =>
            s.source === self.source &&
            s.kind !== "source" &&
            s.kind !== "join" &&
            !s.joinPath &&
            s.name !== self.name,
      )
      .slice(0, maxLines)
      .map(
         (s) =>
            `- ${s.name} (${s.kind}${s.dataType ? ` / ${s.dataType}` : ""})`,
      );
   return lines.length > 0 ? lines.join("\n") : "(none)";
}

/** The keyphrase prompt's inputs for one field, under the given classes. */
export function keyphraseField(
   entity: EnrichableEntity,
   siblings: readonly EnrichableEntity[],
   classes: EgressClasses,
   maxCodeChars: number,
): KeyphraseField {
   return {
      source: entity.source ?? entity.name,
      name: entity.name,
      type: entity.kind,
      dataType: entity.dataType,
      description: classes.docs ? entity.embedDoc : "",
      schema: classes.schemaContext
         ? schemaLines(siblings, entity)
         : NOT_PROVIDED,
      code: classes.code
         ? entity.code
            ? sanitizeCode(entity.code, maxCodeChars)
            : "No code, this is a raw database column"
         : NOT_PROVIDED,
      modelPath: entity.modelPath,
   };
}

/** Cap on the serialized source handed to the summary prompt. */
export const MAX_SUMMARY_INPUT_CHARS = 12_000;

/**
 * The summary prompt's inputs for one source: its documentation, and a
 * serialization of its own fields and, when schema context is allowed, its
 * joins and the fields reachable through them.
 */
export function summaryInput(
   source: EnrichableEntity,
   members: readonly EnrichableEntity[],
   classes: EgressClasses,
): { sourceName: string; sourceDocs: string; entities: string } {
   const own = members.filter(
      (m) => m.source === source.name && !m.joinPath && m.kind !== "source",
   );
   const line = (m: EnrichableEntity) => {
      const type = `${m.kind}${m.dataType ? ` / ${m.dataType}` : ""}`;
      const doc = classes.docs && m.embedDoc ? `: ${m.embedDoc}` : "";
      return `- ${m.name} (${type})${doc}`;
   };
   const out: string[] = [`source ${source.name}`];
   for (const m of own.filter((m) => m.kind !== "join")) out.push(line(m));
   if (classes.schemaContext) {
      const joins = own.filter((m) => m.kind === "join");
      if (joins.length > 0) {
         out.push("", "declared joins:");
         for (const j of joins) out.push(line(j));
      }
      const reachable = members.filter(
         (m) => m.source === source.name && m.joinPath && m.kind !== "join",
      );
      if (reachable.length > 0) {
         out.push("", "fields reachable through joins:");
         for (const m of reachable) out.push(line(m));
      }
   }
   let text = out.join("\n");
   if (text.length > MAX_SUMMARY_INPUT_CHARS) {
      text = `${text.slice(0, MAX_SUMMARY_INPUT_CHARS)}\n… (truncated)`;
   }
   return {
      sourceName: source.name,
      sourceDocs: classes.docs ? source.embedDoc : "",
      entities: text,
   };
}
