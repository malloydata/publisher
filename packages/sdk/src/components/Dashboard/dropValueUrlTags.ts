// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { parseAnnotation, type Tag } from "@malloydata/malloy-tag";

/**
 * A document held as text is written by whoever the viewer opened it from, so
 * its results are drawn without the tags that turn a cell value into a URL or
 * markup: `# image` (an `<img src=value>`), `# link` (an `<a href>`), and a
 * `# label` with HTML in it. The renderer has no option for this, so the
 * annotations are rewritten in the result before it is drawn.
 *
 * This also drops them from a model's own fields, which is the price of not
 * having to know where a field came from: a field the caller could redefine
 * (`except:` then a new `dimension:` under the same name) keeps the model's
 * tag. A host that wants model images in documents held as text has to ask for
 * them; the default is the safe one.
 */

const HTML_START = /<[A-Za-z!/]/;

/** Remove the offending properties from `tag`, in place, at any depth; whether any was removed. */
function strip(tag: Tag): boolean {
   let changed = false;
   for (const [name, child] of Array.from(tag.entries())) {
      if (child.deleted) continue;
      if (name === "image" || name === "link") {
         tag.delete(name);
         changed = true;
         continue;
      }
      if (name === "label") {
         let label: string | undefined;
         try {
            label = tag.text("label");
         } catch {
            label = undefined;
         }
         if (label !== undefined && HTML_START.test(label)) {
            tag.delete("label");
            changed = true;
            continue;
         }
      }
      const elements = Array.isArray(child.eq) ? child.eq : [];
      for (const nested of [child, ...elements]) {
         if (strip(nested)) changed = true;
      }
   }
   return changed;
}

/** One annotation line with the offending tags removed, or the line as it was. */
function cleanAnnotation(value: string): string {
   const text = value.trim();
   // A route other than the plain MOTLY one (`#(docs)`, `#"`, `#!`) and a `##` note are never drawn as tags.
   if (!/^#(?:\||[ \t\r\n]|$)/.test(text)) return value;
   // The renderer hydrates `@env.` and may read a line this parser rejects, so neither can be inspected: the line goes.
   if (text.includes("@env.")) return "";
   let parsed: ReturnType<typeof parseAnnotation>;
   try {
      parsed = parseAnnotation(text);
   } catch {
      return "";
   }
   if (parsed.log.length > 0) return "";
   // `toString()` already carries the `# ` prefix.
   return strip(parsed.tag) ? `${parsed.tag.toString()}\n` : value;
}

function clean(node: unknown): void {
   if (Array.isArray(node)) {
      for (const item of node) clean(item);
      return;
   }
   if (node === null || typeof node !== "object") return;
   const record = node as Record<string, unknown>;
   for (const [key, value] of Object.entries(record)) {
      if (key === "annotations" && Array.isArray(value)) {
         for (const annotation of value) {
            const holder = annotation as { value?: unknown };
            if (typeof holder?.value === "string") {
               holder.value = cleanAnnotation(holder.value);
            }
         }
      } else {
         clean(value);
      }
   }
}

/**
 * The result with every `# image`, `# link` and HTML-bearing `# label` removed
 * from its annotations; the input string untouched when there is none or it is
 * not JSON, for the renderer to report.
 */
export function dropValueUrlTags(result: string): string {
   let parsed: unknown;
   try {
      parsed = JSON.parse(result);
   } catch {
      return result;
   }
   const before = JSON.stringify(parsed);
   clean(parsed);
   const after = JSON.stringify(parsed);
   return after === before ? result : after;
}
