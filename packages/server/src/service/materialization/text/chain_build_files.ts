// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The text half of a chained `storage=` build: the downstream's declaration,
 * the files it reaches with every declaration off its path blanked, and the
 * persistence flag its entry file needs. The served copy re-roots the stored
 * upstreams in these files; nothing here compiles.
 *
 * Pure.
 */

import type { authorModelLiftContext } from "./author_model";
import { keepDeclarations } from "./declarations_kept";
import { reachPersisted } from "./source_map";

type AuthorModelLift = ReturnType<typeof authorModelLiftContext>;

const PERSISTENCE_FLAG = /^\s*##!\s*experimental[^\n]*persistence/m;

/** A position a re-rooted or kept declaration starts at. */
interface Position {
   line: number;
   character: number;
}

/**
 * The files a chained build of `downstreamSourceID` compiles from, before the
 * stored upstreams are re-rooted in them: the downstream's file and every file
 * it reaches by import, each with only the declarations on the downstream's
 * path (and the re-rooted upstreams) kept. Declines, with why, when the
 * downstream's declaration cannot be found, it reaches past its stored
 * upstreams, or a file it reaches cannot be read.
 */
export function chainBuildFileSet(args: {
   lift: AuthorModelLift;
   downstreamSourceID: string;
   bindings: readonly { sourceName: string; sourceIDs?: readonly string[] }[];
   /** Each upstream the served copy re-roots, where it is declared. */
   reroots: readonly { sourceID: string; url?: string; range?: unknown }[];
   /** The files a file imports, as the compile resolved them. */
   importsOf(url: string): string[];
}):
   | { declined: string }
   | { entry: string; downstreamName: string; files: Map<string, string> } {
   const { lift } = args;
   const why = (declined: string) => ({ declined });
   const downstreamName = lift.sourceNameById.get(args.downstreamSourceID);
   const location =
      downstreamName === undefined
         ? undefined
         : lift.contents[downstreamName]?.location;
   const declared =
      location === undefined ? undefined : lift.liftText(location);
   const name =
      declared === undefined
         ? undefined
         : /^\s*(`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)/.exec(declared)?.[1];
   if (!downstreamName || !location?.url || !location.range || !name) {
      return why("the downstream's declaration could not be found");
   }
   // Everything the downstream reads, up to its stored upstreams: the
   // re-rooted ones, and those that read one by name (a rename) and stay
   // as written.
   const stored = new Set(args.reroots.map((r) => r.sourceID));
   const storedNames = new Set<string>();
   for (const b of args.bindings) {
      if (b.sourceIDs) for (const id of b.sourceIDs) stored.add(id);
      else storedNames.add(b.sourceName);
   }
   const reached = reachPersisted(
      lift,
      downstreamName,
      (n, id) => stored.has(id) || storedNames.has(n),
   );
   if (reached.raw) {
      return why(
         `it reads ${reached.rawLeaves.join(", ")}, which no re-rooted upstream stands in for`,
      );
   }
   const keep = new Map<string, Position[]>();
   const keepAt = (url: string | undefined, range: unknown) => {
      const start = (range as { start?: Position } | undefined)?.start;
      if (!url || !start) return;
      keep.set(url, [...(keep.get(url) ?? []), start]);
   };
   keepAt(location.url, location.range);
   for (const visited of reached.visited) {
      const at = lift.contents[visited]?.location;
      keepAt(at?.url, at?.range);
   }
   for (const r of args.reroots) keepAt(r.url, r.range);
   // Every file the downstream's file reaches by import, blanked: the
   // imports the compile resolved, never text that only looks like one.
   const files = new Map<string, string>();
   const pending = [location.url];
   while (pending.length > 0) {
      const url = pending.pop()!;
      if (files.has(url)) continue;
      const text = lift.fileText(url);
      const kept =
         text === undefined
            ? undefined
            : keepDeclarations(text, keep.get(url) ?? []);
      if (text === undefined || kept === undefined) {
         return why(`a file it reaches could not be read (${url})`);
      }
      files.set(url, kept);
      pending.push(...args.importsOf(url));
   }
   return {
      entry: location.url,
      downstreamName: name.startsWith("`") ? name.slice(1, -1) : name,
      files,
   };
}

/**
 * `files` with `##! experimental.persistence` prepended to the entry file
 * when it lacks one, and each re-rooted span in it shifted past the flag.
 */
export function withPersistenceFlag(
   files: Map<string, string>,
   entry: string,
   spans: { url: string; start: number; end: number }[],
): void {
   const text = files.get(entry)!;
   if (PERSISTENCE_FLAG.test(text)) return;
   const flag = "##! experimental.persistence\n";
   files.set(entry, `${flag}${text}`);
   for (const span of spans) {
      if (span.url !== entry) continue;
      span.start += flag.length;
      span.end += flag.length;
   }
}
