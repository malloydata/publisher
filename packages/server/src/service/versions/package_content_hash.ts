// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as crypto from "crypto";
import * as fs from "fs";
import * as path from "path";

/** Directories that are never part of a package's content. */
const SKIPPED_DIRECTORIES = new Set([".git"]);

/**
 * SHA-256 (hex) over a package tree: what decides whether a second publish of
 * a version carries the same content as the first.
 *
 * Only content counts, never where or when the tree was written. Entries are
 * taken in sorted order of their package-relative, `/`-separated path, so the
 * order a filesystem lists them in cannot change the hash, and each entry is
 * framed (`kind \0 path \0 length-or-target \0`) before its bytes, so moving
 * bytes between files, or a file between directories, always changes it.
 * Modification times and permissions are ignored: a re-download of the same
 * archive writes new ones.
 *
 * A symbolic link is hashed as its target text and never followed, so a link
 * out of the tree cannot pull outside content into the hash. `.git` is skipped
 * because a clone carries repository state, not package content.
 *
 * Reading the files costs more than hashing them, mostly in waiting on each
 * one in turn, so the entries ahead of the one being hashed are read at the
 * same time (READ_AHEAD of them). Only files up to READ_WHOLE_MAX_BYTES are
 * read ahead whole; a larger one is streamed when its turn comes, so the
 * memory held stays bounded however large the package is. Neither changes the
 * digest, which is always taken in sorted order.
 */
export async function hashPackageTree(root: string): Promise<string> {
   const hash = crypto.createHash("sha256");
   const entries = await listEntries(root, "");
   entries.sort((a, b) =>
      a.relative < b.relative ? -1 : a.relative > b.relative ? 1 : 0,
   );
   const reads: (Promise<EntryContent> | undefined)[] = [];
   const readAhead = (index: number) => {
      if (index >= entries.length || reads[index] !== undefined) return;
      const read = readEntry(root, entries[index]);
      // Awaited in turn below; until then a failure must not go unhandled.
      read.catch(() => undefined);
      reads[index] = read;
   };
   for (let index = 0; index < entries.length; index++) {
      for (let ahead = index; ahead < index + READ_AHEAD; ahead++) {
         readAhead(ahead);
      }
      const content = await reads[index]!;
      reads[index] = undefined;
      const { relative } = entries[index];
      if (content.kind === "dir") {
         hash.update(`dir\0${relative}\0\0`);
      } else if (content.kind === "link") {
         hash.update(`link\0${relative}\0${content.target}\0`);
      } else {
         hash.update(`file\0${relative}\0${content.size}\0`);
         if (content.bytes) {
            hash.update(content.bytes);
         } else {
            await new Promise<void>((resolve, reject) => {
               fs.createReadStream(content.absolute, {
                  highWaterMark: READ_WHOLE_MAX_BYTES,
               })
                  .on("data", (chunk) => hash.update(chunk))
                  .on("end", () => resolve())
                  .on("error", reject);
            });
         }
      }
   }
   return hash.digest("hex");
}

/** How many entries are read at once, ahead of the one being hashed. */
const READ_AHEAD = 16;
/** The largest file read ahead whole; a larger one is streamed in turn. */
const READ_WHOLE_MAX_BYTES = 1024 * 1024;

type EntryContent =
   | { kind: "dir" }
   | { kind: "link"; target: string }
   | { kind: "file"; absolute: string; size: number; bytes?: Buffer };

async function readEntry(
   root: string,
   entry: TreeEntry,
): Promise<EntryContent> {
   const absolute = path.join(root, ...entry.relative.split("/"));
   if (entry.kind === "dir") return { kind: "dir" };
   if (entry.kind === "link") {
      return { kind: "link", target: await fs.promises.readlink(absolute) };
   }
   const { size } = await fs.promises.stat(absolute);
   if (size > READ_WHOLE_MAX_BYTES) return { kind: "file", absolute, size };
   const bytes = await fs.promises.readFile(absolute);
   return { kind: "file", absolute, size: bytes.length, bytes };
}

interface TreeEntry {
   kind: "file" | "dir" | "link";
   relative: string;
}

async function listEntries(root: string, prefix: string): Promise<TreeEntry[]> {
   const directory =
      prefix === "" ? root : path.join(root, ...prefix.split("/"));
   const dirents = await fs.promises.readdir(directory, {
      withFileTypes: true,
   });
   const entries: TreeEntry[] = [];
   for (const dirent of dirents) {
      const relative = prefix === "" ? dirent.name : `${prefix}/${dirent.name}`;
      if (dirent.isSymbolicLink()) {
         entries.push({ kind: "link", relative });
      } else if (dirent.isDirectory()) {
         if (SKIPPED_DIRECTORIES.has(dirent.name)) continue;
         entries.push({ kind: "dir", relative });
         entries.push(...(await listEntries(root, relative)));
      } else if (dirent.isFile()) {
         entries.push({ kind: "file", relative });
      }
   }
   return entries;
}
