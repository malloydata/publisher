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
 */
export async function hashPackageTree(root: string): Promise<string> {
   const hash = crypto.createHash("sha256");
   const entries = await listEntries(root, "");
   entries.sort((a, b) =>
      a.relative < b.relative ? -1 : a.relative > b.relative ? 1 : 0,
   );
   for (const entry of entries) {
      const absolute = path.join(root, ...entry.relative.split("/"));
      if (entry.kind === "dir") {
         hash.update(`dir\0${entry.relative}\0\0`);
      } else if (entry.kind === "link") {
         const target = await fs.promises.readlink(absolute);
         hash.update(`link\0${entry.relative}\0${target}\0`);
      } else {
         const { size } = await fs.promises.stat(absolute);
         hash.update(`file\0${entry.relative}\0${size}\0`);
         await new Promise<void>((resolve, reject) => {
            fs.createReadStream(absolute)
               .on("data", (chunk) => hash.update(chunk))
               .on("end", () => resolve())
               .on("error", reject);
         });
      }
   }
   return hash.digest("hex");
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
