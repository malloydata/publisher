import { createHash } from "crypto";
import fs from "fs";
import path from "path";

/**
 * SHA-256 of the exact model bytes a package is serving, keyed by
 * package-relative path. Paths are sorted so the hash is independent of
 * Map insertion order. Missing files are hashed as empty so a deleted
 * model still changes the digest.
 */
export function computeSourceContentSha(
   packagePath: string,
   modelPaths: Iterable<string>,
): string {
   const hash = createHash("sha256");
   for (const modelPath of [...modelPaths].sort()) {
      hash.update(modelPath);
      hash.update("\0");
      const absolute = path.join(packagePath, modelPath);
      try {
         hash.update(fs.readFileSync(absolute));
      } catch {
         hash.update("");
      }
      hash.update("\0");
   }
   return hash.digest("hex");
}

export function mintServedRevision(): string {
   return crypto.randomUUID();
}
