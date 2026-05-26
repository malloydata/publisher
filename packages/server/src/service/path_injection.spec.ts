import { describe, expect, it } from "bun:test";
import { BadRequestError } from "../errors";
import { deleteDuckLakeConnectionFile } from "./connection";

const TRAVERSAL_NAMES: ReadonlyArray<readonly [string, string]> = [
   ["leading traversal", "../etc"],
   ["embedded traversal", "foo/../../bar"],
   ["slash in name", "foo/bar"],
   ["backslash in name", "foo\\bar"],
   ["leading dot", ".staging"],
   ["bare dot-dot", ".."],
   ["bare dot", "."],
   ["empty", ""],
   ["NUL byte", "foo\0bar"],
   ["oversized", "a".repeat(256)],
   ["absolute", "/etc/passwd"],
] as const;

describe("deleteDuckLakeConnectionFile path-injection guards", () => {
   it.each(TRAVERSAL_NAMES)(
      "rejects %s as connectionName (%p)",
      async (_label, connectionName) => {
         await expect(
            deleteDuckLakeConnectionFile(connectionName, "/tmp/env"),
         ).rejects.toBeInstanceOf(BadRequestError);
      },
   );

   it.each([
      ["relative", "relative/path"],
      ["traversal", "/var/lib/../../etc"],
      ["NUL byte", "/var/lib/env\0"],
      ["bare dot-dot", ".."],
   ])("rejects %s as environmentPath (%p)", async (_label, environmentPath) => {
      await expect(
         deleteDuckLakeConnectionFile("conn", environmentPath),
      ).rejects.toBeInstanceOf(BadRequestError);
   });
});
