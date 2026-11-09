// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import {
   REFERENCE_END,
   REFERENCE_START,
} from "../../../src/settings/reference";

/** scripts/generate-settings-reference.ts, run as `bun` runs it in CI. */
describe("the settings reference generator", () => {
   const SCRIPT = path.resolve(
      import.meta.dir,
      "../../../scripts/generate-settings-reference.ts",
   );
   const tempDoc = (text: string) => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "reference-"));
      const file = path.join(dir, "configuration.md");
      fs.writeFileSync(file, text);
      return file;
   };

   it("is rewritten by the generator, which then reports it up to date", () => {
      const file = tempDoc(
         `# Configuration\n\n${REFERENCE_START}\n\n| old |\n\n${REFERENCE_END}\n\nAfter.\n`,
      );
      const first = Bun.spawnSync(["bun", SCRIPT, file]);
      expect(first.exitCode).toBe(0);
      expect(first.stdout.toString()).toContain("Updated");
      const rewritten = fs.readFileSync(file, "utf8");
      expect(rewritten).toContain("| `mcp.port` |");
      expect(rewritten).toEndWith("After.\n");
      const second = Bun.spawnSync(["bun", SCRIPT, file]);
      expect(second.stdout.toString()).toContain("is up to date");
      expect(fs.readFileSync(file, "utf8")).toBe(rewritten);
   });

   it("is refused by the generator when the doc has no table to rewrite", () => {
      const file = tempDoc("# Configuration\n\nNo markers here.\n");
      const run = Bun.spawnSync(["bun", SCRIPT, file]);
      expect(run.exitCode).not.toBe(0);
      expect(run.stderr.toString()).toContain(
         "has no generated settings table to rewrite",
      );
      expect(fs.readFileSync(file, "utf8")).toBe(
         "# Configuration\n\nNo markers here.\n",
      );
   });
});
