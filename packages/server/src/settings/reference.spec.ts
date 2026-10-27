// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { withSettings } from "../test_helpers/settings_env";
import { REGISTRY, Settings } from ".";
import {
   helpText,
   REFERENCE_END,
   REFERENCE_START,
   settingsLine,
   withReferenceTable,
} from "./reference";

const DOC = path.resolve(import.meta.dir, "../../../../docs/configuration.md");

/** The doc as the generator writes it: a Windows checkout has CRLF line endings. */
const readDoc = () => fs.readFileSync(DOC, "utf8").replaceAll("\r\n", "\n");

describe("the generated settings reference", () => {
   it("is what docs/configuration.md holds (run `bun run generate-settings-reference` after a registry change)", () => {
      const doc = readDoc();
      expect(doc).toContain(REFERENCE_START);
      expect(doc).toContain(REFERENCE_END);
      expect(withReferenceTable(doc)).toBe(doc);
   });

   it("names every variable and flag a setting is read from", () => {
      const doc = readDoc();
      const table = doc.slice(
         doc.indexOf(REFERENCE_START),
         doc.indexOf(REFERENCE_END),
      );
      for (const entry of REGISTRY) {
         for (const name of [
            ...(entry.aliases.env ?? []),
            ...(entry.aliases.flags ?? []),
         ]) {
            expect(table, name).toContain(`\`${name}`);
         }
      }
   });

   it("says in words what a default that depends on other settings is", () => {
      for (const entry of REGISTRY) {
         if (typeof entry.default === "function") {
            expect(entry.defaultText, entry.key).toBeDefined();
         }
      }
   });
});

describe("--help", () => {
   it("lists every flag the server reads, with its default", () => {
      const help = helpText();
      for (const entry of REGISTRY) {
         for (const flag of entry.aliases.flags ?? []) {
            expect(help, flag).toContain(flag);
         }
      }
      // The API switches were missing before the text was generated.
      expect(help).toContain("--serving-api-enabled, --no-serving-api-enabled");
      expect(help).toContain("--admin-api-enabled, --no-admin-api-enabled");
      expect(help).toMatch(
         /--port <n> +Port of the REST API and the web app\. \(default: 4000\)/,
      );
      expect(help).toContain("--help, -h");
   });
});

describe("the startup settings line", () => {
   it("names each setting given, its value and where it came from, and hides secrets", () => {
      const line = withSettings(
         {
            env: {
               PUBLISHER_PORT: "4100",
               PUBLISHER_ADMIN_CATALOG_DATABASE: "postgresql://u:pw@h/db",
            },
            argv: ["--mcp_port", "4141"],
            file: { frozenConfig: true, environments: [] },
         },
         ({ serverRoot }) =>
            settingsLine(Settings.current()).replaceAll(
               path.join(serverRoot, "publisher.config.json"),
               "<file>",
            ),
      );
      expect(line).toBe(
         "Settings not at their defaults: frozenConfig=true from <file>, port=4100 from PUBLISHER_PORT, environments from <file>, mcp.port=4141 from --mcp_port, admin.catalogDatabase=(set) from PUBLISHER_ADMIN_CATALOG_DATABASE",
      );
      expect(line).not.toContain("pw");
   });

   it("says so when every setting is at its default", () => {
      expect(withSettings({}, () => settingsLine(Settings.current()))).toBe(
         "Settings: every setting is at its default.",
      );
   });
});

describe("the generated reference, when it is out of date", () => {
   const SCRIPT = path.resolve(
      import.meta.dir,
      "../../scripts/generate-settings-reference.ts",
   );
   const tempDoc = (text: string) => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "reference-"));
      const file = path.join(dir, "configuration.md");
      fs.writeFileSync(file, text);
      return file;
   };

   it("is caught: a stale table is not what the registry generates", () => {
      const stale = `# Configuration\n\n${REFERENCE_START}\n\n| old |\n\n${REFERENCE_END}\n`;
      expect(withReferenceTable(stale)).not.toBe(stale);
      expect(withReferenceTable(stale)).toContain("| `mcp.port` |");
   });

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

describe("the startup settings line, for a value that did not parse", () => {
   it("names the setting without the value", () => {
      const line = withSettings({ env: { MCP_PORT: "40a0" } }, () =>
         settingsLine(Settings.current()),
      );
      expect(line).toContain("mcp.port=(invalid) from MCP_PORT");
      expect(line).not.toContain("40a0");
   });
});
