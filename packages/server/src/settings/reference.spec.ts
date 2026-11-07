// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import fs from "fs";
import path from "path";
import { withSettings } from "../test_helpers/settings_env";
import { REGISTRY, Settings } from ".";
import {
   helpText,
   REFERENCE_END,
   REFERENCE_START,
   settingsLine,
   settingsReferenceTable,
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

   it("gives a negated name's default as unset, not the key's default read backwards", () => {
      const doc = readDoc();
      const negated = REGISTRY.filter((entry) => entry.aliases.negated);
      expect(negated.map((entry) => entry.key)).toContain(
         "mcp.configFileEnabled",
      );
      for (const entry of negated) {
         const row = doc
            .split("\n")
            .find((line) => line.startsWith(`| \`${entry.key}\` |`));
         expect(row, entry.key).toBeDefined();
         // Setting | Env var | CLI flag | Default | Meaning
         expect(row!.split(" | ")[3], entry.key).toBe("_unset_");
      }
   });

   it("names another setting by its key, with no {key} reference left", () => {
      const table = settingsReferenceTable();
      expect(table).toContain("| `host` when given, else `127.0.0.1` |");
      expect(table).not.toMatch(/\{(host|mcp\.host|root|config)\}/);
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

   it("names other settings by the flags that set them", () => {
      const help = helpText();
      expect(help).toContain("and MCP too unless --mcp_host is given.");
      expect(help).toContain("(default: --host when given, else 127.0.0.1)");
      expect(help).toContain("--host does not move it.");
      expect(help).not.toMatch(/\{[a-zA-Z.]+\}|unless mcp\.host/);
   });

   it("gives a negated flag no default, and says what it stops", () => {
      const line = helpText()
         .split("\n")
         .find((text) => text.trimStart().startsWith("--no-mcp-config "));
      expect(line).toBeDefined();
      expect(line).toContain(
         "Do not write .mcp.json into the working directory.",
      );
      expect(line).toContain("is inside a git working tree");
      expect(line).not.toContain("(default:");
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
         "Settings given: frozenConfig=true from <file>, port=4100 from PUBLISHER_PORT, environments from <file>, mcp.port=4141 from --mcp_port, admin.catalogDatabase=(set) from PUBLISHER_ADMIN_CATALOG_DATABASE",
      );
      expect(line).not.toContain("pw");
   });

   it("names a setting given its default value too", () => {
      expect(
         withSettings({ argv: ["--port", "4000"] }, () =>
            settingsLine(Settings.current()),
         ),
      ).toBe("Settings given: port=4000 from --port");
   });

   it("keeps an access key id and a URL's user and password out of the log", () => {
      const line = withSettings(
         {
            env: {
               PUBLISHER_ADMIN_STORAGE_KEY_ID: "AKIAIOSFODNN7EXAMPLE",
               PUBLISHER_ADMIN_STORAGE_ENDPOINT: "https://k:s@minio:9000",
               EMBEDDING_API_BASE: "https://user:hunter2@proxy.example/v1",
            },
         },
         () => settingsLine(Settings.current()),
      );
      expect(line).toContain(
         "admin.storage.keyId=(set) from PUBLISHER_ADMIN_STORAGE_KEY_ID",
      );
      expect(line).toContain(
         'admin.storage.endpoint="https://***@minio:9000" from PUBLISHER_ADMIN_STORAGE_ENDPOINT',
      );
      expect(line).toContain(
         'retrieval.embedding.baseUrl="https://***@proxy.example/v1" from EMBEDDING_API_BASE',
      );
      for (const leak of ["AKIAIOSFODNN7EXAMPLE", "k:s", "hunter2", "user:"]) {
         expect(line).not.toContain(leak);
      }
   });

   it("hides a URL's password that holds a slash or an @", () => {
      const line = withSettings(
         {
            env: {
               EMBEDDING_API_BASE: "https://user:pa/ss@llm.example.com/v1",
               PUBLISHER_ADMIN_STORAGE_ENDPOINT: "https://user:p@ss@minio:9000",
            },
         },
         () => settingsLine(Settings.current()),
      );
      expect(line).toContain(
         'retrieval.embedding.baseUrl="https://***@llm.example.com/v1" from EMBEDDING_API_BASE',
      );
      expect(line).toContain(
         'admin.storage.endpoint="https://***@minio:9000" from PUBLISHER_ADMIN_STORAGE_ENDPOINT',
      );
      for (const leak of ["pa/ss", "p@ss", "ss@", "user"]) {
         expect(line).not.toContain(leak);
      }
   });

   it("says so when every setting is at its default", () => {
      expect(withSettings({}, () => settingsLine(Settings.current()))).toBe(
         "Settings: every setting is at its default.",
      );
   });
});

describe("the generated reference, when it is out of date", () => {
   it("is caught: a stale table is not what the registry generates", () => {
      const stale = `# Configuration\n\n${REFERENCE_START}\n\n| old |\n\n${REFERENCE_END}\n`;
      expect(withReferenceTable(stale)).not.toBe(stale);
      expect(withReferenceTable(stale)).toContain("| `mcp.port` |");
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
