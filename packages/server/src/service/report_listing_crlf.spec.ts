// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A report saved with CRLF line endings (a Windows checkout, an editor set to
 * CRLF) lists the same title and description as one saved with LF: no `\r`
 * reaches the listing from any step of the title chain (`title=`, the doc
 * comment's first line and the rest of it, and a `.malloynb`'s first markdown
 * heading).
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Environment } from "./environment";

const crlf = (text: string) => text.replace(/\r?\n/g, "\r\n");

const BASE = `source: s is duckdb.sql("select 1 as x") extend {
   view: v is { group_by: x }
}
`;

const FILES: Record<string, string> = {
   "base.malloy": BASE,
   "explicit.malloy":
      `##" Ignored for the title, kept as the description.\n` +
      `## artifact { layout=linear title="Explicit title" tiles=["s -> v"] }\n` +
      `import "base.malloy"\n`,
   "doc.malloy":
      `##" Carrier volumes\n##" Shipments per carrier,\n##" refreshed nightly.\n` +
      `## artifact { layout=linear tiles=["s -> v"] }\nimport "base.malloy"\n`,
   "heading.malloynb":
      `>>>markdown\n# Brands\nProse.\n` +
      `>>>malloy\nimport "base.malloy"\nrun: s -> v\n`,
   "grid.malloy":
      `##" Grid title\n##" Grid description.\n` +
      `## artifact { layout=grid tiles=["s -> v"] }\nimport "base.malloy"\n`,
};

describe("report listing over CRLF text", () => {
   let rootDir: string;
   let env: Environment;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-crlf-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const listing = async (eol: (text: string) => string) => {
      const name = eol === crlf ? "crlf" : "lf";
      await env.installPackage(name, async (stagingPath) => {
         await fs.mkdir(stagingPath, { recursive: true });
         await fs.writeFile(
            path.join(stagingPath, "publisher.json"),
            JSON.stringify({ name, version: "1.0.0" }),
         );
         for (const [file, text] of Object.entries(FILES))
            await fs.writeFile(path.join(stagingPath, file), eol(text));
      });
      const pkg = await env.getPackage(name, false);
      return Object.fromEntries(
         pkg
            .listReports()
            .map((r) => [
               r.path,
               { title: r.title, description: r.description },
            ]),
      );
   };

   it("lists the same titles and descriptions as LF text, with no carriage return", async () => {
      const fromLf = await listing((text) => text);
      const fromCrlf = await listing(crlf);
      expect(fromLf).toEqual({
         "doc.malloy": {
            title: "Carrier volumes",
            description: "Shipments per carrier,\nrefreshed nightly.",
         },
         "explicit.malloy": {
            title: "Explicit title",
            description: "Ignored for the title, kept as the description.",
         },
         "grid.malloy": {
            title: "Grid title",
            description: "Grid description.",
         },
         "heading.malloynb": { title: "Brands", description: undefined },
      });
      expect(fromCrlf).toEqual(fromLf);
      expect(JSON.stringify(fromCrlf)).not.toContain("\\r");
   });
});
