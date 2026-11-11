// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import { Annotations } from "@malloydata/malloy";
import { readdirSync, readFileSync, statSync } from "fs";
import path from "path";
import { logger } from "../logger";
import * as sinon from "sinon";
import { ENV_REFERENCE_DROPPED, parseRouteTag, routeHasKey } from "./motly";

const SECRET_VAR = "PUBLISHER_SPEC_ROUTE_TAG_SECRET";
const SECRET = "s3cr3t-from-the-server-environment";

/** Annotations over `lines`, one note per line, as the translator records them. */
function annotationsOf(lines: string[]): Annotations {
   return new Annotations({
      notes: lines.map((text, line) => ({
         text,
         at: {
            url: "file:///m.malloy",
            range: {
               start: { line, character: 0 },
               end: { line, character: text.length },
            },
         },
      })),
   } as ConstructorParameters<typeof Annotations>[0]);
}

describe("parseRouteTag", () => {
   afterEach(() => {
      delete process.env[SECRET_VAR];
   });

   it("is parseAsTag itself when no line carries an @env. reference", () => {
      const annotations = annotationsOf([
         `#@ persist name="orders" storage=lake`,
         "#@ queryMetadata { owner=finance }",
         "# label=Orders",
      ]);
      const ours = parseRouteTag(annotations, "@");
      const theirs = annotations.parseAsTag("@");
      expect(ours.tag.toJSON()).toEqual(theirs.tag.toJSON());
      expect(ours.log).toEqual(theirs.log);
      expect(ours.tag.text("name")).toBe("orders");
   });

   it("drops each line that reads the environment, keeps the rest, and logs the drop", () => {
      process.env[SECRET_VAR] = SECRET;
      const annotations = annotationsOf([
         `#@ persist name=@env.${SECRET_VAR}`,
         "#@ storage=lake",
         `#@ queryMetadata { owner=@env.${SECRET_VAR} }`,
      ]);
      // The unguarded parse is the leak this closes.
      expect(
         JSON.stringify(annotations.parseAsTag("@").tag.toJSON()),
      ).toContain(SECRET);

      const { tag, log } = parseRouteTag(annotations, "@");
      expect(JSON.stringify(tag.toJSON())).not.toContain(SECRET);
      expect(tag.has("persist")).toBe(false);
      expect(tag.text("storage")).toBe("lake");
      expect(
         log.filter((entry) => entry.message === ENV_REFERENCE_DROPPED),
      ).toHaveLength(2);
      expect(log[0]).toMatchObject({
         code: "env-reference-dropped",
         severity: "warn",
         at: { range: { start: { line: 0 } } },
      });
   });

   it("guards the model tag route and compiler flags too", () => {
      process.env[SECRET_VAR] = SECRET;
      const annotations = annotationsOf([
         `## queryMetadata { owner=@env.${SECRET_VAR} }`,
         "## team=core",
      ]);
      const { tag } = parseRouteTag(annotations);
      expect(JSON.stringify(tag.toJSON())).not.toContain(SECRET);
      expect(tag.text("team")).toBe("core");
   });

   it("still drops a line when the variable is unset", () => {
      const { tag, log } = parseRouteTag(
         annotationsOf([`#@ persist name=@env.${SECRET_VAR}`]),
         "@",
      );
      expect(tag.has("persist")).toBe(false);
      expect(log.map((entry) => entry.code)).toEqual(["env-reference-dropped"]);
   });
});

describe("routeHasKey", () => {
   it("decides presence over every line, as Malloy does, and returns only a boolean", () => {
      const annotations = annotationsOf([`#@ persist name=@env.${SECRET_VAR}`]);
      expect(routeHasKey(annotations, "@", "persist")).toBe(true);
      expect(parseRouteTag(annotations, "@").tag.has("persist")).toBe(false);
      expect(routeHasKey(annotations, "@", "storage")).toBe(false);
   });
});

describe("a dropped line is warned about once per location", () => {
   it("warns once, however many times the same annotation is read", () => {
      const warn = sinon.stub(logger, "warn");
      try {
         const annotations = annotationsOf([
            "#@ persist storage=lake",
            `#@ queryMetadata { owner=@env.${SECRET_VAR}_ONCE }`,
         ]);
         parseRouteTag(annotations, "@");
         parseRouteTag(annotations, "@");
         const calls = warn
            .getCalls()
            .filter((call) => String(call.args[0]).includes("@env."));
         expect(calls).toHaveLength(1);
         expect(calls[0].args[1]).toMatchObject({
            at: "file:///m.malloy:2",
            route: "@",
         });
      } finally {
         warn.restore();
      }
   });
});

describe("every parseAsTag in the server goes through parseRouteTag", () => {
   // A direct call hydrates `@env.` from the server's environment.
   const SRC = path.resolve(__dirname, "..");
   const ALLOWED = new Set([path.join("service", "motly.ts")]);

   function sourceFiles(dir: string): string[] {
      return readdirSync(dir).flatMap((name) => {
         const full = path.join(dir, name);
         if (statSync(full).isDirectory()) return sourceFiles(full);
         return /\.tsx?$/.test(name) && !/\.spec\.tsx?$/.test(name)
            ? [full]
            : [];
      });
   }

   /** Files with a line that matches `pattern`, ignoring comment lines. */
   function filesCalling(pattern: RegExp): string[] {
      return sourceFiles(SRC)
         .filter((file) =>
            readFileSync(file, "utf8")
               .split("\n")
               .some(
                  (line) => pattern.test(line) && !/^\s*(\*|\/\/)/.test(line),
               ),
         )
         .map((file) => path.relative(SRC, file));
   }

   it("has no direct parseAsTag or tagParse call outside motly.ts", () => {
      // `Explore.tagParse()` hydrates `@env.` the same way.
      expect(
         filesCalling(/\.(parseAsTag|tagParse)\(/).filter(
            (file) => !ALLOWED.has(file),
         ),
      ).toEqual([]);
   });

   it("imports the tag parser's parse functions only where each line is screened first", () => {
      // Each of these drops or refuses a line carrying `@env.` before it parses:
      // motly.ts (hasEnvReference), classification.ts (refuses the value) and
      // the `#(dbmeta)` reader in package_entities.ts. A new importer must too.
      const SCREENED = new Set([
         path.join("service", "motly.ts"),
         path.join("service", "classification.ts"),
         path.join("retrieval", "model", "package_entities.ts"),
      ]);
      expect(
         filesCalling(
            /import\s*\{[^}]*\b(parseAnnotation|parseTag|TagParser)\b[^}]*\}\s*from\s*"@malloydata\/malloy-tag"/,
         ).filter((file) => !SCREENED.has(file)),
      ).toEqual([]);
   });
});
